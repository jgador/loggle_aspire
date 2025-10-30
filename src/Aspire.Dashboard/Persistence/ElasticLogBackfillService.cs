// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
using System.Globalization;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json.Serialization;
using Aspire.Dashboard.Otlp.Model;
using Aspire.Dashboard.Otlp.Storage;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Microsoft.Extensions.Options;
using OpenTelemetry.Proto.Collector.Logs.V1;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Resource.V1;

namespace Aspire.Dashboard.Persistence;

internal sealed class ElasticLogBackfillService : IHostedService
{
    private readonly ElasticPersistenceOptions _options;
    private readonly Microsoft.Extensions.Logging.ILogger<ElasticLogBackfillService> _logger;
    private readonly TelemetryRepository _telemetryRepository;
    private readonly DashboardCircuitTracker _circuitTracker;
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private CancellationTokenSource? _executionCts;
    private Task? _executionTask;

    public ElasticLogBackfillService(
        IOptions<ElasticPersistenceOptions> options,
        Microsoft.Extensions.Logging.ILogger<ElasticLogBackfillService> logger,
        TelemetryRepository telemetryRepository,
        DashboardCircuitTracker circuitTracker,
        IHostApplicationLifetime hostApplicationLifetime)
    {
        _options = options.Value;
        _logger = logger;
        _telemetryRepository = telemetryRepository;
        _circuitTracker = circuitTracker;
        _hostApplicationLifetime = hostApplicationLifetime;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.IsConfigured)
        {
            _logger.LogDebug("Elastic backfill skipped because configuration is incomplete.");
            return Task.CompletedTask;
        }

        _executionCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _hostApplicationLifetime.ApplicationStopping);

        _executionTask = Task.Run(() => ExecuteAsync(_executionCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_executionTask is null)
        {
            return;
        }

        try
        {
            _executionCts?.Cancel();
        }
        finally
        {
            try
            {
                await _executionTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Host is stopping; suppress cancellation.
            }
            finally
            {
                _executionCts?.Dispose();
                _executionCts = null;
                _executionTask = null;
            }
        }
    }

    private async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        try
        {
            await WaitForDashboardReadyAsync(cancellationToken).ConfigureAwait(false);
            await WaitForElasticsearchAsync(cancellationToken).ConfigureAwait(false);
            await _circuitTracker.WaitForFirstCircuitAsync(cancellationToken).ConfigureAwait(false);

            var documents = await FetchRecentLogsAsync(cancellationToken).ConfigureAwait(false);
            if (documents.Count == 0)
            {
                _logger.LogInformation("No persisted logs found in {DataStream}.", _options.DataStream);
                return;
            }

            IngestIntoRepository(documents);
            _logger.LogInformation("Loaded {LogCount} logs from Elasticsearch into the Aspire dashboard.", documents.Count);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown requested – nothing to do.
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "Failed to backfill logs from Elasticsearch. Aspire will continue with live telemetry only.");
        }
    }

    private async Task WaitForDashboardReadyAsync(CancellationToken cancellationToken)
    {
        if (_hostApplicationLifetime.ApplicationStarted.IsCancellationRequested)
        {
            return;
        }

        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = _hostApplicationLifetime.ApplicationStarted.Register(static state =>
        {
            var source = (TaskCompletionSource<object?>)state!;
            source.TrySetResult(null);
        }, tcs);
        using var cancellationRegistration = cancellationToken.Register(static state =>
        {
            var source = (TaskCompletionSource<object?>)state!;
            source.TrySetCanceled();
        }, tcs);

        await tcs.Task.ConfigureAwait(false);
    }

    private async Task WaitForElasticsearchAsync(CancellationToken cancellationToken)
    {
        using var httpClient = CreateHttpClient();

        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            attempt++;
            try
            {
                using var response = await httpClient.GetAsync("_cluster/health?timeout=5s", cancellationToken).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    _logger.LogDebug("Elasticsearch cluster responded to health check (attempt {Attempt}).", attempt);
                    return;
                }

                var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogDebug("Elasticsearch health check returned {StatusCode} on attempt {Attempt}. Body: {Body}", (int)response.StatusCode, attempt, body);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                _logger.LogDebug(ex, "Elasticsearch not ready (attempt {Attempt}).", attempt);
            }

            await Task.Delay(TimeSpan.FromSeconds(Math.Min(attempt, 10)), cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool IsTransient(Exception exception)
    {
        return exception is HttpRequestException or TaskCanceledException;
    }

    private async Task<List<ElasticStoredLogDocument>> FetchRecentLogsAsync(CancellationToken cancellationToken)
    {
        object query = _options.LookbackWindow > TimeSpan.Zero
            ? new Dictionary<string, object>
            {
                ["range"] = new Dictionary<string, object>
                {
                    ["@timestamp"] = new Dictionary<string, object>
                    {
                        ["gte"] = DateTime.UtcNow.Subtract(_options.LookbackWindow).ToString("o")
                    }
                }
            }
            : new Dictionary<string, object>
            {
                ["match_all"] = new { }
            };

        var requestBody = new Dictionary<string, object?>
        {
            ["size"] = _options.PreloadLogCount,
            ["sort"] = new object[]
            {
                new Dictionary<string, object>
                {
                    ["@timestamp"] = new Dictionary<string, object>
                    {
                        ["order"] = "desc"
                    }
                }
            },
            ["query"] = query
        };

        using var httpClient = CreateHttpClient();

        var requestJson = System.Text.Json.JsonSerializer.Serialize(requestBody, s_serializerOptions);
        using var httpContent = new System.Net.Http.StringContent(requestJson, System.Text.Encoding.UTF8, "application/json");
        using var httpResponse = await httpClient.PostAsync(
            $"{_options.DataStream}/_search",
            httpContent,
            cancellationToken).ConfigureAwait(false);

        if (!httpResponse.IsSuccessStatusCode)
        {
            var body = await httpResponse.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new InvalidOperationException($"Elasticsearch search failed ({(int)httpResponse.StatusCode}): {body}");
        }

        var responseStream = await httpResponse.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var payload = await System.Text.Json.JsonSerializer.DeserializeAsync<SearchEnvelope>(responseStream, s_serializerOptions, cancellationToken).ConfigureAwait(false);
        if (payload?.Hits?.Documents is not { Count: > 0 })
        {
            return [];
        }

        return payload.Hits.Documents
            .Select(hit => hit.Source)
            .Where(source => source is not null)
            .Select(source => source!)
            .ToList();
    }

    private void IngestIntoRepository(List<ElasticStoredLogDocument> documents)
    {
        // newest first from search; reverse to add oldest-first
        var ordered = documents
            .OrderBy(d => d.Timestamp)
            .ToList();

        var resourceGroups = ordered.GroupBy(doc => new ResourceKey(
            doc.ServiceName ?? string.Empty,
            doc.ServiceInstanceId ?? string.Empty,
            doc.ServiceVersion ?? string.Empty));

        var request = new ExportLogsServiceRequest();

        foreach (var group in resourceGroups)
        {
            var resourceLogs = new ResourceLogs
            {
                Resource = BuildResource(group.Key, group.First())
            };

            var scopeLogs = new ScopeLogs();

            foreach (var doc in group)
            {
                var record = CreateLogRecord(doc);
                scopeLogs.LogRecords.Add(record);
            }

            resourceLogs.ScopeLogs.Add(scopeLogs);
            request.ResourceLogs.Add(resourceLogs);
        }

        if (request.ResourceLogs.Count == 0)
        {
            return;
        }

        var addContext = new AddContext();
        _telemetryRepository.AddLogs(addContext, request.ResourceLogs);

        if (addContext.FailureCount > 0)
        {
            _logger.LogWarning("Failed to ingest {FailureCount} persisted log records into repository.", addContext.FailureCount);
        }
    }

    private static Resource BuildResource(ResourceKey key, ElasticStoredLogDocument sample)
    {
        var resource = new Resource();
        var attributes = resource.Attributes;

        AppendStringAttribute(attributes, OtlpResource.SERVICE_NAME, key.ServiceName);
        AppendStringAttribute(attributes, OtlpResource.SERVICE_INSTANCE_ID, key.ServiceInstanceId);

        if (!string.IsNullOrEmpty(key.ServiceVersion))
        {
            AppendStringAttribute(attributes, "service.version", key.ServiceVersion);
        }

        if (sample.ServiceAttributes is { Count: > 0 })
        {
            foreach (var attr in sample.ServiceAttributes)
            {
                if (string.IsNullOrWhiteSpace(attr.Name))
                {
                    continue;
                }

                AppendStringAttribute(attributes, attr.Name!, attr.Value);
            }
        }

        return resource;
    }

    private static LogRecord CreateLogRecord(ElasticStoredLogDocument doc)
    {
        var timestamp = EnsureUtc(doc.Timestamp);
        var unixNano = ToUnixNano(timestamp);

        var record = new LogRecord
        {
            TimeUnixNano = unixNano,
            ObservedTimeUnixNano = unixNano,
            Flags = doc.Flags,
            Body = new AnyValue { StringValue = doc.Message ?? string.Empty }
        };

        if (TryParseLogLevel(doc.LogLevel, out var parsedLevel))
        {
            record.SeverityNumber = MapSeverity(parsedLevel);
            record.SeverityText = parsedLevel.ToString();
        }

        if (!string.IsNullOrEmpty(doc.TraceId) && TryDecodeHex(doc.TraceId, expectedBytes: 16, out var traceId))
        {
            record.TraceId = ByteString.CopyFrom(traceId);
        }

        if (!string.IsNullOrEmpty(doc.SpanId) && TryDecodeHex(doc.SpanId, expectedBytes: 8, out var spanId))
        {
            record.SpanId = ByteString.CopyFrom(spanId);
        }

        if (!string.IsNullOrEmpty(doc.ParentId))
        {
            record.Attributes.Add(new KeyValue
            {
                Key = "ParentId",
                Value = new AnyValue { StringValue = doc.ParentId }
            });
        }

        if (!string.IsNullOrEmpty(doc.OriginalFormat))
        {
            record.Attributes.Add(new KeyValue
            {
                Key = "{OriginalFormat}",
                Value = new AnyValue { StringValue = doc.OriginalFormat }
            });
        }

        if (doc.Attributes is { Count: > 0 })
        {
            foreach (var attr in doc.Attributes)
            {
                if (string.IsNullOrWhiteSpace(attr.Name))
                {
                    continue;
                }

                record.Attributes.Add(new KeyValue
                {
                    Key = attr.Name!,
                    Value = new AnyValue { StringValue = attr.Value ?? string.Empty }
                });
            }
        }

        return record;
    }

    private static void AppendStringAttribute(RepeatedField<KeyValue> attributes, string name, string? value)
    {
        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(value))
        {
            return;
        }

        attributes.Add(new KeyValue
        {
            Key = name,
            Value = new AnyValue { StringValue = value }
        });
    }

    private static ulong ToUnixNano(DateTime timestampUtc)
    {
        var utc = EnsureUtc(timestampUtc);
        var ticks = utc.Subtract(DateTime.UnixEpoch).Ticks;
        if (ticks <= 0)
        {
            return 0;
        }

        return (ulong)ticks * (ulong)TimeSpan.NanosecondsPerTick;
    }

    private static DateTime EnsureUtc(DateTime value)
    {
        return value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
        };
    }

    private static SeverityNumber MapSeverity(Microsoft.Extensions.Logging.LogLevel level) => level switch
    {
        Microsoft.Extensions.Logging.LogLevel.Trace => SeverityNumber.Trace,
        Microsoft.Extensions.Logging.LogLevel.Debug => SeverityNumber.Debug,
        Microsoft.Extensions.Logging.LogLevel.Information => SeverityNumber.Info,
        Microsoft.Extensions.Logging.LogLevel.Warning => SeverityNumber.Warn,
        Microsoft.Extensions.Logging.LogLevel.Error => SeverityNumber.Error,
        Microsoft.Extensions.Logging.LogLevel.Critical => SeverityNumber.Fatal,
        _ => SeverityNumber.Unspecified
    };

    private static bool TryParseLogLevel(string? value, out Microsoft.Extensions.Logging.LogLevel level)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            if (Enum.TryParse(value, ignoreCase: true, out Microsoft.Extensions.Logging.LogLevel parsed))
            {
                level = parsed;
                return true;
            }

            if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var numeric))
            {
                if (Enum.IsDefined(typeof(Microsoft.Extensions.Logging.LogLevel), numeric))
                {
                    level = (Microsoft.Extensions.Logging.LogLevel)numeric;
                    return true;
                }

                var mapped = MapOtelSeverityNumber(numeric);
                if (mapped.HasValue)
                {
                    level = mapped.Value;
                    return true;
                }
            }
        }

        level = Microsoft.Extensions.Logging.LogLevel.None;
        return false;
    }

    private static Microsoft.Extensions.Logging.LogLevel? MapOtelSeverityNumber(int severityNumber) => severityNumber switch
    {
        >= 1 and <= 4 => Microsoft.Extensions.Logging.LogLevel.Trace,
        >= 5 and <= 8 => Microsoft.Extensions.Logging.LogLevel.Debug,
        >= 9 and <= 12 => Microsoft.Extensions.Logging.LogLevel.Information,
        >= 13 and <= 16 => Microsoft.Extensions.Logging.LogLevel.Warning,
        >= 17 and <= 20 => Microsoft.Extensions.Logging.LogLevel.Error,
        >= 21 and <= 24 => Microsoft.Extensions.Logging.LogLevel.Critical,
        _ => null
    };

    private static bool TryDecodeHex(string value, int expectedBytes, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();

        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var sanitized = value.Trim();
        if (sanitized.Length != expectedBytes * 2)
        {
            return false;
        }

        try
        {
            var decoded = Convert.FromHexString(sanitized);
            if (decoded.Length != expectedBytes)
            {
                return false;
            }

            bytes = decoded;
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static readonly System.Text.Json.JsonSerializerOptions s_serializerOptions = new(System.Text.Json.JsonSerializerDefaults.Web);

    private System.Net.Http.HttpClient CreateHttpClient()
    {
        var httpClient = new System.Net.Http.HttpClient
        {
            BaseAddress = new Uri(_options.Url!)
        };

        if (!string.IsNullOrEmpty(_options.Username))
        {
            var password = _options.Password ?? string.Empty;
            var credentialBytes = System.Text.Encoding.UTF8.GetBytes($"{_options.Username}:{password}");
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(credentialBytes));
        }

        return httpClient;
    }

    private sealed class SearchEnvelope
    {
        [JsonPropertyName("hits")]
        public SearchHits? Hits { get; set; }
    }

    private sealed class SearchHits
    {
        [JsonPropertyName("hits")]
        public List<SearchHit>? Documents { get; set; }
    }

    private sealed class SearchHit
    {
        [JsonPropertyName("_source")]
        public ElasticStoredLogDocument? Source { get; set; }
    }

    private readonly record struct ResourceKey(string ServiceName, string ServiceInstanceId, string ServiceVersion);
}
