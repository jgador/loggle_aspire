#pragma warning disable CA2007
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Aspire.Dashboard.Configuration;
using Aspire.Dashboard.Model.Otlp;
using Aspire.Dashboard.Otlp.Model;
using Aspire.Dashboard.Otlp.Storage;
using Google.Protobuf.Collections;
using Microsoft.Extensions.Options;
using OpenTelemetry.Proto.Common.V1;

namespace Aspire.Dashboard.Elasticsearch;

internal sealed class ElasticsearchLogsDataSource : ILogsDataSource, IDisposable
{
    private const string JsonMediaType = "application/json";
    private static readonly JsonSerializerOptions s_serializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly ElasticsearchLogsOptions _options;
    private readonly ILogger<ElasticsearchLogsDataSource> _logger;
    private readonly TelemetryLimitOptions _limits;
    private readonly OtlpContext _otlpContext;

    public ElasticsearchLogsDataSource(
        IOptions<ElasticsearchLogsOptions> options,
        IOptions<DashboardOptions> dashboardOptions,
        ILogger<ElasticsearchLogsDataSource> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dashboardOptions);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options.Value ?? new ElasticsearchLogsOptions();
        if (string.IsNullOrWhiteSpace(_options.Endpoint))
        {
            throw new InvalidOperationException("ElasticsearchLogs.Endpoint configuration value is required.");
        }

        _logger = logger;
        _limits = dashboardOptions.Value.TelemetryLimits;

        _otlpContext = new OtlpContext
        {
            Logger = logger,
            Options = _limits
        };

        var handler = new HttpClientHandler();
        if (_options.DisableCertificateValidation)
        {
            handler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
        }

        _httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri(_options.Endpoint, UriKind.Absolute),
            Timeout = TimeSpan.FromSeconds(30)
        };

        if (!string.IsNullOrEmpty(_options.ApiKey))
        {
            _httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("ApiKey", _options.ApiKey);
        }
        else if (!string.IsNullOrEmpty(_options.Username))
        {
            var credentialBytes = Encoding.UTF8.GetBytes($"{_options.Username}:{_options.Password ?? string.Empty}");
            var headerValue = System.Convert.ToBase64String(credentialBytes);
            _httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", headerValue);
        }
    }

    public async Task<PagedResult<OtlpLogEntry>> GetLogsAsync(LogQueryParameters parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        var payload = BuildSearchPayload(parameters);
        var requestUri = string.IsNullOrEmpty(_options.Index)
            ? "_search"
            : $"{_options.Index.TrimEnd('/')}/_search";

        using var content = new StringContent(payload, Encoding.UTF8, JsonMediaType);
        using var response = await _httpClient.PostAsync(requestUri, content, cancellationToken).ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogError("Elasticsearch search request failed with status {StatusCode}: {Body}", response.StatusCode, body);
            response.EnsureSuccessStatusCode();
        }

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var searchResponse = await JsonSerializer.DeserializeAsync<SearchResponse>(stream, s_serializerOptions, cancellationToken).ConfigureAwait(false);

        if (searchResponse?.Hits?.Documents is null || searchResponse.Hits.Documents.Count == 0)
        {
            return PagedResult<OtlpLogEntry>.Empty;
        }

        var items = new List<OtlpLogEntry>(searchResponse.Hits.Documents.Count);
        foreach (var hit in searchResponse.Hits.Documents)
        {
            if (hit.Source is null)
            {
                continue;
            }

            try
            {
                items.Add(Convert(hit.Source));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to convert Elasticsearch log document.");
            }
        }

        var total = searchResponse.Hits.Total?.Value ?? items.Count;
        if (total < 0)
        {
            total = items.Count;
        }

        var cappedTotal = total > int.MaxValue ? int.MaxValue : (int)total;
        var isFull = total >= _limits.MaxLogCount;

        return new PagedResult<OtlpLogEntry>
        {
            TotalItemCount = cappedTotal,
            Items = items,
            IsFull = isFull
        };
    }

    private string BuildSearchPayload(LogQueryParameters parameters)
    {
        var mustArray = new JsonArray();

        if (parameters.ResourceKey is { } resourceKey)
        {
            mustArray.Add(CreateTermQuery("serviceName.keyword", resourceKey.Name));
            if (!string.IsNullOrEmpty(resourceKey.InstanceId))
            {
                mustArray.Add(CreateTermQuery("serviceInstanceId.keyword", resourceKey.InstanceId));
            }
        }

        if (!string.IsNullOrWhiteSpace(parameters.MessageContains))
        {
            mustArray.Add(new JsonObject
            {
                ["match"] = new JsonObject
                {
                    ["message"] = new JsonObject
                    {
                        ["query"] = parameters.MessageContains
                    }
                }
            });
        }

        if (parameters.MinimumLogLevel is { } minLevel)
        {
            var levels = GetLogLevelsGreaterThanOrEqual(minLevel)
                .Select(level => JsonValue.Create(level.ToString()))
                .ToArray();

            if (levels.Length > 0)
            {
                var array = new JsonArray();
                foreach (var value in levels)
                {
                    array.Add(value);
                }

                mustArray.Add(new JsonObject
                {
                    ["terms"] = new JsonObject
                    {
                        ["logLevel.keyword"] = array
                    }
                });
            }
        }

        foreach (var filter in parameters.Filters)
        {
            if (TryCreateFilterQuery(filter, out var query))
            {
                mustArray.Add(query);
            }
            else
            {
                _logger.LogDebug("Skipping unsupported structured log filter: Field = {Field}, Condition = {Condition}", filter.Field, filter.Condition);
            }
        }

        JsonNode queryNode;
        if (mustArray.Count == 0)
        {
            queryNode = new JsonObject
            {
                ["match_all"] = new JsonObject()
            };
        }
        else
        {
            queryNode = new JsonObject
            {
                ["bool"] = new JsonObject
                {
                    ["must"] = mustArray
                }
            };
        }

        var sort = new JsonArray
        {
            new JsonObject
            {
                ["@timestamp"] = new JsonObject
                {
                    ["order"] = "desc"
                }
            }
        };

        var root = new JsonObject
        {
            ["from"] = parameters.StartIndex,
            ["size"] = parameters.Count,
            ["sort"] = sort,
            ["query"] = queryNode
        };

        return root.ToJsonString(new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });
    }

    private static JsonObject CreateTermQuery(string field, string value)
    {
        return new JsonObject
        {
            ["term"] = new JsonObject
            {
                [field] = value
            }
        };
    }

    private static JsonObject CreateContainsQuery(string field, string value)
    {
        return new JsonObject
        {
            ["wildcard"] = new JsonObject
            {
                [field] = $"*{EscapeWildcard(value)}*"
            }
        };
    }

    private static string EscapeWildcard(string value)
    {
        return value
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("*", "\\*", StringComparison.Ordinal)
            .Replace("?", "\\?", StringComparison.Ordinal);
    }

    private static bool TryCreateFilterQuery(FieldTelemetryFilter filter, out JsonObject query)
    {
        query = null!;
        if (string.IsNullOrWhiteSpace(filter.Value))
        {
            return false;
        }

        return filter.Field switch
        {
            nameof(OtlpLogEntry.Message) or KnownStructuredLogFields.MessageField => TryCreateTextQuery("message", filter, out query),
            nameof(OtlpLogEntry.TraceId) or KnownStructuredLogFields.TraceIdField => TryCreateTextQuery("traceId.keyword", filter, out query, exact: true),
            nameof(OtlpLogEntry.SpanId) or KnownStructuredLogFields.SpanIdField => TryCreateTextQuery("spanId.keyword", filter, out query, exact: true),
            nameof(OtlpLogEntry.OriginalFormat) or KnownStructuredLogFields.OriginalFormatField => TryCreateTextQuery("originalFormat", filter, out query),
            KnownResourceFields.ServiceNameField => TryCreateTextQuery("serviceName.keyword", filter, out query, exact: true),
            KnownStructuredLogFields.CategoryField => TryCreateTextQuery("scopeName.keyword", filter, out query, exact: true),
            nameof(OtlpLogEntry.Severity) => TryCreateSeverityQuery(filter, out query),
            _ => false
        };
    }

    private static bool TryCreateTextQuery(string field, FieldTelemetryFilter filter, out JsonObject query, bool exact = false)
    {
        query = null!;
        return filter.Condition switch
        {
            FilterCondition.Equals => (query = exact ? CreateTermQuery(field, filter.Value) : CreateMatchPhraseQuery(field, filter.Value)) != null,
            FilterCondition.Contains => (query = exact ? CreateContainsQuery(field, filter.Value) : CreateMatchQuery(field, filter.Value)) != null,
            _ => false
        };
    }

    private static JsonObject CreateMatchPhraseQuery(string field, string value)
    {
        return new JsonObject
        {
            ["match_phrase"] = new JsonObject
            {
                [field] = value
            }
        };
    }

    private static JsonObject CreateMatchQuery(string field, string value)
    {
        return new JsonObject
        {
            ["match"] = new JsonObject
            {
                [field] = new JsonObject
                {
                    ["query"] = value
                }
            }
        };
    }

    private static bool TryCreateSeverityQuery(FieldTelemetryFilter filter, out JsonObject query)
    {
        query = null!;

        if (filter.Condition == FilterCondition.Equals && Enum.TryParse<LogLevel>(filter.Value, ignoreCase: true, out var level))
        {
            return TryCreateTextQuery("logLevel.keyword", new FieldTelemetryFilter
            {
                Field = filter.Field,
                Condition = FilterCondition.Equals,
                Value = level.ToString()
            }, out query, exact: true);
        }

        if (filter.Condition == FilterCondition.GreaterThanOrEqual && Enum.TryParse<LogLevel>(filter.Value, ignoreCase: true, out var threshold))
        {
            var levels = GetLogLevelsGreaterThanOrEqual(threshold)
                .Select(l => JsonValue.Create(l.ToString()))
                .ToArray();

            if (levels.Length == 0)
            {
                return false;
            }

            var array = new JsonArray();
            foreach (var value in levels)
            {
                array.Add(value);
            }

            query = new JsonObject
            {
                ["terms"] = new JsonObject
                {
                    ["logLevel.keyword"] = array
                }
            };
            return true;
        }

        return false;
    }

    private static IEnumerable<LogLevel> GetLogLevelsGreaterThanOrEqual(LogLevel minimum)
    {
        foreach (var level in new[]
                 {
                     LogLevel.Trace,
                     LogLevel.Debug,
                     LogLevel.Information,
                     LogLevel.Warning,
                     LogLevel.Error,
                     LogLevel.Critical
                 })
        {
            if (level >= minimum)
            {
                yield return level;
            }
        }
    }

    private OtlpLogEntry Convert(ElasticsearchLogDocument document)
    {
        var resource = new OtlpResource(
            document.ServiceName ?? "unknown_service",
            string.IsNullOrWhiteSpace(document.ServiceInstanceId) ? null : document.ServiceInstanceId,
            uninstrumentedPeer: false,
            _otlpContext);

        var resourceAttributes = ConvertToRepeatedField(document.ServiceAttributes);
        var resourceView = new OtlpResourceView(resource, resourceAttributes);

        var scopeAttributes = ConvertToPairs(document.ScopeAttributes);
        var scope = new OtlpScope(
            document.ScopeName ?? string.Empty,
            document.ScopeVersion ?? string.Empty,
            scopeAttributes);

        var attributes = ConvertToPairs(document.Attributes);

        return new OtlpLogEntry(
            document.TimeStamp,
            document.Flags,
            ResolveLogLevel(document.LogLevel),
            document.Message ?? string.Empty,
            document.SpanId ?? string.Empty,
            document.TraceId ?? string.Empty,
            document.ParentId ?? string.Empty,
            document.OriginalFormat,
            resourceView,
            scope,
            attributes);
    }

    private static LogLevel ResolveLogLevel(string? logLevel)
    {
        if (string.IsNullOrWhiteSpace(logLevel))
        {
            return LogLevel.None;
        }

        if (Enum.TryParse<LogLevel>(logLevel, ignoreCase: true, out var parsed))
        {
            return parsed;
        }

        return LogLevel.None;
    }

    private static KeyValuePair<string, string>[] ConvertToPairs(List<ElasticsearchNameValue>? values)
    {
        if (values is null || values.Count == 0)
        {
            return Array.Empty<KeyValuePair<string, string>>();
        }

        var result = new KeyValuePair<string, string>[values.Count];
        for (var i = 0; i < values.Count; i++)
        {
            result[i] = new KeyValuePair<string, string>(values[i].Name, values[i].Value);
        }

        return result;
    }

    private static RepeatedField<KeyValue> ConvertToRepeatedField(List<ElasticsearchNameValue>? values)
    {
        var field = new RepeatedField<KeyValue>();
        if (values is null)
        {
            return field;
        }

        foreach (var item in values)
        {
            field.Add(new KeyValue
            {
                Key = item.Name,
                Value = new AnyValue { StringValue = item.Value }
            });
        }

        return field;
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }

    private sealed class SearchResponse
    {
        [JsonPropertyName("hits")]
        public HitsMetadata? Hits { get; set; }
    }

    private sealed class HitsMetadata
    {
        [JsonPropertyName("total")]
        public TotalCount? Total { get; set; }

        [JsonPropertyName("hits")]
        public List<SearchHit> Documents { get; set; } = new();
    }

    private sealed class TotalCount
    {
        [JsonPropertyName("value")]
        public long Value { get; set; }
    }

    private sealed class SearchHit
    {
        [JsonPropertyName("_source")]
        public ElasticsearchLogDocument? Source { get; set; }
    }
}
#pragma warning restore CA2007

