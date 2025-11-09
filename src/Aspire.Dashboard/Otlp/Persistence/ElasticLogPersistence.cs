// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
namespace Aspire.Dashboard.Otlp.Persistence;

internal sealed class ElasticLogPersistence : ILogPersistence, IDisposable
{
    private static readonly System.Text.Json.JsonSerializerOptions s_serializerOptions = new System.Text.Json.JsonSerializerOptions
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    private readonly Microsoft.Extensions.Options.IOptionsMonitor<Aspire.Dashboard.Configuration.DashboardOptions> _optionsMonitor;
    private readonly Microsoft.Extensions.Logging.ILogger<ElasticLogPersistence> _logger;
    private readonly object _httpClientLock = new();
    private HttpClient? _httpClient;
    private HttpMessageHandler? _httpHandler;
    private string? _clientKey;

    public ElasticLogPersistence(Microsoft.Extensions.Options.IOptionsMonitor<Aspire.Dashboard.Configuration.DashboardOptions> optionsMonitor, Microsoft.Extensions.Logging.ILogger<ElasticLogPersistence> logger)
    {
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    public bool IsEnabled => _optionsMonitor.CurrentValue.LogStorage.Mode == Aspire.Dashboard.Configuration.LogStorageMode.Elasticsearch;

    public void Persist(IReadOnlyCollection<Aspire.Dashboard.Otlp.Model.OtlpLogEntry> entries, CancellationToken cancellationToken)
    {
        if (!IsEnabled || entries.Count == 0)
        {
            return;
        }

        var options = _optionsMonitor.CurrentValue.LogStorage.Elasticsearch;
        if (string.IsNullOrWhiteSpace(options.Endpoint) || string.IsNullOrWhiteSpace(options.DataStream))
        {
            _logger.LogWarning("Elasticsearch persistence is enabled but endpoint or data stream is not configured.");
            return;
        }

        var httpClient = GetClient(options);
        var payload = BuildBulkPayload(entries, options);

        using var request = new HttpRequestMessage(HttpMethod.Post, $"{options.DataStream}/_bulk?refresh=false")
        {
            Content = new StringContent(payload, System.Text.Encoding.UTF8, "application/x-ndjson")
        };

        ConfigureRequest(request, options);

        try
        {
            using var response = httpClient.SendAsync(request, cancellationToken).GetAwaiter().GetResult();
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Failed to persist {Count} log entries to Elasticsearch. Status code: {StatusCode}", entries.Count, response.StatusCode);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist {Count} log entries to Elasticsearch.", entries.Count);
        }
    }

    public IReadOnlyList<PersistedLogHit> GetLatest(int count, CancellationToken cancellationToken)
    {
        if (!IsEnabled || count <= 0)
        {
            return Array.Empty<PersistedLogHit>();
        }

        var options = _optionsMonitor.CurrentValue.LogStorage.Elasticsearch;
        if (string.IsNullOrWhiteSpace(options.Endpoint) || string.IsNullOrWhiteSpace(options.DataStream))
        {
            return Array.Empty<PersistedLogHit>();
        }

        var httpClient = GetClient(options);
        var body = new
        {
            size = Math.Min(count, options.InitialLoadCount),
            sort = new[]
            {
                new Dictionary<string, object>
                {
                    ["@timestamp"] = new Dictionary<string, string> { ["order"] = "desc" }
                }
            }
        };

        return ExecuteSearch(httpClient, options, body, cancellationToken, reverseResults: true);
    }

    public IReadOnlyList<PersistedLogHit> GetSince(DateTime timestampExclusive, int maxCount, CancellationToken cancellationToken)
    {
        if (!IsEnabled || maxCount <= 0)
        {
            return Array.Empty<PersistedLogHit>();
        }

        var options = _optionsMonitor.CurrentValue.LogStorage.Elasticsearch;
        if (string.IsNullOrWhiteSpace(options.Endpoint) || string.IsNullOrWhiteSpace(options.DataStream))
        {
            return Array.Empty<PersistedLogHit>();
        }

        var httpClient = GetClient(options);
        var body = new
        {
            size = Math.Min(maxCount, options.IncrementalBatchSize),
            sort = new[]
            {
                new Dictionary<string, object>
                {
                    ["@timestamp"] = new Dictionary<string, string> { ["order"] = "asc" }
                }
            },
            query = new Dictionary<string, object>
            {
                ["range"] = new Dictionary<string, object>
                {
                    ["@timestamp"] = new Dictionary<string, string>
                    {
                        ["gte"] = timestampExclusive.ToUniversalTime().ToString("O")
                    }
                }
            }
        };

        return ExecuteSearch(httpClient, options, body, cancellationToken, reverseResults: false);
    }

    public void Dispose()
    {
        lock (_httpClientLock)
        {
            _httpClient?.Dispose();
            _httpClient = null;

            _httpHandler?.Dispose();
            _httpHandler = null;

            _clientKey = null;
        }
    }

    private IReadOnlyList<Aspire.Dashboard.Otlp.Persistence.PersistedLogHit> ExecuteSearch(HttpClient httpClient, Aspire.Dashboard.Configuration.ElasticsearchLogStorageOptions options, object body, CancellationToken cancellationToken, bool reverseResults)
    {
        var requestContent = System.Text.Json.JsonSerializer.Serialize(body, s_serializerOptions);
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{options.DataStream}/_search")
        {
            Content = new StringContent(requestContent, System.Text.Encoding.UTF8, "application/json")
        };
        ConfigureRequest(request, options);

        try
        {
            using var response = httpClient.SendAsync(request, cancellationToken).GetAwaiter().GetResult();
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Failed to query logs from Elasticsearch. Status code: {StatusCode}", response.StatusCode);
                return Array.Empty<PersistedLogHit>();
            }

            using var stream = response.Content.ReadAsStream(cancellationToken);
            var envelope = System.Text.Json.JsonSerializer.Deserialize<SearchResponseEnvelope>(stream, s_serializerOptions);
            if (envelope?.Hits?.Hits is null || envelope.Hits.Hits.Count == 0)
            {
                return Array.Empty<PersistedLogHit>();
            }

            var hits = new List<Aspire.Dashboard.Otlp.Persistence.PersistedLogHit>(envelope.Hits.Hits.Count);
            foreach (var hit in envelope.Hits.Hits)
            {
                if (hit.Source is null)
                {
                    continue;
                }

                hits.Add(new Aspire.Dashboard.Otlp.Persistence.PersistedLogHit(hit.Id ?? string.Empty, hit.Source));
            }

            if (reverseResults)
            {
                hits.Reverse();
            }

            return hits;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query logs from Elasticsearch.");
            return Array.Empty<PersistedLogHit>();
        }
    }

    private static string BuildBulkPayload(IReadOnlyCollection<Aspire.Dashboard.Otlp.Model.OtlpLogEntry> entries, Aspire.Dashboard.Configuration.ElasticsearchLogStorageOptions options)
    {
        var builder = new System.Text.StringBuilder(entries.Count * 512);

        foreach (var entry in entries)
        {
            var document = MapToDocument(entry);
            builder.Append("{\"create\":{\"_index\":\"")
                   .Append(options.DataStream)
                   .Append("\"}}\n");
            builder.Append(System.Text.Json.JsonSerializer.Serialize(document, s_serializerOptions))
                   .Append('\n');
        }

        return builder.ToString();
    }

    private HttpClient GetClient(Aspire.Dashboard.Configuration.ElasticsearchLogStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.Endpoint))
        {
            throw new InvalidOperationException($"{nameof(options.Endpoint)} must be configured when log storage mode is set to Elasticsearch.");
        }

        var key = string.Join("|", options.Endpoint, options.Username, options.Password, options.DisableServerCertificateValidation);

        lock (_httpClientLock)
        {
            if (_httpClient != null && string.Equals(_clientKey, key, StringComparison.Ordinal))
            {
                return _httpClient;
            }

            _httpClient?.Dispose();
            _httpHandler?.Dispose();

            HttpMessageHandler handler = new HttpClientHandler
            {
                AutomaticDecompression = System.Net.DecompressionMethods.All
            };

            if (options.DisableServerCertificateValidation && handler is HttpClientHandler clientHandler)
            {
                clientHandler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
            }

            var client = new HttpClient(handler)
            {
                BaseAddress = new Uri(options.Endpoint, UriKind.Absolute),
                Timeout = TimeSpan.FromSeconds(30)
            };

            _httpClient = client;
            _httpHandler = handler;
            _clientKey = key;

            return client;
        }
    }

    private static void ConfigureRequest(HttpRequestMessage request, Aspire.Dashboard.Configuration.ElasticsearchLogStorageOptions options)
    {
        if (!string.IsNullOrEmpty(options.Username) && !string.IsNullOrEmpty(options.Password))
        {
            var token = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{options.Username}:{options.Password}"));
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", token);
        }
    }

    private static ElasticLogDocument MapToDocument(Aspire.Dashboard.Otlp.Model.OtlpLogEntry entry)
    {
        var resource = entry.ResourceView.Resource;

        var serviceAttributes = new List<ElasticNameValue>(entry.ResourceView.Properties.Length);
        foreach (var property in entry.ResourceView.Properties)
        {
            serviceAttributes.Add(new ElasticNameValue { Name = property.Key, Value = property.Value });
        }

        string? serviceVersion = null;
        foreach (var attribute in serviceAttributes)
        {
            if (Aspire.StringComparers.OtlpAttribute.Equals(attribute.Name, "service.version"))
            {
                serviceVersion = attribute.Value;
                break;
            }
        }

        var scopeAttributes = new List<ElasticNameValue>(entry.Scope.Attributes.Length);
        foreach (var attribute in entry.Scope.Attributes)
        {
            scopeAttributes.Add(new ElasticNameValue { Name = attribute.Key, Value = attribute.Value });
        }

        var logAttributes = new List<ElasticNameValue>(entry.Attributes.Length);
        foreach (var attribute in entry.Attributes)
        {
            logAttributes.Add(new ElasticNameValue { Name = attribute.Key, Value = attribute.Value });
        }

        return new ElasticLogDocument
        {
            Timestamp = entry.TimeStamp,
            Flags = entry.Flags,
            Severity = entry.Severity.ToString(),
            Message = entry.Message,
            SpanId = entry.SpanId,
            TraceId = entry.TraceId,
            ParentId = entry.ParentId,
            OriginalFormat = entry.OriginalFormat,
            ServiceName = resource.ResourceName,
            ServiceInstanceId = resource.InstanceId,
            ServiceVersion = serviceVersion,
            ServiceAttributes = serviceAttributes,
            Attributes = logAttributes,
            ScopeName = entry.Scope.Name,
            ScopeVersion = entry.Scope.Version,
            ScopeAttributes = scopeAttributes,
            LogId = entry.InternalId
        };
    }

    private sealed class SearchResponseEnvelope
    {
        [System.Text.Json.Serialization.JsonPropertyName("hits")]
        public HitsContainer? Hits { get; set; }
    }

    private sealed class HitsContainer
    {
        [System.Text.Json.Serialization.JsonPropertyName("hits")]
        public List<Hit> Hits { get; set; } = new();
    }

    private sealed class Hit
    {
        [System.Text.Json.Serialization.JsonPropertyName("_id")]
        public string? Id { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("_source")]
        public ElasticLogDocument? Source { get; set; }
    }
}
