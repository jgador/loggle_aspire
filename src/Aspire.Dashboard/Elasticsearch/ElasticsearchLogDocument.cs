// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Text.Json.Serialization;

namespace Aspire.Dashboard.Elasticsearch;

internal sealed class ElasticsearchLogDocument
{
    [JsonPropertyName("@timestamp")]
    public DateTime TimeStamp { get; set; }

    [JsonPropertyName("serviceName")]
    public string? ServiceName { get; set; }

    [JsonPropertyName("serviceInstanceId")]
    public string? ServiceInstanceId { get; set; }

    [JsonPropertyName("serviceVersion")]
    public string? ServiceVersion { get; set; }

    [JsonPropertyName("flags")]
    public uint Flags { get; set; }

    [JsonPropertyName("logLevel")]
    public string? LogLevel { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("spanId")]
    public string? SpanId { get; set; }

    [JsonPropertyName("traceId")]
    public string? TraceId { get; set; }

    [JsonPropertyName("parentId")]
    public string? ParentId { get; set; }

    [JsonPropertyName("originalFormat")]
    public string? OriginalFormat { get; set; }

    [JsonPropertyName("attributes")]
    public List<ElasticsearchNameValue>? Attributes { get; set; }

    [JsonPropertyName("serviceAttributes")]
    public List<ElasticsearchNameValue>? ServiceAttributes { get; set; }

    [JsonPropertyName("scopeAttributes")]
    public List<ElasticsearchNameValue>? ScopeAttributes { get; set; }

    [JsonPropertyName("scopeName")]
    public string? ScopeName { get; set; }

    [JsonPropertyName("scopeVersion")]
    public string? ScopeVersion { get; set; }
}

internal sealed class ElasticsearchNameValue
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;

    [JsonPropertyName("value")]
    public string Value { get; set; } = string.Empty;
}
