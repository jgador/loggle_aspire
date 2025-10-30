// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Text.Json.Serialization;

namespace Aspire.Dashboard.Persistence;

internal sealed class ElasticStoredLogDocument
{
    [JsonPropertyName("@timestamp")]
    public DateTime Timestamp { get; set; }

    [JsonPropertyName("serviceName")]
    public string? ServiceName { get; set; }

    [JsonPropertyName("serviceInstanceId")]
    public string? ServiceInstanceId { get; set; }

    [JsonPropertyName("serviceVersion")]
    public string? ServiceVersion { get; set; }

    [JsonPropertyName("serviceAttributes")]
    public List<ElasticNameValue>? ServiceAttributes { get; set; }

    [JsonPropertyName("attributes")]
    public List<ElasticNameValue>? Attributes { get; set; }

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
}

internal sealed class ElasticNameValue
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("value")]
    public string? Value { get; set; }
}
