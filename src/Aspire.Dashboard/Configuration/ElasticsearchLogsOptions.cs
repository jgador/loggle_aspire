// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

namespace Aspire.Dashboard.Configuration;

/// <summary>
/// Configuration used by the dashboard to read structured logs directly from Elasticsearch.
/// </summary>
public sealed class ElasticsearchLogsOptions
{
    public const string SectionName = "ElasticsearchLogs";

    /// <summary>
    /// Base URI for the Elasticsearch cluster (e.g. http://localhost:9200).
    /// </summary>
    public string? Endpoint { get; set; }

    /// <summary>
    /// Optional Elasticsearch index or data stream that stores the structured logs.
    /// When omitted the default index pattern from the client configuration is used.
    /// </summary>
    public string? Index { get; set; }

    /// <summary>
    /// Optional username for basic authentication.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Optional password for basic authentication.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Optional API key authentication header value (in the format id:api_key).
    /// When provided it takes precedence over username/password.
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// When true TLS certificate validation is disabled. Only use for development scenarios.
    /// </summary>
    public bool DisableCertificateValidation { get; set; }
}
