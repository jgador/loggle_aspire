// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

namespace Aspire.Dashboard.Persistence;

public class ElasticPersistenceOptions
{
    public const string SectionName = "Loggle:Elastic";

    public string? Url { get; set; }

    public string DataStream { get; set; } = "logs-loggle-default";

    /// <summary>
    /// Maximum number of log documents to hydrate into the in-memory telemetry store during dashboard startup.
    /// </summary>
    public int PreloadLogCount { get; set; } = 5000;

    /// <summary>
    /// Optional basic-auth username when connecting to Elasticsearch.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Optional basic-auth password when connecting to Elasticsearch.
    /// </summary>
    public string? Password { get; set; }

    public TimeSpan LookbackWindow { get; set; } = TimeSpan.FromHours(24);

    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Url) &&
        !string.IsNullOrWhiteSpace(DataStream) &&
        PreloadLogCount > 0;
}
