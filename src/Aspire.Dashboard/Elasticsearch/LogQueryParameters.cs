#pragma warning disable IDE0005
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Model.Otlp;
using Aspire.Dashboard.Otlp.Storage;
using Microsoft.Extensions.Logging;

namespace Aspire.Dashboard.Elasticsearch;

public sealed class LogQueryParameters
{
    public ResourceKey? ResourceKey { get; init; }
    public int StartIndex { get; init; }
    public int Count { get; init; }
    public string? MessageContains { get; init; }
    public LogLevel? MinimumLogLevel { get; init; }
    public IReadOnlyList<FieldTelemetryFilter> Filters { get; init; } = Array.Empty<FieldTelemetryFilter>();
}

#pragma warning restore IDE0005
