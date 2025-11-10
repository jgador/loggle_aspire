// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Model;
using Aspire.Dashboard.Otlp.Storage;

namespace Aspire.Dashboard.Elasticsearch;

public interface ILogsDataSource
{
    System.Threading.Tasks.Task<PagedResult<OtlpLogEntry>> GetLogsAsync(LogQueryParameters parameters, System.Threading.CancellationToken cancellationToken);
}
