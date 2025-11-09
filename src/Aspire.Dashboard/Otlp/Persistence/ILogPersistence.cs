// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Model;

namespace Aspire.Dashboard.Otlp.Persistence;

public interface ILogPersistence
{
    bool IsEnabled { get; }

    void Persist(IReadOnlyCollection<OtlpLogEntry> entries, CancellationToken cancellationToken);

    IReadOnlyList<PersistedLogHit> GetLatest(int count, CancellationToken cancellationToken);

    IReadOnlyList<PersistedLogHit> GetSince(DateTime timestampExclusive, int maxCount, CancellationToken cancellationToken);
}
