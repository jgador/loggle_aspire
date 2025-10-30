// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Microsoft.AspNetCore.Components.Server.Circuits;

namespace Aspire.Dashboard.Persistence;

/// <summary>
/// Tracks when the first interactive Blazor circuit is established so that background services can delay work
/// that relies on the front-end being fully ready.
/// </summary>
internal sealed class DashboardCircuitTracker : CircuitHandler
{
    private readonly TaskCompletionSource _firstCircuitTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public override Task OnCircuitOpenedAsync(Circuit circuit, CancellationToken cancellationToken)
    {
        _firstCircuitTcs.TrySetResult();
        return Task.CompletedTask;
    }

    public Task WaitForFirstCircuitAsync(CancellationToken cancellationToken)
    {
        return _firstCircuitTcs.Task.WaitAsync(cancellationToken);
    }
}
