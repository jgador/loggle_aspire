// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Elasticsearch;
using Aspire.Dashboard.Model.Otlp;
using Aspire.Dashboard.Otlp.Model;
using Aspire.Dashboard.Otlp.Storage;
using LogLevelAlias = Microsoft.Extensions.Logging.LogLevel;

namespace Aspire.Dashboard.Model;

public class StructuredLogsViewModel
{
    private readonly TelemetryRepository _telemetryRepository;
    private readonly ILogsDataSource? _logsDataSource;
    private readonly List<FieldTelemetryFilter> _filters = new();

    private PagedResult<OtlpLogEntry>? _logs;
    private ResourceKey? _resourceKey;
    private string _filterText = string.Empty;
    private int _logsStartIndex;
    private int _logsCount;
    private LogLevelAlias? _logLevel;
    private bool _currentDataHasErrors;

    public StructuredLogsViewModel(TelemetryRepository telemetryRepository, ILogsDataSource? logsDataSource = null)
    {
        _telemetryRepository = telemetryRepository;
        _logsDataSource = logsDataSource;
    }

    public ResourceKey? ResourceKey { get => _resourceKey; set => SetValue(ref _resourceKey, value); }
    public string FilterText { get => _filterText; set => SetValue(ref _filterText, value); }
    public IReadOnlyList<FieldTelemetryFilter> Filters => _filters;

    public void ClearFilters()
    {
        _filters.Clear();
        _logs = null;
        _currentDataHasErrors = false;
    }

    public void AddFilter(FieldTelemetryFilter filter)
    {
        foreach (var existingFilter in _filters)
        {
            if (existingFilter.Equals(filter))
            {
                return;
            }
        }

        _filters.Add(filter);
        _logs = null;
        _currentDataHasErrors = false;
    }

    public bool RemoveFilter(FieldTelemetryFilter filter)
    {
        if (_filters.Remove(filter))
        {
            _logs = null;
            _currentDataHasErrors = false;
            return true;
        }

        return false;
    }

    public int StartIndex { get => _logsStartIndex; set => SetValue(ref _logsStartIndex, value); }
    public int Count { get => _logsCount; set => SetValue(ref _logsCount, value); }
    public LogLevelAlias? LogLevel { get => _logLevel; set => SetValue(ref _logLevel, value); }

    private void SetValue<T>(ref T field, T value)
    {
        if (EqualityComparer<T>.Default.Equals(field, value))
        {
            return;
        }

        field = value;
        _logs = null;
        _currentDataHasErrors = false;
    }

    public PagedResult<OtlpLogEntry> GetLogs()
    {
        var logs = _logs;
        if (logs == null)
        {
            logs = FetchLogs(StartIndex, Count, includeErrorFilter: false);
            _logs = logs;
        }

        return logs;
    }

    public bool HasErrors() => _currentDataHasErrors;

    public PagedResult<OtlpLogEntry> GetErrorLogs(int count) => FetchLogs(startIndex: 0, count, includeErrorFilter: true);

    public List<TelemetryFilter> GetFilters() => BuildFieldFilters(includeErrorFilter: false).Cast<TelemetryFilter>().ToList();

    public void ClearData()
    {
        _logs = null;
        _currentDataHasErrors = false;
    }

    private PagedResult<OtlpLogEntry> FetchLogs(int startIndex, int count, bool includeErrorFilter)
    {
        var fieldFilters = BuildFieldFilters(includeErrorFilter);

        if (_logsDataSource is null)
        {
            var context = new GetLogsContext
            {
                ResourceKey = ResourceKey,
                StartIndex = startIndex,
                Count = count,
                Filters = fieldFilters.Cast<TelemetryFilter>().ToList()
            };

            var result = _telemetryRepository.GetLogs(context);
            _currentDataHasErrors = result.Items.Any(log => log.IsError);
            return result;
        }

        var parameters = new LogQueryParameters
        {
            ResourceKey = ResourceKey,
            StartIndex = startIndex,
            Count = count,
            MessageContains = string.IsNullOrWhiteSpace(FilterText) ? null : FilterText,
            MinimumLogLevel = includeErrorFilter ? LogLevelAlias.Error : _logLevel,
            Filters = fieldFilters
        };

        var esResult = _logsDataSource.GetLogsAsync(parameters, CancellationToken.None).GetAwaiter().GetResult();
        _currentDataHasErrors = esResult.Items.Any(log => log.IsError);
        return esResult;
    }

    private List<FieldTelemetryFilter> BuildFieldFilters(bool includeErrorFilter)
    {
        var filters = _filters.Select(f => new FieldTelemetryFilter
        {
            Field = f.Field,
            Condition = f.Condition,
            Value = f.Value
        }).ToList();

        if (!string.IsNullOrWhiteSpace(FilterText))
        {
            filters.Add(new FieldTelemetryFilter
            {
                Field = nameof(OtlpLogEntry.Message),
                Condition = FilterCondition.Contains,
                Value = FilterText
            });
        }

        if (!includeErrorFilter && _logLevel is { } level && level != LogLevelAlias.Trace)
        {
            filters.Add(new FieldTelemetryFilter
            {
                Field = nameof(OtlpLogEntry.Severity),
                Condition = FilterCondition.GreaterThanOrEqual,
                Value = level.ToString()
            });
        }

        if (includeErrorFilter)
        {
            filters.RemoveAll(f => f.Field == nameof(OtlpLogEntry.Severity));
            filters.Add(new FieldTelemetryFilter
            {
                Field = nameof(OtlpLogEntry.Severity),
                Condition = FilterCondition.GreaterThanOrEqual,
                Value = LogLevelAlias.Error.ToString()
            });
        }

        return filters;
    }
}
