using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>A ClickHouse table, view or query read as one Arrow stream. The schema is ClickHouse's
/// own: <c>DESCRIBE</c> decides which columns are cast, then the real projection runs with
/// <c>LIMIT 0</c> and the empty stream's schema message is the declared schema, so declared and
/// produced schemas cannot drift. Owns its <see cref="ChHttpClient"/>; the engine disposes the
/// source exactly once.</summary>
internal sealed class ChSource(ChHttpClient client, ILogger logger) : ISource
{
    private readonly ConcurrentDictionary<string, IReadOnlyList<ChColumn>> _columns = new(StringComparer.Ordinal);

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var config = ParseConfig(spec);
        var columns = await ColumnsAsync(spec, config, ct).ConfigureAwait(false);
        var probe = ChReadPlan.Build(config, spec, ReadHints.None, columns, pushPredicate: false, limitZero: true, client.Config.Redactor);
        return new DatasetSchema(await ProbeAsync(probe, config, Context(spec, config), ct).ConfigureAwait(false));
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var config = ParseConfig(spec);
        var context = Context(spec, config);
        var columns = await ColumnsAsync(spec, config, ct).ConfigureAwait(false);
        var push = config.Table is not null && !string.IsNullOrEmpty(hints.PredicateSql);

        if (push)
        {
            // DuckDB's dialect is not ClickHouse's: run the read's own shape with LIMIT 0 first. A
            // predicate ClickHouse rejects is dropped -- always safe, DuckDB re-applies the pipeline's
            // SQL to whatever lands -- provided the read without it is valid.
            var probe = ChReadPlan.Build(config, spec, hints, columns, pushPredicate: true, limitZero: true, client.Config.Redactor);
            try
            {
                await ProbeAsync(probe, config, context, ct).ConfigureAwait(false);
            }
            catch (PzConnectorException ex) when (!ex.IsTransient)
            {
                var without = ChReadPlan.Build(config, spec, hints, columns, pushPredicate: false, limitZero: true, client.Config.Redactor);
                await ProbeAsync(without, config, context, ct).ConfigureAwait(false);
                logger.LogWarning("clickhouse: {Context}: ClickHouse rejected the pushed-down filter, so the read is unfiltered and pz filters locally ({Reason})",
                    context, ex.Message);
                push = false;
            }
        }

        var plan = ChReadPlan.Build(config, spec, hints, columns, push, limitZero: false, client.Config.Redactor);
        return [new ChPartition(client, plan, config, context, logger)];
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<IReadOnlyList<ChColumn>> ColumnsAsync(DatasetSpec spec, ChReadConfig config, CancellationToken ct)
    {
        var key = $"{spec.Dataset}|{config.Table?.Quoted ?? config.Query}";
        if (_columns.TryGetValue(key, out var cached))
        {
            return cached;
        }

        var context = Context(spec, config);
        var rows = await client.QueryRowsAsync(ChReadPlan.DescribeSql(config), $"{context}: describing", ChCodes.Read_QueryFailed, ct).ConfigureAwait(false);
        var columns = ChReadPlan.Columns(rows, client.Config.Redactor, context);
        _columns[key] = columns;
        return columns;
    }

    private async Task<Apache.Arrow.Schema> ProbeAsync(ChReadPlan plan, ChReadConfig config, string context, CancellationToken ct)
    {
        using var response = await client.SendAsync(new ChRequest($"{context}: probing", ChCodes.Read_QueryFailed)
        {
            Sql = plan.Sql,
            Parameters = plan.WireParameters,
            Settings = ChReadPlan.Settings(config, null),
        }, ct).ConfigureAwait(false);
        await using var body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        return await ChArrowResponseReader.ReadSchemaAsync(body, client.Config.Redactor, context, ct).ConfigureAwait(false);
    }

    private ChReadConfig ParseConfig(DatasetSpec spec)
    {
        var errors = new List<string>();
        return ChReadConfig.Parse(spec, client.Config, errors)
            ?? throw new PzConnectorException(string.Join("; ", errors), isTransient: false);
    }

    private static string Context(DatasetSpec spec, ChReadConfig config) => $"reading dataset '{spec.Dataset}' ({config.Label})";
}
