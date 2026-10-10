using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

internal sealed record ChWriteTarget(ChTableRef Target, ChTableInfo? Existing, ChTableInfo? Local, ChDdlContext Ddl)
{
    /// <summary>The table whose engine decides merge semantics: a Distributed target's local table.</summary>
    public ChTableInfo? Effective => Local ?? Existing;

    /// <summary>Where DDL and deletes are aimed: a Distributed table holds no data of its own.</summary>
    public ChTableRef DdlTable => Local?.Ref ?? Target;
}

/// <summary>Opens one write session per output: validates offline first (mode, keys, reserved
/// columns, options, every column's type), then looks the target up and creates the staging table.
/// Owns its <see cref="ChHttpClient"/>; the engine disposes the sink exactly once.</summary>
internal sealed class ChSink(ChHttpClient client, TimeProvider time, ILogger logger) : ISink
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private ChServerInfo? _server;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var cfg = client.Config;
        var context = $"output '{spec.Output}'";
        if (spec.Mode is not ("append" or "replace" or "merge"))
        {
            throw Refuse(ChCodes.Write_BadWriteMode, $"{context}: strategy '{spec.Mode}' is not supported; use append, replace or merge");
        }

        if (string.Equals(spec.SchemaPolicy, "evolve", StringComparison.Ordinal))
        {
            throw Refuse(ChCodes.Write_BadOutputOption, $"{context}: schema evolution is not supported; use 'fail_on_change' and ALTER the target by hand");
        }

        var names = schema.FieldsList.Select(f => f.Name).ToList();
        var reserved = names.Where(n => string.Equals(n, ChDdl.SequenceColumn, StringComparison.OrdinalIgnoreCase)
            || string.Equals(n, ChDdl.VersionColumn, StringComparison.OrdinalIgnoreCase)).ToList();
        if (reserved.Count > 0)
        {
            throw Refuse(ChCodes.Write_MergeKeys, $"{context}: column(s) {string.Join(", ", reserved.Select(r => $"'{r}'"))} are reserved by the sink; rename them in the pipeline");
        }

        if (spec.Mode == "merge")
        {
            if (spec.Keys.Count == 0)
            {
                throw Refuse(ChCodes.Write_MergeKeys, $"{context}: strategy: merge requires at least one key (keys:)");
            }

            var missing = spec.Keys.Where(k => !names.Contains(k, StringComparer.Ordinal)).ToList();
            if (missing.Count > 0)
            {
                throw Refuse(ChCodes.Write_MergeKeys, $"{context}: merge key(s) {string.Join(", ", missing.Select(k => $"'{k}'"))} are not in the write schema");
            }
        }

        var errors = new List<string>();
        var output = ChWriteConfig.Parse(spec, cfg, errors) ?? throw new PzConnectorException(string.Join("; ", errors), isTransient: false);

        var notNull = new HashSet<string>(spec.Keys.Concat(output.OrderBy ?? []), StringComparer.Ordinal);
        var typeErrors = new List<string>();
        var stagingColumns = new List<(string, string)>();
        var targetColumns = new List<(string, string)>();
        foreach (var field in schema.FieldsList)
        {
            var staged = ChTypes.ToDdl(field, forceNotNull: false, out var error);
            var created = ChTypes.ToDdl(field, forceNotNull: notNull.Contains(field.Name), out _);
            if (staged is null || created is null)
            {
                typeErrors.Add(error!);
                continue;
            }

            stagingColumns.Add((field.Name, staged));
            targetColumns.Add((field.Name, created));
        }

        if (typeErrors.Count > 0)
        {
            throw Refuse(ChCodes.Write_UnsupportedArrowType, $"{context}: {string.Join("; ", typeErrors)}");
        }

        var server = await ServerAsync(ct).ConfigureAwait(false);
        var ddl = new ChDdlContext(cfg.Cluster, server.IsCloud);
        var existing = await ChTopology.GetTableAsync(client, output.Target, ct).ConfigureAwait(false);
        var local = existing is { IsDistributed: true, DistributedLocal: { } localRef }
            ? await ChTopology.GetTableAsync(client, localRef, ct).ConfigureAwait(false)
            : null;
        var target = new ChWriteTarget(output.Target, existing, local, ddl);

        if (existing is not null)
        {
            var present = new HashSet<string>(existing.Columns.Select(c => c.Name), StringComparer.Ordinal);
            var absent = names.Where(n => !present.Contains(n)).ToList();
            if (absent.Count > 0)
            {
                throw Refuse(ChCodes.Write_TargetColumnMissing,
                    $"{context}: {output.Target.Display} has no column {string.Join(", ", absent.Select(a => $"'{a}'"))} -- add it (ALTER TABLE ... ADD COLUMN), or drop the table and let the sink recreate it");
            }

            if (spec.Mode == "replace" && target.Effective is { SupportsExchange: false } noExchange)
            {
                throw Refuse(ChCodes.Write_NoExchange,
                    $"{context}: database '{noExchange.Ref.Database}' uses the {noExchange.DatabaseEngine} engine, which has no EXCHANGE TABLES; replace needs an Atomic database");
            }
        }

        var merge = spec.Mode == "merge" ? ValidateMerge(spec, output, target, names, context) : null;

        var tag = Guid.NewGuid().ToString("N")[..16];
        var staging = output.Target.WithTable("_pz_stg_" + tag);
        if (spec.Mode == "merge")
        {
            stagingColumns.Add((ChDdl.SequenceColumn, "UInt64"));
        }

        await client.ExecuteAsync(ChDdl.CreateStaging(staging, ddl, stagingColumns, tag), $"{context}: creating the staging table",
            ChCodes.Write_CommitFailed, ct).ConfigureAwait(false);

        return new ChWriteSession(client, time, logger, spec, output, target, staging, tag, names, targetColumns, merge, context);
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        _gate.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Replacing (default): the effective table must be a ReplacingMergeTree; a version column
    /// the sink does not own must be in the write. delete_insert: any MergeTree engine.</summary>
    private ChMergePlan ValidateMerge(OutputSpec spec, ChWriteConfig output, ChWriteTarget target, IReadOnlyList<string> names, string context)
    {
        if (output.MergeStrategy == ChWriteConfig.DeleteInsert)
        {
            return new ChMergePlan(ChWriteConfig.DeleteInsert, SupplyVersion: false);
        }

        if (target.Effective is not { } effective)
        {
            // The sink creates the table with its own version column (unless engine: overrides it).
            return new ChMergePlan(ChWriteConfig.Replacing, SupplyVersion: output.Engine is null);
        }

        if (!effective.IsReplacing)
        {
            throw Refuse(ChCodes.Write_NotReplacing,
                $"{context}: {effective.Ref.Display} is a {effective.Engine}, which does not deduplicate; set merge_strategy: delete_insert, or recreate the table as ReplacingMergeTree");
        }

        var version = effective.ReplacingVersionColumn;
        if (version is null || version == ChDdl.VersionColumn)
        {
            return new ChMergePlan(ChWriteConfig.Replacing, SupplyVersion: version is not null);
        }

        if (!names.Contains(version, StringComparer.Ordinal))
        {
            throw Refuse(ChCodes.Write_TargetColumnMissing,
                $"{context}: {effective.Ref.Display} orders versions by column '{version}', which the write does not carry; add it to the pipeline's output");
        }

        return new ChMergePlan(ChWriteConfig.Replacing, SupplyVersion: false);
    }

    private async Task<ChServerInfo> ServerAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return _server ??= await ChTopology.ValidateDeploymentAsync(client, forWrites: true, ct).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private PzConnectorException Refuse(string code, string text) => new(ChCodes.Message(code, client.Config.Redactor, text), isTransient: false);
}

/// <summary>How a merge lands, decided at <c>BeginWriteAsync</c>. <see cref="SupplyVersion"/> is true
/// only when the version column is the sink's own <c>_pz_version</c>, filled with the commit time.</summary>
internal sealed record ChMergePlan(string Strategy, bool SupplyVersion);
