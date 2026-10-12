using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>One output's write. Batches stream into the staging table as they arrive; the commit runs
/// the one step that touches the target. Until commit the target is untouched, so abort is a DROP of
/// the staging table. CLEANUP NEVER THROWS: every transient table (staging, shadow, front) is dropped
/// best-effort and logged, so a leftover can never turn a committed write into a failure or mask the
/// commit's own exception.</summary>
internal sealed class ChWriteSession(
    ChHttpClient client, TimeProvider time, ILogger logger, OutputSpec spec, ChWriteConfig output, ChWriteTarget target,
    ChTableRef staging, string tag, IReadOnlyList<string> columns, IReadOnlyList<(string Name, string Type)> targetColumns,
    ChMergePlan? merge, string context) : ISinkWriteSession
{
    private enum State { Open, Committed, Aborted, Disposed }

    private readonly List<ChTableRef> _transient = [staging];
    private long _rows;
    private long _batches;
    private State _state = State.Open;

    private ChDdlContext Ddl => target.Ddl;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        if (_state != State.Open)
        {
            throw new InvalidOperationException($"the session is not open (state: {_state})");
        }

        var cols = merge is null ? columns : [.. columns, ChDdl.SequenceColumn];
        using var response = await client.SendAsync(new ChRequest($"{context}: staging rows", ChCodes.Write_CommitFailed)
        {
            QueryInUrl = $"INSERT INTO {staging.Quoted} ({ChSql.Columns(cols)}) FORMAT ArrowStream",
            Body = new ChArrowContent(batch, merge is null ? null : _rows),
            Settings = Settings(new() { ["wait_end_of_query"] = "1" }),
        }, ct).ConfigureAwait(false);
        await response.Content.CopyToAsync(Stream.Null, ct).ConfigureAwait(false);
        _rows += batch.Length;
        _batches++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureNotDisposed();
        if (_state is State.Committed or State.Aborted)
        {
            throw new InvalidOperationException($"the session is already {_state.ToString().ToLowerInvariant()}");
        }

        _state = State.Committed;
        var start = time.GetTimestamp();
        try
        {
            if (Ddl.Replicated)
            {
                // Staging rows were inserted on one node; wait until every replica has them before a
                // statement on another node reads them.
                await RunAsync(ChDdl.SyncReplica(staging, Ddl), "syncing the staging table", ct).ConfigureAwait(false);
            }

            switch (spec.Mode)
            {
                case "append":
                    await EnsureTargetAsync(replacing: false, ct).ConfigureAwait(false);
                    await RunAsync(ChDdl.InsertSelect(target.Target, columns, staging, null, null), "appending", ct, Distributed(), reportRows: true).ConfigureAwait(false);
                    break;
                case "replace":
                    await ReplaceAsync(ct).ConfigureAwait(false);
                    break;
                case "merge":
                    await MergeAsync(ct).ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidOperationException($"unexpected mode '{spec.Mode}' reached CommitAsync -- BeginWriteAsync should have refused it");
            }

            logger.LogDebug("clickhouse: {Context}: committed {Rows} row(s) in {Batches} batch(es) in {ElapsedMs}ms",
                context, _rows, _batches, time.GetElapsedTime(start).TotalMilliseconds);
            return new WriteResult(_rows, _batches);
        }
        finally
        {
            await CleanupAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureNotDisposed();
        if (_state == State.Committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        if (_state == State.Aborted)
        {
            throw new InvalidOperationException("the session is already aborted");
        }

        _state = State.Aborted;
        await CleanupAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_state == State.Open)
        {
            await AbortAsync(CancellationToken.None).ConfigureAwait(false);
        }

        _state = State.Disposed;
    }

    private async Task ReplaceAsync(CancellationToken ct)
    {
        if (target.Existing is null)
        {
            var fresh = target.Target.WithTable($"{target.Target.Table}__pz_new_{tag}");
            _transient.Add(fresh);
            await RunAsync(Create(fresh, replacing: false), "creating the target", ct).ConfigureAwait(false);
            await RunAsync(ChDdl.InsertSelect(fresh, columns, staging, null, null), "loading the new table", ct, reportRows: true).ConfigureAwait(false);
            await RunAsync(ChDdl.Rename(fresh, target.Target, Ddl), "renaming into place", ct).ConfigureAwait(false);
            _transient.Remove(fresh);
            return;
        }

        if (target.Local is { } local && target.Existing.IsDistributed)
        {
            // The swap happens on the local tables, so the new rows must still be spread across shards:
            // a transient Distributed front over the shadow, with the target's own sharding key.
            var shadowLocal = local.Ref.WithTable($"{local.Ref.Table}__pz_new_{tag}");
            var front = target.Target.WithTable($"_pz_front_{tag}");
            _transient.Add(shadowLocal);
            _transient.Add(front);
            await RunAsync(ChDdl.CreateLike(shadowLocal, local.Ref, Ddl, ChDdl.ShadowEngine(local, tag)), "creating the shadow table", ct).ConfigureAwait(false);
            await RunAsync(ChDdl.CreateDistributed(front, shadowLocal, Ddl, target.Existing.DistributedCluster!, target.Existing.ShardingKey),
                "creating the shadow front", ct).ConfigureAwait(false);
            await RunAsync(ChDdl.InsertSelect(front, columns, staging, null, null), "loading the shadow table", ct, Distributed(force: true), reportRows: true).ConfigureAwait(false);
            await RunAsync(ChDdl.Exchange(local.Ref, shadowLocal, Ddl), "swapping the tables", ct).ConfigureAwait(false);
            return;
        }

        var shadow = target.Target.WithTable($"{target.Target.Table}__pz_new_{tag}");
        _transient.Add(shadow);
        await RunAsync(ChDdl.CreateLike(shadow, target.Target, Ddl, ChDdl.ShadowEngine(target.Existing, tag)), "creating the shadow table", ct).ConfigureAwait(false);
        await RunAsync(ChDdl.InsertSelect(shadow, columns, staging, null, null), "loading the shadow table", ct, reportRows: true).ConfigureAwait(false);
        await RunAsync(ChDdl.Exchange(target.Target, shadow, Ddl), "swapping the tables", ct).ConfigureAwait(false);
    }

    /// <summary>Replacing: one deduplicated INSERT ... SELECT; the table deduplicates across runs in
    /// background merges, so readers use FINAL. delete_insert: delete the staged keys, then the same
    /// insert -- correct without FINAL, but not atomic: between the two statements a reader can see
    /// those keys missing; a retry converges.</summary>
    private async Task MergeAsync(CancellationToken ct)
    {
        var replacing = merge!.Strategy == ChWriteConfig.Replacing;
        var created = target.Existing is null;
        if (created)
        {
            await RunAsync(Create(target.Target, replacing: replacing && output.Engine is null), "creating the target", ct).ConfigureAwait(false);
        }
        else if (!replacing)
        {
            await RunAsync(ChDdl.DeleteKeys(target.DdlTable, Ddl, spec.Keys, staging), "deleting the merged keys", ct,
                new() { ["lightweight_deletes_sync"] = "2", ["allow_nondeterministic_mutations"] = "1" }).ConfigureAwait(false);
        }

        long? version = merge.SupplyVersion ? (time.GetUtcNow().UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10 : null;
        await RunAsync(ChDdl.InsertSelect(target.Target, columns, staging, spec.Keys, version), "merging", ct, Distributed(), reportRows: true).ConfigureAwait(false);
    }

    private async Task EnsureTargetAsync(bool replacing, CancellationToken ct)
    {
        if (target.Existing is null)
        {
            await RunAsync(Create(target.Target, replacing), "creating the target", ct).ConfigureAwait(false);
        }
    }

    private string Create(ChTableRef table, bool replacing)
    {
        var cols = replacing ? [.. targetColumns, (ChDdl.VersionColumn, "UInt64")] : targetColumns;
        var orderBy = output.OrderBy ?? (spec.Mode == "merge" ? spec.Keys : []);
        return ChDdl.CreateTable(table, Ddl, cols, output.Engine ?? ChDdl.CreatedEngine(Ddl, replacing), orderBy, output.PartitionBy);
    }

    private Dictionary<string, string>? Distributed(bool force = false) =>
        force || target.Existing?.IsDistributed == true ? new() { ["insert_distributed_sync"] = "1" } : null;

    private Dictionary<string, string> Settings(Dictionary<string, string>? extra)
    {
        var s = new Dictionary<string, string>(output.Settings, StringComparer.Ordinal);
        foreach (var (k, v) in extra ?? [])
        {
            s[k] = v;
        }

        return s;
    }

    private Task RunAsync(string sql, string step, CancellationToken ct, Dictionary<string, string>? settings = null, bool reportRows = false) =>
        client.ExecuteAsync(sql, $"{context}: {step}", ChCodes.Write_CommitFailed, ct, Settings(settings), reportRows);

    private async Task CleanupAsync()
    {
        foreach (var table in _transient)
        {
            try
            {
                await client.ExecuteAsync(ChDdl.Drop(table, Ddl), $"{context}: dropping {table.Display}", ChCodes.Write_CommitFailed,
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "clickhouse: {Context}: failed to drop {Table}; it is safe to DROP by hand", context, table.Display);
            }
        }

        _transient.Clear();
    }

    private void EnsureNotDisposed()
    {
        if (_state == State.Disposed)
        {
            throw new InvalidOperationException("the session is disposed");
        }
    }
}
