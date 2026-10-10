using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

internal sealed record ChServerInfo(string Version, string User, string Timezone, bool IsCloud);

internal sealed record ChColumnInfo(string Name, string Type);

/// <summary>A table as the server describes it. Everything derived from <see cref="EngineFull"/>
/// reads the clause the server itself printed (see <see cref="ChEngineArgs"/>).</summary>
internal sealed record ChTableInfo(ChTableRef Ref, string Engine, string EngineFull, string DatabaseEngine, IReadOnlyList<ChColumnInfo> Columns)
{
    private IReadOnlyList<string> Args => ChEngineArgs.Split(EngineFull, Engine) ?? [];

    public bool IsDistributed => Engine == "Distributed";

    public bool IsReplacing => Engine is "ReplacingMergeTree" or "ReplicatedReplacingMergeTree" or "SharedReplacingMergeTree";

    public bool IsReplicated => Engine.StartsWith("Replicated", StringComparison.Ordinal);

    public bool SupportsExchange => DatabaseEngine is "Atomic" or "Replicated" or "Shared";

    /// <summary>The first unquoted engine argument: the replication path and replica name are quoted
    /// literals, the version column (and the optional is_deleted column after it) are identifiers.</summary>
    public string? ReplacingVersionColumn => IsReplacing
        ? Args.Where(a => !a.StartsWith('\'')).Select(ChEngineArgs.Unquote).FirstOrDefault()
        : null;

    public string? ReplicationPath => IsReplicated && Args.Count > 0 && Args[0].StartsWith('\'') ? ChEngineArgs.Unquote(Args[0]) : null;

    public ChTableRef? DistributedLocal => IsDistributed && Args.Count >= 3
        ? new ChTableRef(ChEngineArgs.Unquote(Args[1]), ChEngineArgs.Unquote(Args[2]))
        : null;

    public string? DistributedCluster => IsDistributed && Args.Count >= 1 ? ChEngineArgs.Unquote(Args[0]) : null;

    public string? ShardingKey => IsDistributed && Args.Count >= 4 ? Args[3] : null;
}

/// <summary>Everything the connector asks the server about itself: version, Cloud or not, a table's
/// engine and columns, a cluster's shape, and leftovers from crashed writes.</summary>
internal static class ChTopology
{
    public static async Task<ChServerInfo> ReadServerAsync(ChHttpClient client, CancellationToken ct)
    {
        var rows = await client.QueryRowsAsync(
            "SELECT version(), currentUser(), timezone(), (SELECT count() FROM system.settings WHERE name = 'cloud_mode' AND value = '1')",
            "reading server info", ChCodes.Read_QueryFailed, ct).ConfigureAwait(false);
        var row = rows.Single();
        // Cloud is detected by its setting; the host suffix is the fallback for a service whose
        // settings table hides it.
        var isCloud = row[3] != "0" || client.Config.Url.Host.EndsWith(".clickhouse.cloud", StringComparison.OrdinalIgnoreCase);
        return new ChServerInfo(row[0] ?? "?", row[1] ?? "?", row[2] ?? "UTC", isCloud);
    }

    /// <summary>The checks that need the server before the first read or write. A cluster must exist
    /// and must not be named on Cloud; for writes, every node's <c>{replica}</c> macro must be unique
    /// cluster-wide, because the staging table registers every node as a replica of one path.</summary>
    public static async Task<ChServerInfo> ValidateDeploymentAsync(ChHttpClient client, bool forWrites, CancellationToken ct)
    {
        var info = await ReadServerAsync(client, ct).ConfigureAwait(false);
        var cfg = client.Config;
        if (cfg.Cluster is not { } cluster)
        {
            return info;
        }

        if (info.IsCloud)
        {
            throw Refuse(cfg, ChCodes.Config_ClusterOnCloud, "'cluster' does not apply to ClickHouse Cloud, which manages replication itself; remove it");
        }

        if (await GetClusterAsync(client, cluster, ct).ConfigureAwait(false) is null)
        {
            throw Refuse(cfg, ChCodes.Config_UnknownCluster, $"cluster '{cluster}' is not in system.clusters on {cfg.Url.Host}; check the name against remote_servers");
        }

        if (forWrites)
        {
            var rows = await client.QueryRowsAsync(
                $"SELECT count(), uniqExact(r) FROM (SELECT getMacro('replica') AS r FROM clusterAllReplicas({ChSql.Literal(cluster)}, system.one))",
                "checking replica macros", ChCodes.Read_QueryFailed, ct).ConfigureAwait(false);
            if (rows.Single()[0] != rows.Single()[1])
            {
                throw Refuse(cfg, ChCodes.Config_ReplicaMacroNotUnique,
                    $"the {{replica}} macro is not unique across cluster '{cluster}'; give every node its own value (the write staging table needs it)");
            }
        }

        return info;
    }

    public static async Task<ChTableInfo?> GetTableAsync(ChHttpClient client, ChTableRef table, CancellationToken ct)
    {
        var parameters = new Dictionary<string, string> { ["pz_db"] = table.Database, ["pz_table"] = table.Table };
        var rows = await client.QueryRowsAsync(
            "SELECT t.engine, t.engine_full, d.engine FROM system.tables AS t INNER JOIN system.databases AS d ON d.name = t.database " +
            "WHERE t.database = {pz_db:String} AND t.name = {pz_table:String}",
            $"looking up {table.Display}", ChCodes.Read_QueryFailed, ct, parameters).ConfigureAwait(false);
        if (rows.Count == 0)
        {
            return null;
        }

        var columns = await client.QueryRowsAsync(
            "SELECT name, type FROM system.columns WHERE database = {pz_db:String} AND table = {pz_table:String} ORDER BY position",
            $"reading the columns of {table.Display}", ChCodes.Read_QueryFailed, ct, parameters).ConfigureAwait(false);
        return new ChTableInfo(table, rows[0][0]!, rows[0][1] ?? "", rows[0][2] ?? "", columns.Select(c => new ChColumnInfo(c[0]!, c[1]!)).ToList());
    }

    public static async Task<(int Shards, int Replicas)?> GetClusterAsync(ChHttpClient client, string cluster, CancellationToken ct)
    {
        var rows = await client.QueryRowsAsync(
            "SELECT uniqExact(shard_num), max(replica_num) FROM system.clusters WHERE cluster = {pz_cluster:String}",
            "reading the cluster", ChCodes.Read_QueryFailed, ct, new Dictionary<string, string> { ["pz_cluster"] = cluster }).ConfigureAwait(false);
        var shards = int.Parse(rows.Single()[0]!, CultureInfo.InvariantCulture);
        return shards == 0 ? null : (shards, int.Parse(rows.Single()[1]!, CultureInfo.InvariantCulture));
    }

    public static async Task<IReadOnlyList<string>> OrphanStagingTablesAsync(ChHttpClient client, string database, CancellationToken ct)
    {
        var rows = await client.QueryRowsAsync(
            "SELECT name FROM system.tables WHERE database = {pz_db:String} AND startsWith(name, '_pz_stg_') ORDER BY name",
            "listing leftover staging tables", ChCodes.Read_QueryFailed, ct, new Dictionary<string, string> { ["pz_db"] = database }).ConfigureAwait(false);
        return rows.Select(r => r[0]!).ToList();
    }

    private static PzConnectorException Refuse(ChConnectionConfig cfg, string code, string text) =>
        new(ChCodes.Message(code, cfg.Redactor, text), isTransient: false);
}
