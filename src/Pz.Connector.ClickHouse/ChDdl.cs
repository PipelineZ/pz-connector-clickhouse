using System.Globalization;
using System.Text.RegularExpressions;

namespace Pz.Connector.ClickHouse;

/// <summary>Where generated DDL runs. On a self-managed cluster every statement carries
/// <c>ON CLUSTER</c> and created tables are <c>Replicated*</c>; on Cloud the plain engine name is
/// used and Cloud maps it to its Shared variant itself.</summary>
internal sealed record ChDdlContext(string? Cluster, bool IsCloud)
{
    public string OnCluster => Cluster is { } c ? $" ON CLUSTER {ChSql.Quote(c)}" : "";

    public bool Replicated => Cluster is not null && !IsCloud;
}

/// <summary>Every statement the sink sends, built in one place. Identifiers always go through
/// <see cref="ChSql.Quote"/>; the only verbatim text is what the server itself printed (an engine
/// clause) or what the user wrote in <c>engine:</c>/<c>partition_by:</c>.</summary>
internal static partial class ChDdl
{
    public const string SequenceColumn = "_pz_seq";
    public const string VersionColumn = "_pz_version";

    private const string DefaultPath = "'/clickhouse/tables/{uuid}/{shard}', '{replica}'";

    public static string CreatedEngine(ChDdlContext ctx, bool replacing) => (ctx.Replicated, replacing) switch
    {
        (false, false) => "MergeTree",
        (false, true) => $"ReplacingMergeTree({ChSql.Quote(VersionColumn)})",
        (true, false) => $"ReplicatedMergeTree({DefaultPath})",
        (true, true) => $"ReplicatedReplacingMergeTree({DefaultPath}, {ChSql.Quote(VersionColumn)})",
    };

    public static string CreateTable(ChTableRef table, ChDdlContext ctx, IReadOnlyList<(string Name, string Type)> columns,
        string engine, IReadOnlyList<string> orderBy, string? partitionBy) =>
        $"CREATE TABLE {table.Quoted}{ctx.OnCluster} ({ColumnDefs(columns)}) ENGINE = {engine} ORDER BY {OrderBy(orderBy)}"
        + (partitionBy is null ? "" : $" PARTITION BY {partitionBy}");

    /// <summary>On a cluster, every node's replica registers under ONE path, so the staging table is a
    /// single replicated table with a full copy on every node -- readable by an INSERT ... SELECT or a
    /// DELETE ... IN (SELECT ...) on whichever node a load balancer picks.</summary>
    public static string CreateStaging(ChTableRef staging, ChDdlContext ctx, IReadOnlyList<(string Name, string Type)> columns, string tag)
    {
        var engine = ctx.Replicated ? $"ReplicatedMergeTree('/clickhouse/pz/staging/{tag}', '{{replica}}')" : "MergeTree";
        return $"CREATE TABLE {staging.Quoted}{ctx.OnCluster} ({ColumnDefs(columns)}) ENGINE = {engine} ORDER BY tuple()";
    }

    public static string CreateLike(ChTableRef table, ChTableRef like, ChDdlContext ctx, string? engineOverride) =>
        $"CREATE TABLE {table.Quoted}{ctx.OnCluster} AS {like.Quoted}" + (engineOverride is null ? "" : $" ENGINE = {engineOverride}");

    /// <summary>A shadow copied from a replicated table with a STATIC replication path would register
    /// at the same Keeper path as the table it shadows and be refused, so its path gets a per-write
    /// suffix (replacing any suffix a previous replace left, so paths do not grow). A path containing
    /// <c>{uuid}</c> is unique per table already, and a non-replicated engine has no path: both copy
    /// as-is (null).</summary>
    public static string? ShadowEngine(ChTableInfo target, string tag)
    {
        if (target.ReplicationPath is not { } path || path.Contains("{uuid}", StringComparison.Ordinal))
        {
            return null;
        }

        var newPath = PreviousSuffix().Replace(path, "") + "/pz_new_" + tag;
        return ChEngineArgs.ReplaceFirstArgument(target.EngineFull, target.Engine, ChSql.Literal(newPath));
    }

    public static string CreateDistributed(ChTableRef front, ChTableRef local, ChDdlContext ctx, string cluster, string? shardingKey) =>
        $"CREATE TABLE {front.Quoted}{ctx.OnCluster} AS {local.Quoted} ENGINE = Distributed({ChSql.Literal(cluster)}, {ChSql.Literal(local.Database)}, {ChSql.Literal(local.Table)}"
        + (shardingKey is null ? ")" : $", {shardingKey})");

    public static string Exchange(ChTableRef a, ChTableRef b, ChDdlContext ctx) => $"EXCHANGE TABLES {a.Quoted} AND {b.Quoted}{ctx.OnCluster}";

    public static string Rename(ChTableRef from, ChTableRef to, ChDdlContext ctx) => $"RENAME TABLE {from.Quoted} TO {to.Quoted}{ctx.OnCluster}";

    public static string Drop(ChTableRef table, ChDdlContext ctx) => $"DROP TABLE IF EXISTS {table.Quoted}{ctx.OnCluster} SYNC";

    public static string SyncReplica(ChTableRef table, ChDdlContext ctx) => $"SYSTEM SYNC REPLICA{ctx.OnCluster} {table.Quoted}";

    /// <summary>With <paramref name="dedupKeys"/>, only the last staged row per key (highest
    /// <c>_pz_seq</c>) is inserted. With <paramref name="version"/>, the sink's own version column
    /// is filled with it, so a later commit's rows win in a ReplacingMergeTree.</summary>
    public static string InsertSelect(ChTableRef target, IReadOnlyList<string> columns, ChTableRef staging,
        IReadOnlyList<string>? dedupKeys, long? version)
    {
        var targetCols = version is null ? ChSql.Columns(columns) : $"{ChSql.Columns(columns)}, {ChSql.Quote(VersionColumn)}";
        var select = version is { } v ? $"{ChSql.Columns(columns)}, toUInt64({v.ToString(CultureInfo.InvariantCulture)})" : ChSql.Columns(columns);
        var dedup = dedupKeys is { Count: > 0 } keys ? $" ORDER BY {ChSql.Quote(SequenceColumn)} DESC LIMIT 1 BY {ChSql.Columns(keys)}" : "";
        return $"INSERT INTO {target.Quoted} ({targetCols}) SELECT {select} FROM {staging.Quoted}{dedup}";
    }

    public static string DeleteKeys(ChTableRef local, ChDdlContext ctx, IReadOnlyList<string> keys, ChTableRef staging)
    {
        var tuple = keys.Count == 1 ? ChSql.Quote(keys[0]) : $"({ChSql.Columns(keys)})";
        return $"DELETE FROM {local.Quoted}{ctx.OnCluster} WHERE {tuple} IN (SELECT {ChSql.Columns(keys)} FROM {staging.Quoted})";
    }

    private static string ColumnDefs(IReadOnlyList<(string Name, string Type)> columns) =>
        string.Join(", ", columns.Select(c => $"{ChSql.Quote(c.Name)} {c.Type}"));

    private static string OrderBy(IReadOnlyList<string> columns) => columns.Count == 0 ? "tuple()" : $"({ChSql.Columns(columns)})";

    [GeneratedRegex("/pz_new_[0-9a-f]{16}$")]
    private static partial Regex PreviousSuffix();
}
