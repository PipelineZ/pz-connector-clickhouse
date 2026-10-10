using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;
using static Pz.Connector.ClickHouse.Tests.ChSinkBehaviorTests;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse-cluster")]
[Trait("Category", "ClickHouseCluster")]
public sealed class ChClusterTests
{
    private readonly ClickHouseClusterFixture _c;

    public ChClusterTests(ClickHouseClusterFixture c)
    {
        _c = c;
        DockerFacts.SkipUnlessDocker();
    }

    /// <summary>A database with a Distributed table <c>t</c> over a replicated local table whose
    /// replication path is STATIC (no {uuid}) -- the case the shadow-path rewrite exists for.</summary>
    private async Task<string> DistributedTargetAsync(string localEngine)
    {
        var db = "pzc_" + Guid.NewGuid().ToString("N")[..10];
        await _c.QueryAsync(1, $"CREATE DATABASE {db} ON CLUSTER {ClickHouseClusterFixture.Cluster}");
        await _c.QueryAsync(1, $"CREATE TABLE {db}.t_local ON CLUSTER {ClickHouseClusterFixture.Cluster} (id Int64, name Nullable(String)) ENGINE = {localEngine.Replace("{db}", db, StringComparison.Ordinal)} ORDER BY id");
        await _c.QueryAsync(1, $"CREATE TABLE {db}.t ON CLUSTER {ClickHouseClusterFixture.Cluster} AS {db}.t_local ENGINE = Distributed({ClickHouseClusterFixture.Cluster}, {db}, t_local, cityHash64(id))");
        return db;
    }

    private Task WriteAsync(int node, string db, OutputSpec spec, params Apache.Arrow.RecordBatch[] batches) =>
        WriteThroughAsync(new ConnectorConfig(_c.ConnectionConfig(node, db)), spec, batches);

    private static async Task WriteThroughAsync(ConnectorConfig config, OutputSpec spec, Apache.Arrow.RecordBatch[] batches)
    {
        await using var sink = await ((ISinkConnector)new ChConnector()).OpenAsync(config, default);
        await using var session = await sink.BeginWriteAsync(spec, IdName, default);
        foreach (var b in batches)
        {
            await session.WriteBatchAsync(b, default);
            b.Dispose();
        }

        await session.CommitAsync(default);
    }

    private async Task<string> CountAsync(int node, string sql) => (await _c.QueryAsync(node, sql)).Trim();

    [SkippableFact]
    public async Task Append_through_a_distributed_target_lands_on_both_shards()
    {
        var db = await DistributedTargetAsync("ReplicatedMergeTree('/clickhouse/tables/{shard}/{db}/t_local', '{replica}')");
        await WriteAsync(1, db, Out("t", "append"), Rows(IdName, Enumerable.Range(0, 100).Select(i => ((long)i, (string?)"x")).ToArray()));
        Assert.Equal("100", await CountAsync(3, $"SELECT count() FROM {db}.t"));
        Assert.NotEqual("0", await CountAsync(1, $"SELECT count() FROM {db}.t_local"));
        Assert.NotEqual("0", await CountAsync(3, $"SELECT count() FROM {db}.t_local"));
    }

    [SkippableFact]
    public async Task Replace_through_a_distributed_target_swaps_every_shard_and_leaves_nothing_behind()
    {
        var db = await DistributedTargetAsync("ReplicatedMergeTree('/clickhouse/tables/{shard}/{db}/t_local', '{replica}')");
        await WriteAsync(1, db, Out("t", "append"), Rows(IdName, Enumerable.Range(0, 50).Select(i => ((long)i, (string?)"old")).ToArray()));
        await WriteAsync(1, db, Out("t", "replace"), Rows(IdName, (1000, "new"), (1001, "new")));
        await WriteAsync(1, db, Out("t", "replace"), Rows(IdName, (2000, "newer")));

        Assert.Equal("2000\tnewer", (await _c.QueryAsync(4, $"SELECT id, name FROM {db}.t FORMAT TSV")).Trim());
        Assert.Equal("0", await CountAsync(2, $"SELECT count() FROM clusterAllReplicas({ClickHouseClusterFixture.Cluster}, system.tables) WHERE database = '{db}' AND name LIKE '%pz%'"));
    }

    [SkippableFact]
    public async Task Merge_replacing_through_a_distributed_target()
    {
        var db = await DistributedTargetAsync("ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{db}/t_local', '{replica}')");
        await WriteAsync(1, db, new OutputSpec("clickhouse", "t", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] },
            Rows(IdName, (1, "a1"), (2, "b1"), (1, "a2")));
        await WriteAsync(3, db, new OutputSpec("clickhouse", "t", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] },
            Rows(IdName, (2, "b2")));
        Assert.Equal("1\ta2\n2\tb2", (await _c.QueryAsync(2, $"SELECT id, name FROM {db}.t FINAL ORDER BY id FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Merge_delete_insert_through_a_distributed_target()
    {
        var db = await DistributedTargetAsync("ReplicatedMergeTree('/clickhouse/tables/{shard}/{db}/t_local', '{replica}')");
        await WriteAsync(1, db, Out("t", "append"), Rows(IdName, (1, "old"), (2, "keep")));
        await WriteAsync(3, db, new OutputSpec("clickhouse", "t", "merge", "fail_on_change", new Dictionary<string, object?> { ["merge_strategy"] = "delete_insert" }) { Keys = ["id"] },
            Rows(IdName, (1, "new"), (3, "c")));
        Assert.Equal("1\tnew\n2\tkeep\n3\tc", (await _c.QueryAsync(4, $"SELECT id, name FROM {db}.t ORDER BY id FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Staging_rows_written_on_one_node_are_readable_on_every_node()
    {
        var db = "pzc_" + Guid.NewGuid().ToString("N")[..10];
        await _c.QueryAsync(1, $"CREATE DATABASE {db} ON CLUSTER {ClickHouseClusterFixture.Cluster}");
        await using var sink = await ((ISinkConnector)new ChConnector()).OpenAsync(new ConnectorConfig(_c.ConnectionConfig(1, db)), default);
        await using var session = await sink.BeginWriteAsync(Out("t", "append"), IdName, default);
        using var b = Rows(IdName, (1, "a"), (2, "b"), (3, "c"));
        await session.WriteBatchAsync(b, default);

        var staging = (await _c.QueryAsync(4, $"SELECT name FROM system.tables WHERE database = '{db}' AND startsWith(name, '_pz_stg_') FORMAT TSV")).Trim();
        await _c.QueryAsync(4, $"SYSTEM SYNC REPLICA {db}.{staging}");
        Assert.Equal("3", await CountAsync(4, $"SELECT count() FROM {db}.{staging}"));
        await session.AbortAsync(default);
    }

    [SkippableFact]
    public async Task Created_target_on_a_cluster_is_replicated_on_every_node()
    {
        var db = "pzc_" + Guid.NewGuid().ToString("N")[..10];
        await _c.QueryAsync(1, $"CREATE DATABASE {db} ON CLUSTER {ClickHouseClusterFixture.Cluster}");
        await WriteAsync(1, db, Out("t", "append"), Rows(IdName, (1, "a")));
        Assert.Equal("ReplicatedMergeTree", (await _c.QueryAsync(4, $"SELECT engine FROM system.tables WHERE database = '{db}' AND name = 't' FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Non_unique_replica_macro_is_0106()
    {
        var values = _c.ConnectionConfig(1, "default");
        values["cluster"] = "pz_dup";
        var cfg = ChConnector.ParseOrThrow(new ConnectorConfig(values));
        using var client = new ChHttpClient(ChHttpClient.Create(cfg), cfg, NullLogger.Instance);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ChTopology.ValidateDeploymentAsync(client, forWrites: true, default));
        Assert.Contains("PZCH0106", ex.Message);
    }

    [SkippableFact]
    public async Task Check_reports_the_cluster_shape()
    {
        var check = await new ChConnector().CheckConnectionAsync(new ConnectorConfig(_c.ConnectionConfig(1, "default")), default);
        Assert.True(check.Ok, check.Message);
        Assert.Contains("cluster pz_cluster: 2 shard(s) x 2 replica(s)", check.Message, StringComparison.Ordinal);
    }
}
