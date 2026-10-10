namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChTopologyTests
{
    private static ChTableInfo Info(string engine, string engineFull, string dbEngine = "Atomic") =>
        new(new ChTableRef("db", "t"), engine, engineFull, dbEngine, []);

    [Fact]
    public void Distributed_exposes_local_cluster_and_key()
    {
        var t = Info("Distributed", "Distributed('c1', 'db', 't_local', cityHash64(id))");
        Assert.True(t.IsDistributed);
        Assert.Equal(new ChTableRef("db", "t_local"), t.DistributedLocal);
        Assert.Equal("c1", t.DistributedCluster);
        Assert.Equal("cityHash64(id)", t.ShardingKey);
    }

    [Fact]
    public void Distributed_without_key_has_null_key() =>
        Assert.Null(Info("Distributed", "Distributed('c1', 'db', 't_local')").ShardingKey);

    [Theory]
    [InlineData("ReplacingMergeTree", "ReplacingMergeTree(ver) ORDER BY id", "ver")]
    [InlineData("ReplacingMergeTree", "ReplacingMergeTree ORDER BY id", null)]
    [InlineData("ReplicatedReplacingMergeTree", "ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', _pz_version) ORDER BY id", "_pz_version")]
    [InlineData("SharedReplacingMergeTree", "SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ver, is_deleted) ORDER BY id", "ver")]
    public void Replacing_version_column(string engine, string full, string? expected)
    {
        var t = Info(engine, full);
        Assert.True(t.IsReplacing);
        Assert.Equal(expected, t.ReplacingVersionColumn);
    }

    [Fact]
    public void Replicated_path_is_the_first_argument()
    {
        var t = Info("ReplicatedMergeTree", "ReplicatedMergeTree('/clickhouse/tables/{shard}/db/t', '{replica}') ORDER BY id");
        Assert.True(t.IsReplicated);
        Assert.Equal("/clickhouse/tables/{shard}/db/t", t.ReplicationPath);
    }

    [Theory]
    [InlineData("Atomic", true)] [InlineData("Replicated", true)] [InlineData("Shared", true)] [InlineData("Ordinary", false)]
    public void Exchange_support_follows_the_database_engine(string dbEngine, bool expected) =>
        Assert.Equal(expected, Info("MergeTree", "MergeTree ORDER BY id", dbEngine).SupportsExchange);

    [Fact]
    public void Plain_merge_tree_is_neither_replacing_nor_replicated()
    {
        var t = Info("MergeTree", "MergeTree ORDER BY id");
        Assert.False(t.IsReplacing);
        Assert.False(t.IsReplicated);
        Assert.False(t.IsDistributed);
    }
}
