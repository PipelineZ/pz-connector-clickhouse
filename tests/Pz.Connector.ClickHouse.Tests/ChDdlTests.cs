namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChDdlTests
{
    private static readonly ChDdlContext Single = new(null, false);
    private static readonly ChDdlContext Cluster = new("c1", false);
    private static readonly ChDdlContext Cloud = new(null, true);
    private static readonly ChTableRef T = new("db", "t");
    private static readonly ChTableRef Stg = new("db", "_pz_stg_0123456789abcdef");

    [Fact]
    public void Created_engines_by_deployment()
    {
        Assert.Equal("MergeTree", ChDdl.CreatedEngine(Single, replacing: false));
        Assert.Equal("ReplacingMergeTree(`_pz_version`)", ChDdl.CreatedEngine(Single, replacing: true));
        Assert.Equal("ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')", ChDdl.CreatedEngine(Cluster, false));
        Assert.Equal("ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', `_pz_version`)", ChDdl.CreatedEngine(Cluster, true));
        Assert.Equal("ReplacingMergeTree(`_pz_version`)", ChDdl.CreatedEngine(Cloud, true));
    }

    [Fact]
    public void Create_table_with_order_and_partition()
    {
        Assert.Equal("CREATE TABLE `db`.`t` ON CLUSTER `c1` (`id` Int64, `select` Nullable(String)) ENGINE = MergeTree ORDER BY (`id`) PARTITION BY toYYYYMM(d)",
            ChDdl.CreateTable(T, Cluster, [("id", "Int64"), ("select", "Nullable(String)")], "MergeTree", ["id"], "toYYYYMM(d)"));
        Assert.Equal("CREATE TABLE `db`.`t` (`id` Int64) ENGINE = MergeTree ORDER BY tuple()",
            ChDdl.CreateTable(T, Single, [("id", "Int64")], "MergeTree", [], null));
    }

    [Fact]
    public void Staging_engine_by_deployment()
    {
        Assert.Equal("CREATE TABLE `db`.`_pz_stg_0123456789abcdef` (`id` Int64) ENGINE = MergeTree ORDER BY tuple()",
            ChDdl.CreateStaging(Stg, Single, [("id", "Int64")], "0123456789abcdef"));
        Assert.Equal("CREATE TABLE `db`.`_pz_stg_0123456789abcdef` ON CLUSTER `c1` (`id` Int64) ENGINE = ReplicatedMergeTree('/clickhouse/pz/staging/0123456789abcdef', '{replica}') ORDER BY tuple()",
            ChDdl.CreateStaging(Stg, Cluster, [("id", "Int64")], "0123456789abcdef"));
    }

    [Fact]
    public void Create_like_with_and_without_engine_override()
    {
        Assert.Equal("CREATE TABLE `db`.`t__pz_new_x` AS `db`.`t`", ChDdl.CreateLike(new("db", "t__pz_new_x"), T, Single, null));
        Assert.Equal("CREATE TABLE `db`.`s` ON CLUSTER `c1` AS `db`.`t` ENGINE = E", ChDdl.CreateLike(new("db", "s"), T, Cluster, "E"));
    }

    [Fact]
    public void Shadow_engine_rewrites_a_static_replication_path_only()
    {
        ChTableInfo Info(string engine, string full) => new(T, engine, full, "Atomic", []);
        Assert.Null(ChDdl.ShadowEngine(Info("MergeTree", "MergeTree ORDER BY id"), "abc"));
        Assert.Null(ChDdl.ShadowEngine(Info("ReplicatedMergeTree", "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id"), "abc"));
        Assert.Equal("ReplicatedMergeTree('/clickhouse/tables/{shard}/db/t/pz_new_abc', '{replica}') ORDER BY id",
            ChDdl.ShadowEngine(Info("ReplicatedMergeTree", "ReplicatedMergeTree('/clickhouse/tables/{shard}/db/t', '{replica}') ORDER BY id"), "abc"));
        // A path a previous replace already suffixed is re-suffixed, not grown.
        Assert.Equal("ReplicatedMergeTree('/p/pz_new_0000000000000002', '{replica}') ORDER BY id",
            ChDdl.ShadowEngine(Info("ReplicatedMergeTree", "ReplicatedMergeTree('/p/pz_new_0000000000000001', '{replica}') ORDER BY id"), "0000000000000002"));
    }

    [Fact]
    public void Distributed_front()
    {
        Assert.Equal("CREATE TABLE `db`.`f` ON CLUSTER `c1` AS `db`.`l` ENGINE = Distributed('c1', 'db', 'l', cityHash64(id))",
            ChDdl.CreateDistributed(new("db", "f"), new("db", "l"), Cluster, "c1", "cityHash64(id)"));
        Assert.Equal("CREATE TABLE `db`.`f` ON CLUSTER `c1` AS `db`.`l` ENGINE = Distributed('c1', 'db', 'l')",
            ChDdl.CreateDistributed(new("db", "f"), new("db", "l"), Cluster, "c1", null));
    }

    [Fact]
    public void Swap_rename_drop_sync()
    {
        Assert.Equal("EXCHANGE TABLES `db`.`t` AND `db`.`s` ON CLUSTER `c1`", ChDdl.Exchange(T, new("db", "s"), Cluster));
        Assert.Equal("RENAME TABLE `db`.`s` TO `db`.`t`", ChDdl.Rename(new("db", "s"), T, Single));
        Assert.Equal("DROP TABLE IF EXISTS `db`.`t` ON CLUSTER `c1` SYNC", ChDdl.Drop(T, Cluster));
        Assert.Equal("SYSTEM SYNC REPLICA ON CLUSTER `c1` `db`.`t`", ChDdl.SyncReplica(T, Cluster));
    }

    [Fact]
    public void Insert_select_plain_dedup_and_versioned()
    {
        Assert.Equal("INSERT INTO `db`.`t` (`a`, `b`) SELECT `a`, `b` FROM `db`.`_pz_stg_0123456789abcdef`",
            ChDdl.InsertSelect(T, ["a", "b"], Stg, null, null));
        Assert.Equal("INSERT INTO `db`.`t` (`a`, `b`, `_pz_version`) SELECT `a`, `b`, toUInt64(1700000000000000) FROM `db`.`_pz_stg_0123456789abcdef` ORDER BY `_pz_seq` DESC LIMIT 1 BY `a`",
            ChDdl.InsertSelect(T, ["a", "b"], Stg, ["a"], 1700000000000000));
    }

    [Fact]
    public void Delete_keys_single_and_composite()
    {
        Assert.Equal("DELETE FROM `db`.`t` WHERE `k` IN (SELECT `k` FROM `db`.`_pz_stg_0123456789abcdef`)", ChDdl.DeleteKeys(T, Single, ["k"], Stg));
        Assert.Equal("DELETE FROM `db`.`t` ON CLUSTER `c1` WHERE (`k1`, `k2`) IN (SELECT `k1`, `k2` FROM `db`.`_pz_stg_0123456789abcdef`)",
            ChDdl.DeleteKeys(T, Cluster, ["k1", "k2"], Stg));
    }
}
