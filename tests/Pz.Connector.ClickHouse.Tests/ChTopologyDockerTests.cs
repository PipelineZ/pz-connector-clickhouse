using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChTopologyDockerTests
{
    private readonly ClickHouseFixture _ch;

    public ChTopologyDockerTests(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
    }

    private ChHttpClient Client(string db = "default", Dictionary<string, object?>? extra = null)
    {
        var values = _ch.ConnectionConfig(db);
        foreach (var (k, v) in extra ?? []) values[k] = v;
        var cfg = ChConnector.ParseOrThrow(new ConnectorConfig(values));
        return new ChHttpClient(ChHttpClient.Create(cfg), cfg, NullLogger.Instance);
    }

    [SkippableFact]
    public async Task Server_info_reports_version_user_and_not_cloud()
    {
        using var client = Client();
        var info = await ChTopology.ReadServerAsync(client, default);
        Assert.StartsWith("25.8", info.Version, StringComparison.Ordinal);
        Assert.Equal(ClickHouseFixture.User, info.User);
        Assert.False(info.IsCloud);
        Assert.False(string.IsNullOrEmpty(info.Timezone));
    }

    [SkippableFact]
    public async Task Table_lookup_returns_engine_and_ordered_columns()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.r (id UInt64, v String, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id");
        using var client = Client(db);
        var t = await ChTopology.GetTableAsync(client, new ChTableRef(db, "r"), default);
        Assert.NotNull(t);
        Assert.Equal("ReplacingMergeTree", t!.Engine);
        Assert.Equal("ver", t.ReplacingVersionColumn);
        Assert.Equal(["id", "v", "ver"], t.Columns.Select(c => c.Name));
        Assert.Equal("UInt64", t.Columns[0].Type);
        Assert.True(t.SupportsExchange);
        Assert.Null(await ChTopology.GetTableAsync(client, new ChTableRef(db, "missing"), default));
    }

    [SkippableFact]
    public async Task Unknown_cluster_is_0103()
    {
        using var client = Client(extra: new() { ["cluster"] = "no_such_cluster" });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ChTopology.ValidateDeploymentAsync(client, forWrites: true, default));
        Assert.Contains("PZCH0103", ex.Message);
    }

    [SkippableFact]
    public async Task Orphan_staging_tables_are_listed()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}._pz_stg_0123456789abcdef (x UInt8) ENGINE = MergeTree ORDER BY tuple()");
        using var client = Client(db);
        Assert.Equal(["_pz_stg_0123456789abcdef"], await ChTopology.OrphanStagingTablesAsync(client, db, default));
    }
}
