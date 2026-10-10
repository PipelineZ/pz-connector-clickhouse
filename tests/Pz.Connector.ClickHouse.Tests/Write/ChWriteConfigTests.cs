using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChWriteConfigTests
{
    private static readonly ChConnectionConfig Cfg = ChConnector.ParseOrThrow(new ConnectorConfig(
        new Dictionary<string, object?> { ["url"] = "http://h:8123", ["database"] = "analytics" }));

    private static (ChWriteConfig? Config, List<string> Errors) Parse(string mode, Dictionary<string, object?> options)
    {
        var errors = new List<string>();
        return (ChWriteConfig.Parse(new OutputSpec("clickhouse", "out", mode, "fail_on_change", options), Cfg, errors), errors);
    }

    [Fact]
    public void Defaults()
    {
        var (c, errors) = Parse("merge", []);
        Assert.Empty(errors);
        Assert.Equal(new ChTableRef("analytics", "out"), c!.Target);
        Assert.Equal("replacing", c.MergeStrategy);
        Assert.Null(c.Engine);
        Assert.Null(c.OrderBy);
    }

    [Fact]
    public void All_options()
    {
        var (c, errors) = Parse("merge", new()
        {
            ["entity"] = "raw.t", ["merge_strategy"] = "delete_insert", ["engine"] = "ReplacingMergeTree(updated_at)",
            ["order_by"] = new List<object?> { "a", "b" }, ["partition_by"] = "toYYYYMM(d)",
            ["settings"] = new Dictionary<string, object?> { ["async_insert"] = 0.0 },
        });
        Assert.Empty(errors);
        Assert.Equal(new ChTableRef("raw", "t"), c!.Target);
        Assert.Equal("delete_insert", c.MergeStrategy);
        Assert.Equal(["a", "b"], c.OrderBy);
        Assert.Equal("toYYYYMM(d)", c.PartitionBy);
        Assert.Equal("0", c.Settings["async_insert"]);
    }

    [Fact]
    public void Merge_strategy_outside_merge_is_0301() =>
        Assert.Contains(Parse("append", new() { ["merge_strategy"] = "replacing" }).Errors, e => e.Contains("PZCH0301"));

    [Fact]
    public void Unknown_merge_strategy_is_0307() =>
        Assert.Contains(Parse("merge", new() { ["merge_strategy"] = "upsert" }).Errors, e => e.Contains("PZCH0307"));

    [Fact]
    public void Unknown_option_is_0307() =>
        Assert.Contains(Parse("append", new() { ["engin"] = "x" }).Errors, e => e.Contains("PZCH0307") && e.Contains("'engin'"));

    [Fact]
    public void Order_by_must_be_a_list_of_names() =>
        Assert.Contains(Parse("append", new() { ["order_by"] = "a" }).Errors, e => e.Contains("PZCH0307") && e.Contains("order_by"));
}
