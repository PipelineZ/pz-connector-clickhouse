using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChReadConfigTests
{
    private static readonly ChConnectionConfig Cfg = ChConnector.ParseOrThrow(new ConnectorConfig(
        new Dictionary<string, object?> { ["url"] = "http://h:8123", ["database"] = "analytics" }));

    private static (ChReadConfig? Config, List<string> Errors) Parse(string dataset, Dictionary<string, object?> options)
    {
        var errors = new List<string>();
        return (ChReadConfig.Parse(new DatasetSpec("clickhouse", dataset, options), Cfg, errors), errors);
    }

    [Fact]
    public void Entity_defaults_to_the_dataset_name_in_the_connection_database()
    {
        var (c, errors) = Parse("events", []);
        Assert.Empty(errors);
        Assert.Equal(new ChTableRef("analytics", "events"), c!.Table);
        Assert.Equal("analytics.events", c.Label);
    }

    [Fact]
    public void Query_mode_keeps_the_sql()
    {
        var (c, _) = Parse("q", new() { ["query"] = "select 1" });
        Assert.Null(c!.Table);
        Assert.Equal("select 1", c.Query);
        Assert.Equal("query", c.Label);
    }

    [Fact]
    public void Entity_and_query_together_is_0201() =>
        Assert.Contains(Parse("x", new() { ["entity"] = "t", ["query"] = "select 1" }).Errors, e => e.Contains("PZCH0201"));

    [Fact]
    public void Bad_entity_is_0104() =>
        Assert.Contains(Parse("x", new() { ["entity"] = "a.b.c" }).Errors, e => e.Contains("PZCH0104"));

    [Fact]
    public void Unknown_option_is_0205() =>
        Assert.Contains(Parse("x", new() { ["entiy"] = "t" }).Errors, e => e.Contains("PZCH0205") && e.Contains("'entiy'"));

    [Fact]
    public void Dataset_settings_parse() =>
        Assert.Equal("8", Parse("x", new() { ["settings"] = new Dictionary<string, object?> { ["max_threads"] = 8.0 } }).Config!.Settings["max_threads"]);
}
