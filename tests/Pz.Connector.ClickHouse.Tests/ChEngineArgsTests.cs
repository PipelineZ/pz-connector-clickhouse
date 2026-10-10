namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChEngineArgsTests
{
    [Fact]
    public void Splits_top_level_arguments_respecting_quotes_and_brackets()
    {
        var args = ChEngineArgs.Split("Distributed('c', 'db', 'local', cityHash64(id, 'x,y')) SETTINGS fsync_after_insert = 0", "Distributed");
        Assert.Equal(["'c'", "'db'", "'local'", "cityHash64(id, 'x,y')"], args);
    }

    [Fact]
    public void Three_argument_form_has_no_key() =>
        Assert.Equal(3, ChEngineArgs.Split("Distributed('c', 'db', 'local')", "Distributed")!.Count);

    [Fact]
    public void Other_engine_is_null() => Assert.Null(ChEngineArgs.Split("MergeTree ORDER BY id", "Distributed"));

    [Theory]
    [InlineData("'db'", "db")]
    [InlineData("`db`", "db")]
    [InlineData("db", "db")]
    [InlineData(@"'it\'s'", "it's")]
    public void Unquote(string arg, string expected) => Assert.Equal(expected, ChEngineArgs.Unquote(arg));

    [Fact]
    public void Replace_first_argument_keeps_the_rest_verbatim() =>
        Assert.Equal("ReplicatedMergeTree('/new', '{replica}', ver) ORDER BY id SETTINGS index_granularity = 8192",
            ChEngineArgs.ReplaceFirstArgument("ReplicatedMergeTree('/old,x', '{replica}', ver) ORDER BY id SETTINGS index_granularity = 8192", "ReplicatedMergeTree", "'/new'"));
}
