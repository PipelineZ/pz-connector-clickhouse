namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChSqlTests
{
    [Theory]
    [InlineData("events", "analytics", "analytics", "events")]
    [InlineData("raw.events", "analytics", "raw", "events")]
    public void Entity_resolves_one_or_two_parts(string entity, string defaultDb, string db, string table)
    {
        Assert.True(ChTableRef.TryParse(entity, defaultDb, out var t, out var error));
        Assert.Null(error);
        Assert.Equal(new ChTableRef(db, table), t);
    }

    [Theory]
    [InlineData("a.b.c")]
    [InlineData("a.")]
    [InlineData(".b")]
    [InlineData("")]
    [InlineData("we`ird")]
    public void Bad_entity_is_refused(string entity)
    {
        Assert.False(ChTableRef.TryParse(entity, "default", out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public void Quote_doubles_backticks_and_keeps_keywords_safe()
    {
        Assert.Equal("`select`", ChSql.Quote("select"));
        Assert.Equal("`a``b`", ChSql.Quote("a`b"));
        Assert.Equal("`db`.`order`", new ChTableRef("db", "order").Quoted);
    }

    [Fact]
    public void Literal_escapes_backslash_and_quote() =>
        Assert.Equal(@"'it\'s a \\ path'", ChSql.Literal(@"it's a \ path"));

    [Fact]
    public void Columns_joins_quoted() => Assert.Equal("`a`, `b c`", ChSql.Columns(["a", "b c"]));
}
