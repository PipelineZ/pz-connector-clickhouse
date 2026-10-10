namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChTypesParseTests
{
    [Theory]
    [InlineData("UInt64", "UInt64")]
    [InlineData("Nullable(String)", "Nullable(String)")]
    [InlineData("LowCardinality(Nullable(String))", "LowCardinality(Nullable(String))")]
    [InlineData("Decimal(38, 9)", "Decimal(38, 9)")]
    [InlineData("DateTime64(3, 'UTC')", "DateTime64(3, 'UTC')")]
    [InlineData("DateTime('Asia/Tokyo')", "DateTime('Asia/Tokyo')")]
    [InlineData("Array(Nullable(String))", "Array(Nullable(String))")]
    [InlineData("Map(String, UInt64)", "Map(String, UInt64)")]
    [InlineData("Tuple(x Int32, y String)", "Tuple(`x` Int32, `y` String)")]
    [InlineData("Tuple(`a b` Int32, String)", "Tuple(`a b` Int32, String)")]
    [InlineData("Enum8('a' = 1, 'it\\'s' = 2)", "Enum8('a' = 1, 'it\\'s' = 2)")]
    [InlineData("FixedString(16)", "FixedString(16)")]
    [InlineData("Tuple()", "Tuple")]
    public void Parses_and_renders(string text, string rendered) => Assert.Equal(rendered, ChTypes.Parse(text).Render());

    [Fact]
    public void Tuple_field_names_are_captured()
    {
        var t = ChTypes.Parse("Tuple(x Int32, y Nullable(String))");
        Assert.Equal(["x", "y"], t.TypeArgs.Select(a => a.FieldName));
        Assert.Equal("Nullable", t.TypeArgs.Last().Name);
    }

    [Fact]
    public void Unwrapped_strips_nullable_and_low_cardinality() =>
        Assert.Equal("String", ChTypes.Parse("LowCardinality(Nullable(String))").Unwrapped.Name);

    [Theory]
    [InlineData("JSON(max_dynamic_paths=10, a.b UInt32, SKIP a.c)")]
    [InlineData("AggregateFunction(quantiles(0.5, 0.9), Float64)")]
    [InlineData("Variant(String, UInt64)")]
    public void Unusual_parameters_fall_back_to_raw_literals_without_throwing(string text) =>
        Assert.Equal(text.Split('(')[0], ChTypes.Parse(text).Name);

    [Theory]
    [InlineData("")]
    [InlineData("Nullable(String")]
    [InlineData("Array(String))")]
    public void Malformed_text_throws_FormatException(string text) =>
        Assert.Throws<FormatException>(() => ChTypes.Parse(text));
}
