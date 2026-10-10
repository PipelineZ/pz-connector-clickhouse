namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChTypesReadTests
{
    private static ReadTreatment R(string type) => ChTypes.ForRead(ChTypes.Parse(type));

    [Theory]
    [InlineData("UInt64")] [InlineData("Int32")] [InlineData("Float64")] [InlineData("Bool")] [InlineData("String")]
    [InlineData("Nullable(String)")] [InlineData("LowCardinality(String)")] [InlineData("Decimal(38, 9)")]
    [InlineData("DateTime64(6, 'UTC')")] [InlineData("FixedString(4)")] [InlineData("Array(Int64)")]
    [InlineData("Map(String, Array(Nullable(Float64)))")] [InlineData("Tuple(a Int32, b String)")]
    public void Native_types_pass_through(string type) => Assert.Equal(ReadKind.AsIs, R(type).Kind);

    [Theory]
    [InlineData("Date", "toDate32({0})")]
    [InlineData("Nullable(Date)", "toDate32({0})")]
    [InlineData("Date32", "toDate32({0})")]
    [InlineData("DateTime", "toDateTime64({0}, 0)")]
    [InlineData("DateTime('Asia/Tokyo')", "toDateTime64({0}, 0, 'Asia/Tokyo')")]
    [InlineData("Enum8('a' = 1)", "toString({0})")]
    [InlineData("UUID", "toString({0})")]
    [InlineData("IPv4", "toString({0})")]
    [InlineData("IPv6", "toString({0})")]
    [InlineData("Int128", "toString({0})")]
    [InlineData("UInt256", "toString({0})")]
    [InlineData("JSON", "toString({0})")]
    [InlineData("Dynamic", "toString({0})")]
    [InlineData("Variant(String, UInt64)", "toString({0})")]
    [InlineData("Array(Date)", "toString({0})")]
    [InlineData("Tuple(a UUID, b Int32)", "toString({0})")]
    public void Unusable_arrow_forms_are_cast(string type, string template)
    {
        var t = R(type);
        Assert.Equal(ReadKind.Cast, t.Kind);
        Assert.Equal(template, t.CastTemplate);
    }

    [Theory]
    [InlineData("IntervalDay")] [InlineData("Time")] [InlineData("Time64(3)")] [InlineData("Nothing")]
    [InlineData("AggregateFunction(sum, UInt64)")] [InlineData("SimpleAggregateFunction(sum, UInt64)")]
    [InlineData("Array(IntervalSecond)")]
    public void Unreadable_types_are_refused(string type) => Assert.Equal(ReadKind.Refuse, R(type).Kind);

    [Theory]
    [InlineData("UInt64", "UInt64")]
    [InlineData("Nullable(Int32)", "Int32")]
    [InlineData("Decimal(18, 4)", "Decimal(18, 4)")]
    [InlineData("Date", "Date")]
    [InlineData("Date32", "Date32")]
    [InlineData("Nullable(DateTime)", "DateTime")]
    [InlineData("DateTime64(3, 'UTC')", "DateTime64(3, 'UTC')")]
    [InlineData("LowCardinality(String)", "String")]
    public void Cursor_parameter_types(string type, string expected) =>
        Assert.Equal(expected, ChTypes.CursorParameterType(ChTypes.Parse(type)));

    [Theory]
    [InlineData("Float64")] [InlineData("UUID")] [InlineData("Array(Int32)")] [InlineData("Bool")] [InlineData("Enum8('a' = 1)")]
    public void Unsupported_cursor_types_are_null(string type) => Assert.Null(ChTypes.CursorParameterType(ChTypes.Parse(type)));
}
