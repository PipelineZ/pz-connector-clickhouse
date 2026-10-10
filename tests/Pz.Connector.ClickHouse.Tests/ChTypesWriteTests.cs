using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChTypesWriteTests
{
    private static string? Ddl(IArrowType type, bool nullable = false, bool forceNotNull = false) =>
        ChTypes.ToDdl(new Field("c", type, nullable), forceNotNull, out _);

    [Fact]
    public void Scalars_map_per_spec()
    {
        Assert.Equal("Bool", Ddl(BooleanType.Default));
        Assert.Equal("Int8", Ddl(Int8Type.Default));
        Assert.Equal("UInt64", Ddl(UInt64Type.Default));
        Assert.Equal("Float32", Ddl(HalfFloatType.Default));
        Assert.Equal("Float64", Ddl(DoubleType.Default));
        Assert.Equal("Decimal(38, 9)", Ddl(new Decimal128Type(38, 9)));
        Assert.Equal("Decimal(76, 2)", Ddl(new Decimal256Type(76, 2)));
        Assert.Equal("String", Ddl(StringType.Default));
        Assert.Equal("String", Ddl(BinaryType.Default));
        Assert.Equal("FixedString(16)", Ddl(new FixedSizeBinaryType(16)));
        Assert.Equal("Date32", Ddl(Date32Type.Default));
        Assert.Equal("Date32", Ddl(Date64Type.Default));
        Assert.Equal("DateTime64(6, 'UTC')", Ddl(new TimestampType(TimeUnit.Microsecond, "UTC")));
        Assert.Equal("DateTime64(9)", Ddl(new TimestampType(TimeUnit.Nanosecond, (string?)null)));
        Assert.Equal("DateTime64(0)", Ddl(new TimestampType(TimeUnit.Second, (string?)null)));
    }

    [Fact]
    public void Nullable_wraps_scalars_unless_forced_not_null()
    {
        Assert.Equal("Nullable(String)", Ddl(StringType.Default, nullable: true));
        Assert.Equal("String", Ddl(StringType.Default, nullable: true, forceNotNull: true));
    }

    [Fact]
    public void Containers_map_and_never_wrap_the_container_itself()
    {
        var list = new ListType(new Field("item", StringType.Default, true));
        Assert.Equal("Array(Nullable(String))", Ddl(list, nullable: true));

        var st = new StructType([new Field("a", Int32Type.Default, false), new Field("b c", StringType.Default, true)]);
        Assert.Equal("Tuple(`a` Int32, `b c` Nullable(String))", Ddl(st, nullable: true));

        var map = new MapType(new Field("key", StringType.Default, false), new Field("value", Int64Type.Default, true));
        Assert.Equal("Map(String, Nullable(Int64))", Ddl(map));
    }

    [Fact]
    public void Refused_types_set_an_error()
    {
        Assert.Null(ChTypes.ToDdl(new Field("d", new DictionaryType(Int32Type.Default, StringType.Default, false), true), false, out var e1));
        Assert.Contains("dictionary", e1!, StringComparison.OrdinalIgnoreCase);
        Assert.Null(ChTypes.ToDdl(new Field("t", Time64Type.Default, true), false, out var e2));
        Assert.NotNull(e2);
        Assert.Null(ChTypes.ToDdl(new Field("n", NullType.Default, true), false, out _));
        Assert.Null(ChTypes.ToDdl(new Field("l", new ListType(new Field("x", Time64Type.Default, true)), true), false, out _));
    }
}
