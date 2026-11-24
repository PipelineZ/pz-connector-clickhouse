using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChNestedTests
{
    private readonly ClickHouseFixture _ch;

    public ChNestedTests(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
    }

    private static ListType TagsType => new(new Field("item", StringType.Default, true));

    private static Schema NestedSchema() => new([
        new Field("id", Int64Type.Default, false),
        new Field("tags", TagsType, true),
        new Field("rec", new StructType([new Field("n", Int32Type.Default, true), new Field("s", StringType.Default, true)]), true),
        new Field("m", new MapType(StringType.Default, Int32Type.Default), true),
        new Field("pair", new FixedSizeListType(new Field("item", Int32Type.Default, true), 2), true),
    ], null);

    // Row 0 fully populated; row 1 has every nested column null; row 2 has empty containers and null leaves.
    private static RecordBatch NestedBatch()
    {
        var ids = new Int64Array.Builder().AppendRange([0, 1, 2]).Build();

        var tags = new ListArray.Builder(StringType.Default);
        var tagValues = (StringArray.Builder)tags.ValueBuilder;
        tags.Append();
        tagValues.Append("a");
        tagValues.AppendNull();
        tags.AppendNull();
        tags.Append();

        var recType = (StructType)NestedSchema().GetFieldByName("rec").DataType;
        var rec = new StructArray(recType, 3,
            [new Int32Array.Builder().Append(7).AppendNull().AppendNull().Build(), new StringArray.Builder().Append("x").AppendNull().AppendNull().Build()],
            new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build(), nullCount: 1);

        var map = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
        var keys = (StringArray.Builder)map.KeyBuilder;
        var values = (Int32Array.Builder)map.ValueBuilder;
        map.Append();
        keys.Append("k1");
        values.Append(1);
        keys.Append("k2");
        values.AppendNull();
        map.AppendNull();
        map.Append();

        var pairType = (FixedSizeListType)NestedSchema().GetFieldByName("pair").DataType;
        var pairValues = new Int32Array.Builder().Append(1).Append(2).Append(0).Append(0).AppendNull().Append(4).Build();
        var pair = new FixedSizeListArray(new ArrayData(pairType, 3, 1, 0,
            [new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build()], [pairValues.Data]));

        return new RecordBatch(NestedSchema(), [ids, tags.Build(), rec, map.Build(), pair], 3);
    }

    [SkippableFact]
    public async Task Sink_creates_native_nested_columns_and_lands_nulls_as_defaults()
    {
        var db = await _ch.CreateDatabaseAsync();
        var spec = new OutputSpec("clickhouse", "nested_t", "append", "fail_on_change", new Dictionary<string, object?>());
        await ChSinkBehaviorTests.WriteAsync(_ch, db, spec, NestedSchema(), NestedBatch());

        Assert.Equal("Array(Nullable(String))\tTuple(\\n    n Nullable(Int32),\\n    s Nullable(String))\tMap(String, Nullable(Int32))\tArray(Nullable(Int32))",
            (await _ch.QueryAsync($"SELECT any(toTypeName(tags)), any(toTypeName(rec)), any(toTypeName(m)), any(toTypeName(pair)) FROM {db}.nested_t")).Trim());
        Assert.Equal("['a',NULL]\t(7,'x')\t{'k1':1,'k2':NULL}\t[1,2]\n[]\t(NULL,NULL)\t{}\t[]\n[]\t(NULL,NULL)\t{}\t[NULL,4]",
            (await _ch.QueryAsync($"SELECT tags, rec, m, pair FROM {db}.nested_t ORDER BY id")).Trim().Replace("\r", ""));
    }

    [SkippableFact]
    public async Task Source_reads_nested_columns_as_arrow_nested_types()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"""
            CREATE TABLE {db}.t (id UInt64, a Array(Nullable(Int64)), lc Array(LowCardinality(String)), named Tuple(x Int32, y Nullable(String)),
              unnamed Tuple(Int32, String), m Map(String, Array(Int32)), ln Array(LowCardinality(Nullable(String))), deep Array(Tuple(k String, v Float64)))
            ENGINE = MergeTree ORDER BY id
            """);
        await _ch.QueryAsync($"INSERT INTO {db}.t VALUES (1, [1, NULL], ['p'], (1, NULL), (2, 'q'), {{'k': [1, 2]}}, ['z', NULL], [('a', 1.5)])");
        await using var source = await ((ISourceConnector)new ChConnector()).OpenAsync(new ConnectorConfig(_ch.ConnectionConfig(db)), default);
        var spec = new DatasetSpec("clickhouse", "t", new Dictionary<string, object?>());
        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.IsType<ListType>(schema.GetFieldByName("a").DataType);
        Assert.IsType<ListType>(schema.GetFieldByName("lc").DataType);
        Assert.IsType<MapType>(schema.GetFieldByName("m").DataType);
        Assert.IsType<ListType>(schema.GetFieldByName("ln").DataType);
        var named = Assert.IsType<StructType>(schema.GetFieldByName("named").DataType);
        Assert.Equal(["x", "y"], named.Fields.Select(f => f.Name));
        var unnamed = Assert.IsType<StructType>(schema.GetFieldByName("unnamed").DataType);
        Assert.Equal(["1", "2"], unnamed.Fields.Select(f => f.Name));
        var deep = Assert.IsType<ListType>(schema.GetFieldByName("deep").DataType);
        Assert.IsType<StructType>(deep.ValueDataType);
    }
}
