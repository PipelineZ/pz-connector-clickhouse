using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChSourceBehaviorTests
{
    private readonly ClickHouseFixture _ch;

    public ChSourceBehaviorTests(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
    }

    private async Task<ISource> OpenAsync(string db) =>
        await ((ISourceConnector)new ChConnector()).OpenAsync(new ConnectorConfig(_ch.ConnectionConfig(db)), default);

    private static async Task<List<RecordBatch>> ReadAll(ISource source, DatasetSpec spec, ReadHints? hints = null, CancellationToken ct = default)
    {
        var batches = new List<RecordBatch>();
        foreach (var partition in await source.PlanReadAsync(spec, hints ?? ReadHints.None, ct))
        {
            await foreach (var b in partition.ReadAsync(BatchOptions.Default, ct)) batches.Add(b);
        }

        return batches;
    }

    private static DatasetSpec Spec(string dataset, Dictionary<string, object?>? options = null) => new("clickhouse", dataset, options ?? []);

    [SkippableFact]
    public async Task Types_arrive_in_their_declared_arrow_form_and_schema_matches_batches()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"""
            CREATE TABLE {db}.t (id UInt64, d Date, dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'), lc LowCardinality(String),
              n Nullable(Int32), a Array(Nullable(String)), tu Tuple(x Int32, y String), m Map(String, UInt64),
              e Enum8('a' = 1, 'b' = 2), u UUID, big Int128, `select` String, `order` UInt8)
            ENGINE = MergeTree ORDER BY id
            """);
        await _ch.QueryAsync($"INSERT INTO {db}.t VALUES (1, '2024-01-02', '2024-01-02 03:04:05', '2024-01-02 03:04:05.678', 'x', NULL, ['a', NULL], (1, 'b'), {{'k': 1}}, 'b', '00000000-0000-0000-0000-000000000001', 170141183460469231731687303715884105727, 's', 2)");
        await using var source = await OpenAsync(db);
        var schema = (await source.GetSchemaAsync(Spec("t"), default)).Schema;
        var batches = await ReadAll(source, Spec("t"));
        var batch = Assert.Single(batches);
        Assert.Equal(schema.FieldsList.Select(f => (f.Name, f.DataType.TypeId)), batch.Schema.FieldsList.Select(f => (f.Name, f.DataType.TypeId)));

        IArrowType Type(string name) => schema.GetFieldByName(name).DataType;
        Assert.IsType<Date32Type>(Type("d"));
        Assert.IsType<TimestampType>(Type("dt"));
        Assert.IsType<TimestampType>(Type("dt64"));
        Assert.IsType<StringType>(Type("lc"));
        Assert.True(schema.GetFieldByName("n").IsNullable);
        Assert.IsType<ListType>(Type("a"));
        Assert.IsType<StructType>(Type("tu"));
        Assert.IsType<MapType>(Type("m"));
        Assert.IsType<StringType>(Type("e"));
        Assert.IsType<StringType>(Type("u"));
        Assert.IsType<StringType>(Type("big"));
        Assert.Equal("b", ((StringArray)batch.Column("e")).GetString(0));
        Assert.Equal("170141183460469231731687303715884105727", ((StringArray)batch.Column("big")).GetString(0));
        Assert.Equal("s", ((StringArray)batch.Column("select")).GetString(0));
        Assert.Equal(new DateOnly(2024, 1, 2), ((Date32Array)batch.Column("d")).GetDateOnly(0));
    }

    [SkippableFact]
    public async Task Incremental_read_with_nullable_datetime_cursor_and_duckdb_text_value()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.ev (id UInt64, ts Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY id");
        await _ch.QueryAsync($"INSERT INTO {db}.ev VALUES (1, '2024-05-01 09:00:00'), (2, '2024-05-01 10:00:00'), (3, '2024-05-01 11:00:00'), (4, NULL)");
        await using var source = await OpenAsync(db);
        var spec = Spec("ev") with { WatermarkCursor = "ts", WatermarkValue = "2024-05-01 10:00:00+00" };
        var ids = (await ReadAll(source, spec)).SelectMany(b => Enumerable.Range(0, b.Length).Select(i => ((UInt64Array)b.Column("id")).GetValue(i))).ToList();
        Assert.Equal([3UL], ids);
    }

    [SkippableFact]
    public async Task Timestamp_bounds_are_instants_whatever_the_column_timezone()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.tk (id UInt64, ts DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY id");
        await _ch.QueryAsync($"INSERT INTO {db}.tk VALUES (1, toDateTime('2024-05-01 09:00:00', 'UTC')), (2, toDateTime('2024-05-01 10:00:00', 'UTC')), (3, toDateTime('2024-05-01 11:00:00', 'UTC'))");
        await using var source = await OpenAsync(db);
        var spec = Spec("tk") with { WatermarkCursor = "ts", WatermarkValue = "2024-05-01 10:00:00+00" };
        var ids = (await ReadAll(source, spec)).SelectMany(b => Enumerable.Range(0, b.Length).Select(i => ((UInt64Array)b.Column("id")).GetValue(i))).ToList();
        Assert.Equal([3UL], ids);
    }

    [SkippableFact]
    public async Task DateTime64_cursor_in_a_non_utc_zone_honours_lower_and_upper_bounds()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.t64 (id UInt64, ts DateTime64(3, 'Asia/Tokyo')) ENGINE = MergeTree ORDER BY id");
        await _ch.QueryAsync($"INSERT INTO {db}.t64 VALUES (1, toDateTime64('2024-05-01 09:00:00', 3, 'UTC')), (2, toDateTime64('2024-05-01 10:00:00', 3, 'UTC')), (3, toDateTime64('2024-05-01 11:00:00', 3, 'UTC')), (4, toDateTime64('2024-05-01 12:00:00', 3, 'UTC'))");
        await using var source = await OpenAsync(db);
        var spec = Spec("t64") with { WatermarkCursor = "ts", WatermarkValue = "2024-05-01 10:00:00+00", WatermarkUpperBound = "2024-05-01 11:00:00+00" };
        var ids = (await ReadAll(source, spec)).SelectMany(b => Enumerable.Range(0, b.Length).Select(i => ((UInt64Array)b.Column("id")).GetValue(i))).ToList();
        Assert.Equal([3UL], ids);
    }

    [SkippableFact]
    public async Task Pushdown_prunes_columns_and_filters_rows()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.p (id UInt64, name String, extra String) ENGINE = MergeTree ORDER BY id");
        await _ch.QueryAsync($"INSERT INTO {db}.p SELECT number, toString(number), 'x' FROM numbers(10)");
        await using var source = await OpenAsync(db);
        var batches = await ReadAll(source, Spec("p"), new ReadHints(["name", "id"], "id >= 7", null));
        Assert.Equal(["name", "id"], batches[0].Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(3, batches.Sum(b => b.Length));
    }

    [SkippableFact]
    public async Task Rejected_predicate_falls_back_to_unfiltered_read()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.r (id UInt64, name String) ENGINE = MergeTree ORDER BY id");
        await _ch.QueryAsync($"INSERT INTO {db}.r SELECT number, 'abc' FROM numbers(5)");
        await using var source = await OpenAsync(db);
        var batches = await ReadAll(source, Spec("r"), new ReadHints(null, "regexp_matches(name, 'a.*')", null));
        Assert.Equal(5, batches.Sum(b => b.Length));
    }

    [SkippableFact]
    public async Task Server_failure_after_data_started_is_0203_never_a_short_read()
    {
        await using var source = await OpenAsync("default");
        var spec = Spec("boom", new() { ["query"] = "SELECT number, throwIf(number = 3000000) AS t FROM numbers(10000000)" });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAll(source, spec));
        Assert.Contains("PZCH0203", ex.Message);
    }

    [SkippableFact]
    public async Task Cancelling_mid_read_kills_the_server_query()
    {
        await using var source = await OpenAsync("default");
        var marker = Guid.NewGuid().ToString("N");
        var spec = Spec("slow", new() { ["query"] = $"SELECT number, '{marker}' AS m FROM numbers(100000000000)" });
        using var cts = new CancellationTokenSource();
        var partition = (await source.PlanReadAsync(spec, ReadHints.None, default)).Single();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var b in partition.ReadAsync(BatchOptions.Default, cts.Token))
            {
                b.Dispose();
                cts.Cancel();
            }
        });

        var running = "1";
        for (var attempt = 0; attempt < 100 && running != "0"; attempt++)
        {
            running = (await _ch.QueryAsync($"SELECT count() FROM system.processes WHERE query LIKE '%{marker}%' AND query NOT LIKE '%system.processes%'")).Trim();
            if (running != "0") await Task.Delay(100);
        }

        Assert.Equal("0", running);
    }

    [SkippableFact]
    public async Task Unknown_table_is_a_permanent_0203()
    {
        await using var source = await OpenAsync("default");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.GetSchemaAsync(Spec("no_such_table"), default));
        Assert.Contains("PZCH0203", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [SkippableFact]
    public async Task Unreadable_type_is_0204_at_schema_time()
    {
        await using var source = await OpenAsync("default");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(Spec("iv", new() { ["query"] = "SELECT toIntervalDay(1) AS i" }), default));
        Assert.Contains("PZCH0204", ex.Message);
    }
}
