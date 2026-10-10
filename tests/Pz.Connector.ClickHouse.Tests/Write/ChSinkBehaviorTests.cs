using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChSinkBehaviorTests
{
    private readonly ClickHouseFixture _ch;

    public ChSinkBehaviorTests(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
    }

    internal static Schema Schema(params Field[] fields) => new(fields, null);

    internal static RecordBatch Rows(Schema schema, params (long Id, string? Name)[] rows)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (id, name) in rows)
        {
            ids.Append(id);
            if (name is null) names.AppendNull(); else names.Append(name);
        }

        return new RecordBatch(schema, [ids.Build(), names.Build()], rows.Length);
    }

    internal static readonly Schema IdName = Schema(new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, true));

    internal static async Task WriteAsync(ClickHouseFixture ch, string db, OutputSpec spec, Schema schema, params RecordBatch[] batches)
    {
        await using var sink = await ((ISinkConnector)new ChConnector()).OpenAsync(new ConnectorConfig(ch.ConnectionConfig(db)), default);
        await using var session = await sink.BeginWriteAsync(spec, schema, default);
        foreach (var b in batches)
        {
            await session.WriteBatchAsync(b, default);
            b.Dispose();
        }

        await session.CommitAsync(default);
    }

    internal static OutputSpec Out(string output, string mode, Dictionary<string, object?>? options = null, string[]? keys = null) =>
        new OutputSpec("clickhouse", output, mode, "fail_on_change", options ?? []) { Keys = keys ?? [] };

    [SkippableFact]
    public async Task Append_creates_the_target_from_the_write_schema()
    {
        var db = await _ch.CreateDatabaseAsync();
        var schema = Schema(new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, true),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true));
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC")).Append(DateTimeOffset.Parse("2024-01-02T03:04:05.123456Z")).Build();
        var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build(), new StringArray.Builder().AppendNull().Build(), ts], 1);
        await WriteAsync(_ch, db, Out("t", "append"), schema, batch);

        Assert.Equal("id\tInt64\nname\tNullable(String)\nts\tNullable(DateTime64(6, 'UTC'))",
            string.Join("\n", (await _ch.QueryAsync($"SELECT name, type FROM system.columns WHERE database = '{db}' AND table = 't' ORDER BY position FORMAT TabSeparatedRaw")).Trim().Split('\n')));
        Assert.Equal("1\t\\N\t2024-01-02 03:04:05.123456", (await _ch.QueryAsync($"SELECT * FROM {db}.t FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Append_is_at_least_once_a_second_commit_adds_again()
    {
        var db = await _ch.CreateDatabaseAsync();
        await WriteAsync(_ch, db, Out("t", "append"), IdName, Rows(IdName, (1, "a"), (2, "b")));
        await WriteAsync(_ch, db, Out("t", "append"), IdName, Rows(IdName, (1, "a"), (2, "b")));
        Assert.Equal("4", (await _ch.QueryAsync($"SELECT count() FROM {db}.t")).Trim());
    }

    [SkippableFact]
    public async Task Keyword_column_names_round_trip()
    {
        var db = await _ch.CreateDatabaseAsync();
        var schema = Schema(new Field("select", Int64Type.Default, false), new Field("order", StringType.Default, true));
        await WriteAsync(_ch, db, Out("t", "append"), schema, Rows(schema, (7, "x")));
        Assert.Equal("7\tx", (await _ch.QueryAsync($"SELECT `select`, `order` FROM {db}.t FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Replace_keeps_the_existing_table_definition_and_swaps_the_rows()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.t (id Int64, name Nullable(String)) ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id SETTINGS index_granularity = 128");
        await _ch.QueryAsync($"INSERT INTO {db}.t VALUES (100, 'old'), (101, 'old')");
        var before = (await _ch.QueryAsync($"SELECT engine_full FROM system.tables WHERE database = '{db}' AND name = 't' FORMAT TSV")).Trim();

        await WriteAsync(_ch, db, Out("t", "replace"), IdName, Rows(IdName, (1, "new")));

        Assert.Equal(before, (await _ch.QueryAsync($"SELECT engine_full FROM system.tables WHERE database = '{db}' AND name = 't' FORMAT TSV")).Trim());
        Assert.Equal("1\tnew", (await _ch.QueryAsync($"SELECT * FROM {db}.t FORMAT TSV")).Trim());
        Assert.Equal("0", (await _ch.QueryAsync($"SELECT count() FROM system.tables WHERE database = '{db}' AND name LIKE '%pz%'")).Trim());
    }

    [SkippableFact]
    public async Task Replace_onto_a_missing_target_creates_it()
    {
        var db = await _ch.CreateDatabaseAsync();
        await WriteAsync(_ch, db, Out("t", "replace"), IdName, Rows(IdName, (1, "x")));
        Assert.Equal("1", (await _ch.QueryAsync($"SELECT count() FROM {db}.t")).Trim());
        Assert.Equal("0", (await _ch.QueryAsync($"SELECT count() FROM system.tables WHERE database = '{db}' AND name LIKE '%pz%'")).Trim());
    }

    [SkippableFact]
    public async Task Empty_append_still_creates_the_target()
    {
        var db = await _ch.CreateDatabaseAsync();
        await WriteAsync(_ch, db, Out("t", "append"), IdName);
        Assert.Equal("0", (await _ch.QueryAsync($"SELECT count() FROM {db}.t")).Trim());
    }

    [SkippableFact]
    public async Task Abort_leaves_no_staging_table_and_no_target()
    {
        var db = await _ch.CreateDatabaseAsync();
        await using (var sink = await ((ISinkConnector)new ChConnector()).OpenAsync(new ConnectorConfig(_ch.ConnectionConfig(db)), default))
        {
            await using var session = await sink.BeginWriteAsync(Out("t", "append"), IdName, default);
            using var b = Rows(IdName, (1, "a"));
            await session.WriteBatchAsync(b, default);
            await session.AbortAsync(default);
        }

        Assert.Equal("0", (await _ch.QueryAsync($"SELECT count() FROM system.tables WHERE database = '{db}'")).Trim());
    }

    [SkippableFact]
    public async Task Missing_target_column_is_0304()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.t (id Int64) ENGINE = MergeTree ORDER BY id");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(_ch, db, Out("t", "append"), IdName, Rows(IdName, (1, "a"))));
        Assert.Contains("PZCH0304", ex.Message);
        Assert.Contains("'name'", ex.Message);
    }

    [SkippableFact]
    public async Task Unwritable_type_is_0303_before_any_table_exists()
    {
        var db = await _ch.CreateDatabaseAsync();
        var schema = Schema(new Field("d", new DictionaryType(Int32Type.Default, StringType.Default, false), true));
        await using var sink = await ((ISinkConnector)new ChConnector()).OpenAsync(new ConnectorConfig(_ch.ConnectionConfig(db)), default);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(Out("t", "append"), schema, default));
        Assert.Contains("PZCH0303", ex.Message);
        Assert.Equal("0", (await _ch.QueryAsync($"SELECT count() FROM system.tables WHERE database = '{db}'")).Trim());
    }

    [SkippableFact]
    public async Task Unknown_write_mode_is_0301()
    {
        var db = await _ch.CreateDatabaseAsync();
        await using var sink = await ((ISinkConnector)new ChConnector()).OpenAsync(new ConnectorConfig(_ch.ConnectionConfig(db)), default);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(Out("t", "upsert"), IdName, default));
        Assert.Contains("PZCH0301", ex.Message);
    }
}
