using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using static Pz.Connector.ClickHouse.Tests.ChSinkBehaviorTests;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChSinkMergeTests
{
    private readonly ClickHouseFixture _ch;

    public ChSinkMergeTests(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
    }

    private static OutputSpec Merge(string output, string? strategy = null) =>
        Out(output, "merge", strategy is null ? null : new() { ["merge_strategy"] = strategy }, keys: ["id"]);

    [SkippableFact]
    public async Task Replacing_keeps_the_latest_row_per_key_within_a_write_and_across_runs()
    {
        var db = await _ch.CreateDatabaseAsync();
        await WriteAsync(_ch, db, Merge("t"), IdName, Rows(IdName, (1, "a1"), (2, "b1"), (1, "a2")));
        await WriteAsync(_ch, db, Merge("t"), IdName, Rows(IdName, (2, "b2"), (3, "c1")));

        Assert.Contains("ReplacingMergeTree(_pz_version)", await _ch.QueryAsync($"SELECT engine_full FROM system.tables WHERE database = '{db}' AND name = 't'"), StringComparison.Ordinal);
        Assert.Equal("1\ta2\n2\tb2\n3\tc1", (await _ch.QueryAsync($"SELECT id, name FROM {db}.t FINAL ORDER BY id FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Delete_insert_is_correct_without_FINAL_on_a_plain_merge_tree()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.t (id Int64, name Nullable(String)) ENGINE = MergeTree ORDER BY id");
        await _ch.QueryAsync($"INSERT INTO {db}.t VALUES (1, 'old'), (2, 'keep')");
        await WriteAsync(_ch, db, Merge("t", "delete_insert"), IdName, Rows(IdName, (1, "x"), (1, "new"), (3, "c")));
        Assert.Equal("1\tnew\n2\tkeep\n3\tc", (await _ch.QueryAsync($"SELECT id, name FROM {db}.t ORDER BY id FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Delete_insert_onto_a_missing_target_creates_a_plain_merge_tree()
    {
        var db = await _ch.CreateDatabaseAsync();
        await WriteAsync(_ch, db, Merge("t", "delete_insert"), IdName, Rows(IdName, (1, "a"), (1, "b")));
        Assert.Equal("MergeTree", (await _ch.QueryAsync($"SELECT engine FROM system.tables WHERE database = '{db}' AND name = 't' FORMAT TSV")).Trim());
        Assert.Equal("1\tb", (await _ch.QueryAsync($"SELECT id, name FROM {db}.t FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Replacing_merge_into_a_non_replacing_table_is_0302_with_the_hint()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.t (id Int64, name Nullable(String)) ENGINE = MergeTree ORDER BY id");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(_ch, db, Merge("t"), IdName, Rows(IdName, (1, "a"))));
        Assert.Contains("PZCH0302", ex.Message);
        Assert.Contains("delete_insert", ex.Message);
    }

    [SkippableFact]
    public async Task Existing_replacing_table_with_its_own_version_column_needs_it_in_the_write()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}.t (id Int64, name Nullable(String), updated UInt64) ENGINE = ReplacingMergeTree(updated) ORDER BY id");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(_ch, db, Merge("t"), IdName, Rows(IdName, (1, "a"))));
        Assert.Contains("PZCH0304", ex.Message);
        Assert.Contains("'updated'", ex.Message);

        var withVersion = Schema(new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, true), new Field("updated", UInt64Type.Default, false));
        var batch = new RecordBatch(withVersion, [new Int64Array.Builder().Append(1).Append(1).Build(), new StringArray.Builder().Append("v2").Append("v1").Build(),
            new UInt64Array.Builder().Append(2).Append(1).Build()], 2);
        await WriteAsync(_ch, db, Merge("t"), withVersion, batch);
        Assert.Equal("1\tv1", (await _ch.QueryAsync($"SELECT id, name FROM {db}.t FINAL FORMAT TSV")).Trim());
    }

    [SkippableFact]
    public async Task Missing_or_reserved_keys_are_0308()
    {
        var db = await _ch.CreateDatabaseAsync();
        await using var sink = await ((ISinkConnector)new ChConnector()).OpenAsync(new ConnectorConfig(_ch.ConnectionConfig(db)), default);
        var noKeys = Out("t", "merge");
        Assert.Contains("PZCH0308", (await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(noKeys, IdName, default))).Message);
        var reserved = Schema(new Field("id", Int64Type.Default, false), new Field("_pz_seq", Int64Type.Default, false));
        Assert.Contains("PZCH0308", (await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(Merge("t"), reserved, default))).Message);
    }
}
