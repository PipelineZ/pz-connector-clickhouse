using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.ClickHouse.Tests;

/// <summary>TestKit source contract against the single-node fixture. <c>small</c> is 120 rows
/// (id UInt64 0..119, ~90-char name) so a 4 KB batch target yields several batches; <c>large</c> is
/// 150k rows so mid-read cancellation is observable; <c>window</c> holds ids 0..10; <c>flaky</c> is a
/// query that exceeds its own <c>max_execution_time</c> (TIMEOUT_EXCEEDED, a transient code).</summary>
[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChSourceAcceptance : SourceConnectorAcceptanceTests
{
    private static readonly SemaphoreSlim Seed = new(1, 1);
    private static string? _db;
    private readonly ClickHouseFixture _ch;

    public ChSourceAcceptance(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
        SeedAsync().GetAwaiter().GetResult();
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new ChConnector();

    protected override ConnectorConfig ValidConfig => new(_ch.ConnectionConfig(_db!));

    protected override ConnectorConfig? ValidConfigWithIntegerOptionAsDouble
    {
        get
        {
            var values = _ch.ConnectionConfig(_db!);
            values["settings"] = new Dictionary<string, object?> { ["max_threads"] = 2.0 };
            return new ConnectorConfig(values);
        }
    }

    protected override DatasetSpec SmallDataset => new("clickhouse", "small", new Dictionary<string, object?>());

    protected override DatasetSpec? LargeDataset => new("clickhouse", "large", new Dictionary<string, object?>());

    protected override DatasetSpec? TransientFailureDataset => new("clickhouse", "flaky", new Dictionary<string, object?>
    {
        ["query"] = "SELECT number FROM numbers(10) WHERE sleepEachRow(0.3) = 0",
        ["settings"] = new Dictionary<string, object?> { ["max_execution_time"] = 1.0, ["max_block_size"] = 1.0 },
    });

    protected override DatasetSpec? BoundedWindowDataset =>
        new DatasetSpec("clickhouse", "window", new Dictionary<string, object?>()) { WatermarkCursor = "id", WatermarkValue = "3", WatermarkUpperBound = "7" };

    private async Task SeedAsync()
    {
        await Seed.WaitAsync();
        try
        {
            if (_db is not null)
            {
                return;
            }

            var db = await _ch.CreateDatabaseAsync();
            await _ch.QueryAsync($"CREATE TABLE {db}.small (id Int64, name String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8");
            await _ch.QueryAsync($"INSERT INTO {db}.small SELECT number, repeat('a', 90) FROM numbers(120)");
            await _ch.QueryAsync($"CREATE TABLE {db}.large (id Int64) ENGINE = MergeTree ORDER BY id");
            await _ch.QueryAsync($"INSERT INTO {db}.large SELECT number FROM numbers(150000)");
            await _ch.QueryAsync($"CREATE TABLE {db}.window (id Int64) ENGINE = MergeTree ORDER BY id");
            await _ch.QueryAsync($"INSERT INTO {db}.window SELECT number FROM numbers(11)");
            _db = db;
        }
        finally
        {
            Seed.Release();
        }
    }
}
