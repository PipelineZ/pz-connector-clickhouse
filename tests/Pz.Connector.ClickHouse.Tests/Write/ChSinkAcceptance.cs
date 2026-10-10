using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.ClickHouse.Tests;

/// <summary>TestKit sink contract against the single-node fixture. Each instance (xunit makes one
/// per fact) works in its own database so facts never share state; read-back goes through the
/// fixture's own HTTP, not the connector.</summary>
[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChSinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly ClickHouseFixture _ch;
    private readonly string _db;

    public ChSinkAcceptance(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
        _db = _ch.CreateDatabaseAsync().GetAwaiter().GetResult();
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new ChConnector();

    protected override ConnectorConfig ValidConfig => new(_ch.ConnectionConfig(_db));

    protected override OutputSpec SmallOutput => new("clickhouse", "append_out", "append", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? ReplaceOutput => new("clickhouse", "replace_out", "replace", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput =>
        new OutputSpec("clickhouse", "merge_out", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override Task ResetMergeTargetAsync() => _ch.QueryAsync($"DROP TABLE IF EXISTS {_db}.merge_out SYNC");

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var exists = (await _ch.QueryAsync($"EXISTS TABLE {_db}.{spec.Output}")).Trim() == "1";
        if (!exists)
        {
            return [];
        }

        var final = spec.Mode == "merge" ? " FINAL" : "";
        return await _ch.ReadArrowAsync($"SELECT id, name FROM {_db}.{spec.Output}{final} ORDER BY id");
    }
}
