using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse")]
[Trait("Category", "Docker")]
public sealed class ChConnectorCheckTests
{
    private readonly ClickHouseFixture _ch;

    public ChConnectorCheckTests(ClickHouseFixture ch)
    {
        _ch = ch;
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task Check_reports_version_and_user()
    {
        var check = await new ChConnector().CheckConnectionAsync(new ConnectorConfig(_ch.ConnectionConfig()), default);
        Assert.True(check.Ok, check.Message);
        Assert.Contains("ClickHouse 25.8", check.Message, StringComparison.Ordinal);
        Assert.Contains($"as {ClickHouseFixture.User}", check.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Bad_password_is_a_failed_check_with_0401_and_no_secret()
    {
        var values = _ch.ConnectionConfig();
        values["password"] = "wrong-password";
        var check = await new ChConnector().CheckConnectionAsync(new ConnectorConfig(values), default);
        Assert.False(check.Ok);
        Assert.Contains("PZCH0401", check.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("wrong-password", check.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Leftover_staging_tables_are_warned_about()
    {
        var db = await _ch.CreateDatabaseAsync();
        await _ch.QueryAsync($"CREATE TABLE {db}._pz_stg_00000000000000aa (x UInt8) ENGINE = MergeTree ORDER BY tuple()");
        var check = await new ChConnector().CheckConnectionAsync(new ConnectorConfig(_ch.ConnectionConfig(db)), default);
        Assert.True(check.Ok);
        Assert.Contains("1 leftover _pz_stg_* table", check.Message, StringComparison.Ordinal);
    }
}
