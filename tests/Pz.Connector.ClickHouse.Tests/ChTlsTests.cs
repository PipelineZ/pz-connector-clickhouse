using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

[Collection("clickhouse-tls")]
[Trait("Category", "Docker")]
public sealed class ChTlsTests
{
    private readonly ClickHouseTlsFixture _tls;

    public ChTlsTests(ClickHouseTlsFixture tls)
    {
        _tls = tls;
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task Private_ca_through_tls_ca_file_connects()
    {
        var check = await new ChConnector().CheckConnectionAsync(new ConnectorConfig(_tls.ConnectionConfig(withCa: true)), default);
        Assert.True(check.Ok, check.Message);
    }

    [SkippableFact]
    public async Task Without_the_ca_the_handshake_fails_permanently_with_a_hint()
    {
        var check = await new ChConnector().CheckConnectionAsync(new ConnectorConfig(_tls.ConnectionConfig(withCa: false)), default);
        Assert.False(check.Ok);
        Assert.Contains("PZCH0401", check.Message, StringComparison.Ordinal);
        Assert.Contains("tls_ca_file", check.Message, StringComparison.Ordinal);
    }
}
