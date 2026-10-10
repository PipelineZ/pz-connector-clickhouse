using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChConnectorTests
{
    [Fact]
    public void Info_names_the_connector_clickhouse()
    {
        var connector = new ChConnector();
        Assert.Equal("clickhouse", connector.Info.Name);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
    }

    [Fact]
    public void Capabilities_are_exactly_the_spec_set()
    {
        var expected = ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown
            | ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound
            | ConnectorCapabilities.Merge | ConnectorCapabilities.ReplaceWrites;
        Assert.Equal(expected, new ChConnector().Capabilities);
    }
}
