using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>ClickHouse for pz: a source reads a table or a query as Arrow over the HTTP interface; a
/// sink streams Arrow into a staging table and lands it with one append/replace/merge step.</summary>
public sealed class ChConnector : IConnector, ISourceConnector, ISinkConnector, IOutputConfigSchema
{
    private readonly ILoggerFactory _loggerFactory;

    public ChConnector(ILoggerFactory? loggerFactory = null)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
    }

    public ConnectorInfo Info { get; } = new(
        "clickhouse",
        typeof(ChConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow
        | ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.Merge | ConnectorCapabilities.ReplaceWrites;

    public string ConnectionConfigSchema => "{}";

    public string DatasetConfigSchema => "{}";

    public string OutputConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException("Task 8");

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException("Task 11");

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException("Task 13");
}
