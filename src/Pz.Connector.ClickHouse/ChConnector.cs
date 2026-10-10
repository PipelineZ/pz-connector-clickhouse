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

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["url"], "properties": {
            "url": { "type": "string" },
            "user": { "type": "string" },
            "password": { "type": "string" },
            "database": { "type": "string" },
            "cluster": { "type": "string" },
            "tls_ca_file": { "type": "string" },
            "settings": { "type": "object" },
            "base_dir": { "type": "string" } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => "{}";

    public string OutputConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        ChConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    // Parse codes each error where it is raised, so joining them adds no second prefix.
    internal static ChConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return ChConnectionConfig.Parse(config, errors)
            ?? throw new PzConnectorException(string.Join("; ", errors), isTransient: false);
    }

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException("Task 8");

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException("Task 11");

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException("Task 13");
}
