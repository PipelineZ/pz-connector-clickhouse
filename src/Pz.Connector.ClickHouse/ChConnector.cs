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
    private readonly Func<ChConnectionConfig, HttpClient> _httpClientFactory;

    public ChConnector(ILoggerFactory? loggerFactory = null)
        : this(loggerFactory, ChHttpClient.Create)
    {
    }

    /// <summary>Test-only: supplies the <see cref="HttpClient"/> every request goes through, so a fake
    /// handler can stand in for the network.</summary>
    internal ChConnector(ILoggerFactory? loggerFactory, Func<ChConnectionConfig, HttpClient> httpClientFactory)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _httpClientFactory = httpClientFactory;
    }

    internal ChHttpClient OpenClient(ChConnectionConfig cfg) =>
        new(_httpClientFactory(cfg), cfg, _loggerFactory.CreateLogger<ChHttpClient>());

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

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "entity": { "type": "string" },
            "query": { "type": "string" },
            "settings": { "type": "object" } },
          "additionalProperties": false }
        """;

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

    /// <summary>Proves the credential, the cluster (when named) and reports leftovers from crashed
    /// writes. A leftover staging table is a warning, not a failure: it costs disk, not correctness.</summary>
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var cfg = ChConnectionConfig.Parse(config, errors);
        if (cfg is null)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        using var client = OpenClient(cfg);
        try
        {
            var info = await ChTopology.ValidateDeploymentAsync(client, forWrites: true, ct).ConfigureAwait(false);
            var message = $"ClickHouse {info.Version} as {info.User}{(info.IsCloud ? " (Cloud)" : "")}";
            if (cfg.Cluster is { } cluster && await ChTopology.GetClusterAsync(client, cluster, ct).ConfigureAwait(false) is { } shape)
            {
                message += $", cluster {cluster}: {shape.Shards} shard(s) x {shape.Replicas} replica(s)";
            }

            var orphans = await ChTopology.OrphanStagingTablesAsync(client, cfg.Database, ct).ConfigureAwait(false);
            if (orphans.Count > 0)
            {
                message += $"; warning: {orphans.Count} leftover _pz_stg_* table(s) in {cfg.Database} from interrupted writes -- safe to DROP";
            }

            return new ConnectionCheck(true, message);
        }
        catch (PzConnectorException ex)
        {
            return new ConnectionCheck(false, ex.Message);
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var cfg = ParseOrThrow(config);
        // The client belongs to the source from here on; the engine disposes the source exactly once.
        return ValueTask.FromResult<ISource>(new ChSource(OpenClient(cfg), _loggerFactory.CreateLogger<ChSource>()));
    }

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException("Task 13");
}
