using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace Pz.Connector.ClickHouse.Tests;

[CollectionDefinition("clickhouse-cluster")]
public sealed class ClickHouseClusterCollection : ICollectionFixture<ClickHouseClusterFixture>;

/// <summary>One ClickHouse Keeper and four servers forming <c>pz_cluster</c>: 2 shards x 2 replicas,
/// internal replication, each node's <c>{replica}</c> macro its own host name. A second cluster,
/// <c>pz_dup</c>, lists the same host twice so the replica-macro uniqueness check has something to
/// refuse. All nodes share one user so Distributed queries authenticate between nodes.</summary>
public sealed class ClickHouseClusterFixture : IAsyncLifetime
{
    public const string Cluster = "pz_cluster";
    private const string User = ClickHouseFixture.User;
    private const string Password = ClickHouseFixture.Password;
    private static readonly string[] Nodes = ["ch-s1r1", "ch-s1r2", "ch-s2r1", "ch-s2r2"];

    private INetwork? _network;
    private IContainer? _keeper;
    private readonly List<IContainer> _servers = [];
    private readonly List<HttpClient> _http = [];

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        _network = new NetworkBuilder().Build();
        await _network.CreateAsync().ConfigureAwait(false);

        _keeper = new ContainerBuilder("clickhouse/clickhouse-keeper:25.8")
            .WithNetwork(_network).WithNetworkAliases("keeper")
            .WithResourceMapping(Encoding.UTF8.GetBytes(KeeperXml), "/etc/clickhouse-keeper/keeper_config.xml")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(9181))
            .Build();
        await _keeper.StartAsync().ConfigureAwait(false);

        for (var i = 0; i < Nodes.Length; i++)
        {
            var shard = i < 2 ? 1 : 2;
            var server = new ContainerBuilder(ClickHouseFixture.Image)
                .WithNetwork(_network).WithNetworkAliases(Nodes[i])
                .WithEnvironment("CLICKHOUSE_USER", User).WithEnvironment("CLICKHOUSE_PASSWORD", Password)
                .WithEnvironment("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
                .WithResourceMapping(Encoding.UTF8.GetBytes(ServerXml(shard, Nodes[i])), "/etc/clickhouse-server/config.d/pz-cluster.xml")
                .WithPortBinding(8123, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8123).ForPath("/ping")))
                .Build();
            _servers.Add(server);
        }

        await Task.WhenAll(_servers.Select(s => s.StartAsync())).ConfigureAwait(false);
        foreach (var server in _servers)
        {
            var http = new HttpClient { BaseAddress = new Uri($"http://{server.Hostname}:{server.GetMappedPublicPort(8123)}/") };
            http.DefaultRequestHeaders.Add("X-ClickHouse-User", User);
            http.DefaultRequestHeaders.Add("X-ClickHouse-Key", Password);
            _http.Add(http);
        }
    }

    public async Task DisposeAsync()
    {
        _http.ForEach(h => h.Dispose());
        foreach (var server in _servers)
        {
            await server.DisposeAsync().ConfigureAwait(false);
        }

        if (_keeper is not null) await _keeper.DisposeAsync().ConfigureAwait(false);
        if (_network is not null) await _network.DisposeAsync().ConfigureAwait(false);
    }

    public string Url(int node) => _http[node - 1].BaseAddress!.ToString().TrimEnd('/');

    public Dictionary<string, object?> ConnectionConfig(int node, string database) =>
        new() { ["url"] = Url(node), ["user"] = User, ["password"] = Password, ["database"] = database, ["cluster"] = Cluster };

    public async Task<string> QueryAsync(int node, string sql)
    {
        using var response = await _http[node - 1].PostAsync("/", new StringContent(sql, Encoding.UTF8)).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        return response.IsSuccessStatusCode ? text : throw new InvalidOperationException($"node {node}: {text}");
    }

    private static string Replica(string host) =>
        $"<replica><host>{host}</host><port>9000</port><user>{User}</user><password>{Password}</password></replica>";

    private static string ServerXml(int shard, string replica) => $"""
        <clickhouse>
          <remote_servers>
            <{Cluster}>
              <shard><internal_replication>true</internal_replication>{Replica("ch-s1r1")}{Replica("ch-s1r2")}</shard>
              <shard><internal_replication>true</internal_replication>{Replica("ch-s2r1")}{Replica("ch-s2r2")}</shard>
            </{Cluster}>
            <pz_dup>
              <shard>{Replica("ch-s1r1")}</shard>
              <shard>{Replica("ch-s1r1")}</shard>
            </pz_dup>
          </remote_servers>
          <zookeeper><node><host>keeper</host><port>9181</port></node></zookeeper>
          <macros><shard>{shard}</shard><replica>{replica}</replica></macros>
        </clickhouse>
        """;

    private const string KeeperXml = """
        <clickhouse>
          <listen_host>0.0.0.0</listen_host>
          <keeper_server>
            <tcp_port>9181</tcp_port>
            <server_id>1</server_id>
            <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
            <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
            <raft_configuration><server><id>1</id><hostname>keeper</hostname><port>9234</port></server></raft_configuration>
          </keeper_server>
        </clickhouse>
        """;
}
