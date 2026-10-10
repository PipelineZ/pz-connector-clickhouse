using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Testcontainers.ClickHouse;

namespace Pz.Connector.ClickHouse.Tests;

[CollectionDefinition("clickhouse")]
public sealed class ClickHouseCollection : ICollectionFixture<ClickHouseFixture>;

/// <summary>One single-node server per test run. Every fact works in its own database
/// (<see cref="CreateDatabaseAsync"/>) so facts never share state. Helpers talk to the server over
/// plain HTTP, deliberately not through the connector: a fact that used the code under test to seed
/// and verify would prove nothing.</summary>
public sealed class ClickHouseFixture : IAsyncLifetime
{
    public const string Image = "clickhouse/clickhouse-server:25.8";
    public const string User = "pz";
    public const string Password = "pz-fixture-secret";

    private ClickHouseContainer? _container;
    private HttpClient? _http;

    public string Url { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        // Built here rather than in a field initializer: Build() resolves and pings the docker
        // endpoint, so a constructor that built it would throw before the probe above could no-op.
        _container = new ClickHouseBuilder(Image).WithUsername(User).WithPassword(Password).Build();
        await _container.StartAsync().ConfigureAwait(false);
        Url = $"http://{_container.Hostname}:{_container.GetMappedPublicPort(8123)}";
        _http = new HttpClient { BaseAddress = new Uri(Url + "/") };
        _http.DefaultRequestHeaders.Add("X-ClickHouse-User", User);
        _http.DefaultRequestHeaders.Add("X-ClickHouse-Key", Password);
    }

    public async Task DisposeAsync()
    {
        _http?.Dispose();
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Dictionary<string, object?> ConnectionConfig(string database = "default") =>
        new() { ["url"] = Url, ["user"] = User, ["password"] = Password, ["database"] = database };

    public async Task<string> QueryAsync(string sql)
    {
        using var response = await _http!.PostAsync("/", new StringContent(sql, Encoding.UTF8)).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"fixture query failed ({(int)response.StatusCode}): {text}");
        }

        return text;
    }

    public async Task<string> CreateDatabaseAsync()
    {
        var name = "pz_" + Guid.NewGuid().ToString("N")[..12];
        await QueryAsync($"CREATE DATABASE {name}").ConfigureAwait(false);
        return name;
    }

    public async Task<IReadOnlyList<RecordBatch>> ReadArrowAsync(string sql)
    {
        var url = "/?output_format_arrow_compression_method=none&output_format_arrow_string_as_string=1";
        using var response = await _http!.PostAsync(url, new StringContent(sql + " FORMAT ArrowStream", Encoding.UTF8)).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        var bytes = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
        using var reader = new ArrowStreamReader(new MemoryStream(bytes));
        var batches = new List<RecordBatch>();
        while (await reader.ReadNextRecordBatchAsync().ConfigureAwait(false) is { } batch)
        {
            batches.Add(batch);
        }

        return batches;
    }
}
