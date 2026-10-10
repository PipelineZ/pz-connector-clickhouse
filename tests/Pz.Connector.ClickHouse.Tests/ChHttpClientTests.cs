using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChHttpClientTests
{
    private readonly FakeHandler _fake = new();

    private ChHttpClient Client(Dictionary<string, object?>? extra = null)
    {
        var values = new Dictionary<string, object?> { ["url"] = "http://ch:8123", ["user"] = "pz", ["password"] = "hunter22", ["database"] = "analytics" };
        foreach (var (k, v) in extra ?? []) values[k] = v;
        return new ChHttpClient(new HttpClient(_fake), ChConnector.ParseOrThrow(new ConnectorConfig(values)), NullLogger.Instance);
    }

    private static Dictionary<string, string> Query(Uri uri) =>
        uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Split('=', 2)).ToDictionary(p => Uri.UnescapeDataString(p[0]), p => Uri.UnescapeDataString(p[1]));

    [Fact]
    public async Task Credentials_go_in_headers_never_the_url()
    {
        await Client().ExecuteAsync("SELECT 1", "ctx", ChCodes.Read_QueryFailed, default);
        var (req, body) = _fake.Requests.Single();
        Assert.Equal("pz", req.Headers.GetValues("X-ClickHouse-User").Single());
        Assert.Equal("hunter22", req.Headers.GetValues("X-ClickHouse-Key").Single());
        Assert.DoesNotContain("hunter22", req.RequestUri!.ToString(), StringComparison.Ordinal);
        Assert.Equal("SELECT 1", body);
        Assert.Equal(HttpMethod.Post, req.Method);
    }

    [Fact]
    public async Task Url_carries_database_query_id_and_settings_with_request_overriding_connection()
    {
        var client = Client(new() { ["settings"] = new Dictionary<string, object?> { ["max_threads"] = 4.0, ["a"] = "conn" } });
        await client.ExecuteAsync("SELECT 1", "ctx", ChCodes.Read_QueryFailed, default, new Dictionary<string, string> { ["a"] = "req" });
        var q = Query(_fake.Requests.Single().Request.RequestUri!);
        Assert.Equal("analytics", q["database"]);
        Assert.StartsWith("pz-", q["query_id"], StringComparison.Ordinal);
        Assert.Equal("4", q["max_threads"]);
        Assert.Equal("req", q["a"]);
    }

    [Fact]
    public async Task Parameters_are_prefixed_and_insert_query_goes_in_the_url()
    {
        var client = Client();
        using var response = await client.SendAsync(new ChRequest("ctx", ChCodes.Write_CommitFailed)
        {
            QueryInUrl = "INSERT INTO `t` FORMAT ArrowStream",
            Body = new ByteArrayContent([1, 2, 3]),
            Parameters = new Dictionary<string, string> { ["pz_lower"] = "2024-01-01 00:00:00" },
        }, default);
        var q = Query(_fake.Requests.Single().Request.RequestUri!);
        Assert.Equal("INSERT INTO `t` FORMAT ArrowStream", q["query"]);
        Assert.Equal("2024-01-01 00:00:00", q["param_pz_lower"]);
    }

    [Fact]
    public async Task Server_error_is_classified_from_the_exception_code_header()
    {
        _fake.Respond = (_, _) => FakeHandler.Error(500, 252, "Too many parts (300)");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Client().ExecuteAsync("INSERT", "writing 'x'", ChCodes.Write_CommitFailed, default));
        Assert.True(ex.IsTransient);
        Assert.Contains("PZCH0402", ex.Message);
        Assert.DoesNotContain("INSERT", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Server_error_message_is_redacted()
    {
        _fake.Respond = (_, _) => FakeHandler.Error(500, 60, "table hunter22 missing");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Client().ExecuteAsync("x", "ctx", ChCodes.Read_QueryFailed, default));
        Assert.DoesNotContain("hunter22", ex.Message, StringComparison.Ordinal);
        Assert.Contains("PZCH0203", ex.Message);
    }

    [Fact]
    public async Task Transport_failure_is_transient()
    {
        _fake.Respond = (_, _) => throw new HttpRequestException("connection refused");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Client().ExecuteAsync("x", "ctx", ChCodes.Read_QueryFailed, default));
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public async Task Caller_cancellation_is_rethrown_unwrapped()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Client().ExecuteAsync("x", "ctx", ChCodes.Read_QueryFailed, cts.Token));
    }

    [Fact]
    public async Task QueryRows_reads_json_compact_strings()
    {
        _fake.Respond = (_, _) => FakeHandler.Json("""{"meta":[{"name":"a","type":"String"}],"data":[["x",null],["y","2"]],"rows":2}""");
        var rows = await Client().QueryRowsAsync("SELECT a, b FROM t", "ctx", ChCodes.Read_QueryFailed, default);
        Assert.Equal(2, rows.Count);
        Assert.Null(rows[0][1]);
        Assert.Equal("2", rows[1][1]);
        Assert.Equal("JSONCompactStrings", Query(_fake.Requests.Single().Request.RequestUri!)["default_format"]);
    }

    [Fact]
    public async Task Kill_query_never_throws_and_names_the_id()
    {
        _fake.Respond = (_, _) => throw new HttpRequestException("down");
        await Client().KillQueryAsync("pz-abc");
        Assert.Contains("query_id = 'pz-abc'", _fake.Requests.Single().Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Kill_query_goes_on_cluster_when_configured()
    {
        await Client(new() { ["cluster"] = "c1" }).KillQueryAsync("pz-abc");
        Assert.Contains("ON CLUSTER `c1`", _fake.Requests.Single().Body, StringComparison.Ordinal);
    }
}
