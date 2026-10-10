using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

internal sealed record ChRequest(string Context, string FailCode)
{
    public string? Sql { get; init; }
    public string? QueryInUrl { get; init; }
    public HttpContent? Body { get; init; }
    public IReadOnlyDictionary<string, string>? Settings { get; init; }
    public IReadOnlyDictionary<string, string>? Parameters { get; init; }
    public string QueryId { get; init; } = ChHttpClient.NewQueryId();
}

/// <summary>The only code that speaks ClickHouse's HTTP interface. Every request is a POST to the
/// origin with the database, query id, settings (connection first, then request) and typed
/// parameters in the URL; the statement is the body, except for an insert, whose statement goes in
/// <c>?query=</c> because the body is the data. Credentials go only in the
/// <c>X-ClickHouse-User</c>/<c>X-ClickHouse-Key</c> headers. A non-2xx answer is read (bounded),
/// classified by its exception code, and thrown; the statement text never enters a message.</summary>
internal sealed class ChHttpClient(HttpClient http, ChConnectionConfig cfg, ILogger logger) : IDisposable
{
    private const int MaxErrorBody = 64 * 1024;

    public ChConnectionConfig Config => cfg;

    public static string NewQueryId() => "pz-" + Guid.NewGuid().ToString("N");

    /// <summary>No overall timeout: a read or an insert is arbitrarily large, so the engine's
    /// cancellation token is the only cutoff. A connect timeout stays so a black-holed host fails.</summary>
    public static HttpClient Create(ChConnectionConfig cfg)
    {
        var handler = new SocketsHttpHandler
        {
            AutomaticDecompression = System.Net.DecompressionMethods.All,
            ConnectTimeout = TimeSpan.FromSeconds(30),
        };

        if (cfg.CaFile is { } caFile)
        {
            var roots = new X509Certificate2Collection();
            roots.ImportFromPemFile(caFile);
            handler.SslOptions.RemoteCertificateValidationCallback = (_, certificate, presented, errors) =>
            {
                if (certificate is null || (errors & (SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateNotAvailable)) != 0)
                {
                    return false;
                }

                // The server's own intermediates must be offered to the rebuilt chain: only the root is
                // in the CA file, so a leaf+intermediate answer builds only through the presented set.
                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.AddRange(roots);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                if (presented is not null)
                {
                    foreach (var element in presented.ChainElements)
                    {
                        chain.ChainPolicy.ExtraStore.Add(element.Certificate);
                    }
                }

                var loaded = certificate as X509Certificate2 is null ? X509CertificateLoader.LoadCertificate(certificate.GetRawCertData()) : null;
                try
                {
                    return chain.Build(loaded ?? (X509Certificate2)certificate);
                }
                finally
                {
                    loaded?.Dispose();
                }
            };
        }

        return new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
    }

    public async Task<HttpResponseMessage> SendAsync(ChRequest request, CancellationToken ct)
    {
        using var message = new HttpRequestMessage(HttpMethod.Post, BuildUri(request));
        message.Headers.Add("X-ClickHouse-User", cfg.User);
        if (cfg.Password is { } password)
        {
            message.Headers.Add("X-ClickHouse-Key", password);
        }

        message.Content = request.Body ?? new StringContent(request.Sql ?? "", Encoding.UTF8, "text/plain");

        HttpResponseMessage response;
        try
        {
            response = await http.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or OperationCanceledException)
        {
            throw ChErrors.Wrap(ex, cfg.Redactor, $"{request.Context} (query_id {request.QueryId})");
        }

        if (response.IsSuccessStatusCode)
        {
            return response;
        }

        using (response)
        {
            var body = await ReadBoundedAsync(response, ct).ConfigureAwait(false);
            var code = response.Headers.TryGetValues("X-ClickHouse-Exception-Code", out var values)
                && int.TryParse(values.FirstOrDefault(), out var headerCode)
                ? headerCode
                : ChErrors.ParseExceptionCode(body);
            throw ChErrors.FromServer((int)response.StatusCode, code, body, response.Headers.RetryAfter?.Delta, cfg.Redactor,
                $"{request.Context} (query_id {request.QueryId})", request.FailCode);
        }
    }

    public async Task ExecuteAsync(string sql, string context, string failCode, CancellationToken ct,
        IReadOnlyDictionary<string, string>? settings = null)
    {
        using var response = await SendAsync(new ChRequest(context, failCode) { Sql = sql, Settings = settings }, ct).ConfigureAwait(false);
        // Drain so the server-side query is complete (and its errors surfaced) before returning.
        await response.Content.CopyToAsync(Stream.Null, ct).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<IReadOnlyList<string?>>> QueryRowsAsync(string sql, string context, string failCode,
        CancellationToken ct, IReadOnlyDictionary<string, string>? parameters = null)
    {
        var settings = new Dictionary<string, string> { ["default_format"] = "JSONCompactStrings" };
        using var response = await SendAsync(new ChRequest(context, failCode) { Sql = sql, Settings = settings, Parameters = parameters }, ct)
            .ConfigureAwait(false);
        await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        var result = await JsonSerializer.DeserializeAsync(stream, ChJsonContext.Default.ChCompactStrings, ct).ConfigureAwait(false);
        return result?.Data ?? [];
    }

    /// <summary>Best effort: closing the response already aborts a running query on the server; this
    /// is the belt to that brace for a query the server keeps running. Never throws.</summary>
    public async Task KillQueryAsync(string queryId)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var onCluster = cfg.Cluster is { } c ? $" ON CLUSTER {ChSql.Quote(c)}" : "";
        try
        {
            await ExecuteAsync($"KILL QUERY{onCluster} WHERE query_id = {ChSql.Literal(queryId)} ASYNC", "killing a cancelled query",
                ChCodes.Read_QueryFailed, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "clickhouse: KILL QUERY for {QueryId} failed (ignored)", queryId);
        }
    }

    public void Dispose() => http.Dispose();

    private Uri BuildUri(ChRequest request)
    {
        var pairs = new List<KeyValuePair<string, string>>
        {
            new("database", cfg.Database),
            new("query_id", request.QueryId),
        };

        var settings = new Dictionary<string, string>(cfg.Settings, StringComparer.Ordinal);
        foreach (var (k, v) in request.Settings ?? new Dictionary<string, string>())
        {
            settings[k] = v;
        }

        pairs.AddRange(settings);
        pairs.AddRange((request.Parameters ?? new Dictionary<string, string>()).Select(p => new KeyValuePair<string, string>("param_" + p.Key, p.Value)));
        if (request.QueryInUrl is { } query)
        {
            pairs.Add(new("query", query));
        }

        var qs = string.Join("&", pairs.Select(p => $"{Uri.EscapeDataString(p.Key)}={Uri.EscapeDataString(p.Value)}"));
        return new Uri(cfg.Url, "?" + qs);
    }

    private static async Task<string> ReadBoundedAsync(HttpResponseMessage response, CancellationToken ct)
    {
        await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        var buffer = new byte[MaxErrorBody];
        var total = 0;
        int read;
        while (total < buffer.Length && (read = await stream.ReadAsync(buffer.AsMemory(total), ct).ConfigureAwait(false)) > 0)
        {
            total += read;
        }

        return Encoding.UTF8.GetString(buffer, 0, total);
    }
}
