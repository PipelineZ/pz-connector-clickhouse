using System.Net;

namespace Pz.Connector.ClickHouse.Tests;

/// <summary>Stands in for the network in unit tests: records every request (with its body read as
/// text) and answers through <see cref="Respond"/>.</summary>
internal sealed class FakeHandler : HttpMessageHandler
{
    public List<(HttpRequestMessage Request, string? Body)> Requests { get; } = [];

    public Func<HttpRequestMessage, string?, HttpResponseMessage> Respond { get; set; } = (_, _) => new HttpResponseMessage(HttpStatusCode.OK);

    public static HttpResponseMessage Error(int status, int code, string text)
    {
        var r = new HttpResponseMessage((HttpStatusCode)status) { Content = new StringContent($"Code: {code}. DB::Exception: {text}") };
        r.Headers.Add("X-ClickHouse-Exception-Code", code.ToString(System.Globalization.CultureInfo.InvariantCulture));
        return r;
    }

    public static HttpResponseMessage Json(string json) => new(HttpStatusCode.OK) { Content = new StringContent(json) };

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var body = request.Content is null ? null : await request.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        Requests.Add((request, body));
        return Respond(request, body);
    }
}
