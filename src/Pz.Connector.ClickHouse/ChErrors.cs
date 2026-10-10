using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>Turns a ClickHouse failure into the engine's exception, classified for retry. ClickHouse
/// answers HTTP 500 for nearly every error, so the exception code (the
/// <c>X-ClickHouse-Exception-Code</c> header, or <c>Code: N.</c> in the body) decides; the HTTP status
/// only decides when no code is present (a proxy or load balancer answered). Messages always pass the
/// redactor.</summary>
internal static partial class ChErrors
{
    /// <summary>Server conditions that clear on their own: timeouts, network faults between nodes, a
    /// replica gone read-only while Keeper is unreachable, too many parts awaiting merges, too many
    /// concurrent queries, and Keeper exceptions.</summary>
    public static bool IsTransientCode(int code) => code is 159 or 209 or 210 or 242 or 252 or 202 or 999;

    private static bool IsAuthCode(int code) => code is 516 or 192 or 194;

    public static int? ParseExceptionCode(string text)
    {
        var m = CodePattern().Match(text);
        return m.Success && int.TryParse(m.Groups["code"].Value, out var code) ? code : null;
    }

    /// <summary>The first line of the body, redacted and then capped: redacting the full line first means
    /// a secret that straddles the cap cannot leave its prefix behind.</summary>
    public static string Summarize(string body, ChRedactor redactor)
    {
        var line = redactor.Redact(body.Split('\n', 2)[0].Trim());
        return line.Length <= 500 ? line : line[..500];
    }

    public static PzConnectorException FromServer(int httpStatus, int? exceptionCode, string body, TimeSpan? retryAfter,
        ChRedactor redactor, string context, string failCode)
    {
        var summary = Summarize(body, redactor);
        var text = exceptionCode is { } c ? $"{context}: ClickHouse error {c}: {summary}" : $"{context}: HTTP {httpStatus}: {summary}";

        if (exceptionCode is { } auth && IsAuthCode(auth) || exceptionCode is null && httpStatus is 401 or 403)
        {
            return NonTransient(ChCodes.Remote_Unauthorized, redactor, $"{text} -- check 'user'/'password' and the user's grants");
        }

        if (exceptionCode is { } code ? IsTransientCode(code) : httpStatus is 429 or 502 or 503 or 504)
        {
            return new PzConnectorException(ChCodes.Message(ChCodes.Remote_Transient, redactor, text), isTransient: true, retryAfter: retryAfter);
        }

        return NonTransient(failCode, redactor, text);
    }

    /// <summary>A client-side failure before any response arrived. <paramref name="ex"/> must not be an
    /// <see cref="OperationCanceledException"/>: the engine's own cancellation is the caller's to
    /// rethrow unwrapped, so handing one here is a caller bug and throws loudly.</summary>
    public static PzConnectorException Wrap(Exception ex, ChRedactor redactor, string context)
    {
        if (ex is OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"{nameof(ChErrors)}.{nameof(Wrap)} must not be called with {nameof(OperationCanceledException)} -- the caller rethrows cancellation unwrapped.", ex);
        }

        // A certificate the client does not trust will not become trusted by retrying. A handshake that
        // died on a reset connection also surfaces as an AuthenticationException, but with an
        // IOException or SocketException at or below it; that is a network fault and stays transient.
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e is System.Security.Authentication.AuthenticationException && !HasTransportFault(e))
            {
                return new PzConnectorException(ChCodes.Message(ChCodes.Remote_Unauthorized, redactor,
                    $"{context}: TLS handshake failed ({e.Message}) -- for a private CA set 'tls_ca_file' on the connection"),
                    isTransient: false, innerException: ex);
            }
        }

        return new PzConnectorException(ChCodes.Message(ChCodes.Remote_Transient, redactor, $"{context}: {ex.Message}"),
            isTransient: true, innerException: ex);
    }

    private static bool HasTransportFault(Exception from)
    {
        for (var e = from; e is not null; e = e.InnerException)
        {
            if (e is IOException or System.Net.Sockets.SocketException)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>A client-side timeout or abort that the engine did not ask for (an internal timeout, a
    /// handler tearing the request down): nothing is wrong with the request, so a retry may succeed.</summary>
    public static PzConnectorException Aborted(ChRedactor redactor, string context) =>
        new(ChCodes.Message(ChCodes.Remote_Transient, redactor, $"{context}: the request was aborted before an answer arrived"), isTransient: true);

    private static PzConnectorException NonTransient(string code, ChRedactor redactor, string text) =>
        new(ChCodes.Message(code, redactor, text), isTransient: false);

    [GeneratedRegex(@"\bCode:\s*(?<code>\d+)\.")]
    private static partial Regex CodePattern();
}
