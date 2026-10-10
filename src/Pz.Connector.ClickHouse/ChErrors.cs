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

    public static string Summarize(string body)
    {
        var line = body.Split('\n', 2)[0].Trim();
        return line.Length <= 500 ? line : line[..500];
    }

    public static PzConnectorException FromServer(int httpStatus, int? exceptionCode, string body, TimeSpan? retryAfter,
        ChRedactor redactor, string context, string failCode)
    {
        var summary = Summarize(body);
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

        return new PzConnectorException(ChCodes.Message(ChCodes.Remote_Transient, redactor, $"{context}: {ex.Message}"),
            isTransient: true, innerException: ex);
    }

    private static PzConnectorException NonTransient(string code, ChRedactor redactor, string text) =>
        new(ChCodes.Message(code, redactor, text), isTransient: false);

    [GeneratedRegex(@"\bCode:\s*(?<code>\d+)\.")]
    private static partial Regex CodePattern();
}
