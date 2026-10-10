using System.Text.RegularExpressions;

namespace Pz.Connector.ClickHouse;

/// <summary>Strips credentials from any text that may reach a <c>PzConnectorException</c> message.
/// Registered secrets (the password) are replaced longest first. Beyond them, the shapes an HTTP
/// stack or ClickHouse itself may echo are rewritten even when the value is not one of ours: the
/// <c>X-ClickHouse-Key</c> and <c>Authorization</c> headers, a <c>password=</c> pair, and URL user info.
/// Secrets shorter than 3 characters are not matched; replacing them would shred unrelated text.</summary>
internal sealed partial class ChRedactor
{
    public const string Mask = "***";

    public static readonly ChRedactor None = new([]);

    private readonly string[] _secrets;

    public ChRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = secrets.Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal).OrderByDescending(s => s.Length).ToArray();
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
        }

        text = KeyHeader().Replace(text, m => $"{m.Groups["key"].Value} {Mask}");
        text = AuthorizationHeader().Replace(text, m => $"{m.Groups["key"].Value} {Mask}");
        text = PasswordPair().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
        return UrlUserInfo().Replace(text, m => $"{m.Groups["scheme"].Value}{m.Groups["user"].Value}:{Mask}@");
    }

    [GeneratedRegex("""(?<key>\bX-ClickHouse-Key:)\s*\S+""", RegexOptions.IgnoreCase)]
    private static partial Regex KeyHeader();

    [GeneratedRegex("""(?<key>\bAuthorization:)\s+(?:Bearer|Basic)\s+\S+""", RegexOptions.IgnoreCase)]
    private static partial Regex AuthorizationHeader();

    [GeneratedRegex("""(?<key>\b(?:password|key)\b)=(?:"[^"]*"|[^\s;,&]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex PasswordPair();

    [GeneratedRegex("""(?<scheme>https?://)(?<user>[^:/@\s]+):[^@/\s]+@""", RegexOptions.IgnoreCase)]
    private static partial Regex UrlUserInfo();
}
