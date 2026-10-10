using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>The typed connection surface. <see cref="Url"/> always ends with <c>/</c> and carries no
/// user info, path, query or fragment. Every error names a key or a rule, never the value that failed:
/// a rejected value here can be a credential, and the redactor that would mask it is built from the
/// very config being parsed -- so errors are coded through <see cref="ChRedactor.None"/>.</summary>
internal sealed record ChConnectionConfig(
    Uri Url,
    string User,
    string? Password,
    string Database,
    string? Cluster,
    string? CaFile,
    IReadOnlyDictionary<string, string> Settings,
    ChRedactor Redactor)
{
    // base_dir is never written by a user: pz supplies it because the package declares
    // PzProjectDirectoryAnchor, so a relative tls_ca_file resolves against the project directory.
    private static readonly string[] KnownKeys = ["url", "user", "password", "database", "cluster", "tls_ca_file", "settings", "base_dir"];

    public static ChConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add(Coded(ChCodes.Config_Invalid, $"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys.Where(k => k != "base_dir"))}"));
        }

        var url = ParseUrl(config.GetString("url"), errors);
        var user = NonEmpty(config, "user", errors) ?? "default";
        var password = config.GetString("password") is { Length: > 0 } p ? p : null;
        var database = NonEmpty(config, "database", errors) ?? "default";
        var cluster = NonEmpty(config, "cluster", errors);
        var caFile = ParseCaFile(config, url, errors);
        var settings = ParseSettings(config.Values.GetValueOrDefault("settings"), "settings", ChCodes.Config_Invalid, ChRedactor.None, errors);

        if (errors.Count != start || url is null)
        {
            return null;
        }

        return new ChConnectionConfig(url, user, password, database, cluster, caFile, settings,
            new ChRedactor(password is null ? [] : [password]));
    }

    /// <summary>A <c>settings:</c> map (connection, dataset or output level). Values are scalars
    /// rendered as ClickHouse reads them in a URL parameter. A number reaches an out-of-process
    /// connector as a double (protobuf Struct has no integer), so an integral double renders without
    /// a decimal point.</summary>
    public static IReadOnlyDictionary<string, string> ParseSettings(object? raw, string label, string code, ChRedactor redactor, List<string> errors)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (raw is null)
        {
            return result;
        }

        if (raw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add(ChCodes.Message(code, redactor, $"'{label}' must be a map of setting name to scalar value"));
            return result;
        }

        foreach (var (name, value) in map)
        {
            if (RenderSetting(value) is { } text)
            {
                result[name] = text;
            }
            else
            {
                errors.Add(ChCodes.Message(code, redactor, $"'{label}.{name}' must be a string, number or boolean"));
            }
        }

        return result;
    }

    public static string? RenderSetting(object? value) => value switch
    {
        string s => s,
        bool b => b ? "1" : "0",
        double d when d == Math.Floor(d) && Math.Abs(d) < 9.007199254740992E15 => ((long)d).ToString(CultureInfo.InvariantCulture),
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        long l => l.ToString(CultureInfo.InvariantCulture),
        int i => i.ToString(CultureInfo.InvariantCulture),
        _ => null,
    };

    private static string Coded(string code, string text) => ChCodes.Message(code, ChRedactor.None, text);

    private static string? NonEmpty(ConnectorConfig config, string key, List<string> errors)
    {
        if (!config.Values.ContainsKey(key))
        {
            return null;
        }

        var value = config.GetString(key);
        if (string.IsNullOrEmpty(value))
        {
            errors.Add(Coded(ChCodes.Config_Invalid, $"'{key}' must not be empty"));
            return null;
        }

        return value;
    }

    private static Uri? ParseUrl(string? text, List<string> errors)
    {
        const string Hint = "the HTTP interface origin, e.g. http://localhost:8123 or https://abc.clickhouse.cloud:8443";
        if (string.IsNullOrEmpty(text))
        {
            errors.Add(Coded(ChCodes.Config_UrlInvalid, $"'url' is required ({Hint})"));
            return null;
        }

        if (!Uri.TryCreate(text, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
        {
            errors.Add(Coded(ChCodes.Config_UrlInvalid, $"'url' must be an http or https URL ({Hint})"));
            return null;
        }

        // User info would be a credential outside 'user'/'password': it would never reach a request
        // (credentials go in headers) and GetLeftPart would drop it silently.
        if (!string.IsNullOrEmpty(uri.UserInfo) || uri.AbsolutePath is not ("/" or "")
            || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment))
        {
            errors.Add(Coded(ChCodes.Config_UrlInvalid, "'url' must be the origin only -- no user info, path, query or fragment; put credentials in 'user'/'password'"));
            return null;
        }

        return new Uri(uri.GetLeftPart(UriPartial.Authority) + "/");
    }

    private static string? ParseCaFile(ConnectorConfig config, Uri? url, List<string> errors)
    {
        if (config.GetString("tls_ca_file") is not { Length: > 0 } path)
        {
            return null;
        }

        if (url is not null && url.Scheme != "https")
        {
            errors.Add(Coded(ChCodes.Config_Invalid, "'tls_ca_file' only applies to an https 'url'"));
            return null;
        }

        if (!Path.IsPathRooted(path) && config.GetString("base_dir") is { Length: > 0 } baseDir)
        {
            path = Path.Combine(baseDir, path);
        }

        if (!File.Exists(path))
        {
            errors.Add(Coded(ChCodes.Config_CaFileUnreadable, "'tls_ca_file' does not exist (a relative path resolves against the project directory)"));
            return null;
        }

        return Path.GetFullPath(path);
    }
}
