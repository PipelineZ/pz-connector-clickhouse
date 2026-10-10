namespace Pz.Connector.ClickHouse;

/// <summary>A table named the way ClickHouse names it: <c>table</c> (in the connection's database) or
/// <c>database.table</c>. A name is an object name, never a SQL fragment, so a backtick is refused
/// rather than escaped -- there is no legitimate ClickHouse object a user would name that way through
/// pz, and accepting one would make the quoted form ambiguous to a reader of the logs.</summary>
internal readonly record struct ChTableRef(string Database, string Table)
{
    public string Quoted => $"{ChSql.Quote(Database)}.{ChSql.Quote(Table)}";

    public string Display => $"{Database}.{Table}";

    public ChTableRef WithTable(string table) => this with { Table = table };

    public static bool TryParse(string entity, string defaultDatabase, out ChTableRef table, out string? error)
    {
        table = default;
        if (string.IsNullOrEmpty(entity) || entity.Contains('`'))
        {
            error = $"entity '{entity}' must be 'table' or 'database.table' and must not contain a backtick";
            return false;
        }

        var parts = entity.Split('.');
        if (parts.Length > 2 || parts.Any(p => p.Length == 0))
        {
            error = $"entity '{entity}' must be 'table' or 'database.table' (one dot at most, no empty part)";
            return false;
        }

        table = parts.Length == 2 ? new ChTableRef(parts[0], parts[1]) : new ChTableRef(defaultDatabase, parts[0]);
        error = null;
        return true;
    }
}

internal static class ChSql
{
    public static string Quote(string identifier) => $"`{identifier.Replace("`", "``", StringComparison.Ordinal)}`";

    public static string Literal(string value) =>
        $"'{value.Replace(@"\", @"\\", StringComparison.Ordinal).Replace("'", @"\'", StringComparison.Ordinal)}'";

    public static string Columns(IEnumerable<string> names) => string.Join(", ", names.Select(Quote));
}
