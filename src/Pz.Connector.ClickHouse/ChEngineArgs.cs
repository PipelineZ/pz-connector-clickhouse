namespace Pz.Connector.ClickHouse;

/// <summary>Reads the argument list of a server-generated engine clause, e.g.
/// <c>Distributed('c', 'db', 'local', cityHash64(id))</c> from <c>system.tables.engine_full</c>.
/// ClickHouse exposes no column for a Distributed table's sharding key, so the clause the server
/// itself printed is the only source. This splits top-level arguments only; quotes and brackets are
/// honoured so a comma inside a key expression does not split it. It never reads user SQL.</summary>
internal static class ChEngineArgs
{
    public static IReadOnlyList<string>? Split(string engineFull, string engineName)
    {
        var prefix = engineName + "(";
        if (!engineFull.StartsWith(prefix, StringComparison.Ordinal))
        {
            return null;
        }

        var args = new List<string>();
        var depth = 0;
        var start = prefix.Length;
        for (var i = prefix.Length; i < engineFull.Length; i++)
        {
            var c = engineFull[i];
            if (c is '\'' or '`' or '"')
            {
                i = SkipQuoted(engineFull, i);
                continue;
            }

            if (c is '(' or '[')
            {
                depth++;
            }
            else if (c is ')' or ']')
            {
                if (depth == 0)
                {
                    var last = engineFull[start..i].Trim();
                    if (last.Length > 0)
                    {
                        args.Add(last);
                    }

                    return args;
                }

                depth--;
            }
            else if (c == ',' && depth == 0)
            {
                args.Add(engineFull[start..i].Trim());
                start = i + 1;
            }
        }

        return null;
    }

    public static string Unquote(string arg)
    {
        arg = arg.Trim();
        if (arg.Length >= 2 && arg[0] == '\'' && arg[^1] == '\'')
        {
            return arg[1..^1].Replace(@"\'", "'", StringComparison.Ordinal).Replace(@"\\", @"\", StringComparison.Ordinal);
        }

        if (arg.Length >= 2 && arg[0] == '`' && arg[^1] == '`')
        {
            return arg[1..^1].Replace("``", "`", StringComparison.Ordinal);
        }

        return arg;
    }

    /// <summary>The same clause with its first argument replaced -- everything after that argument
    /// (the remaining arguments, ORDER BY, SETTINGS) is kept byte for byte.</summary>
    public static string ReplaceFirstArgument(string engineFull, string engineName, string replacement)
    {
        var start = engineName.Length + 1;
        var depth = 0;
        for (var i = start; i < engineFull.Length; i++)
        {
            var c = engineFull[i];
            if (c is '\'' or '`' or '"')
            {
                i = SkipQuoted(engineFull, i);
                continue;
            }

            if (c is '(' or '[')
            {
                depth++;
            }
            else if ((c is ',' && depth == 0) || (c is ')' or ']' && depth-- == 0))
            {
                return engineFull[..start] + replacement + engineFull[i..];
            }
        }

        return engineFull;
    }

    private static int SkipQuoted(string s, int i)
    {
        var quote = s[i];
        for (i++; i < s.Length; i++)
        {
            if (s[i] == '\\')
            {
                i++;
                continue;
            }

            if (s[i] == quote)
            {
                return i;
            }
        }

        return s.Length;
    }
}
