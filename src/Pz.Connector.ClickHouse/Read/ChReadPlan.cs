using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

internal sealed record ChColumn(string Name, ChType Type, ReadTreatment Treatment);

/// <summary>The statement a read runs and the parameters it binds. Table mode pushes the engine's
/// columns, predicate and limit, and turns watermark bounds into typed query parameters
/// (<c>{pz_lower:T}</c>, T the cursor column's own type) -- never spliced literals. Query mode runs
/// the user's SQL verbatim with no pushdown, wrapped only when a column needs a cast. Every WHERE term
/// is self-parenthesized so a disjunctive predicate cannot bind into the watermark's AND.</summary>
internal sealed record ChReadPlan(string Sql, IReadOnlyDictionary<string, string> Parameters)
{
    public const string LowerParameter = "pz_lower";
    public const string UpperParameter = "pz_upper";

    public static string DescribeSql(ChReadConfig config) => $"DESCRIBE ({Source(config)})";

    public static IReadOnlyList<ChColumn> Columns(IReadOnlyList<IReadOnlyList<string?>> describeRows, ChRedactor redactor, string context)
    {
        var columns = new List<ChColumn>();
        foreach (var row in describeRows)
        {
            var name = row[0]!;
            var typeText = row[1]!;
            ChType type;
            try
            {
                type = ChTypes.Parse(typeText);
            }
            catch (FormatException)
            {
                throw Unsupported(redactor, context, name, typeText);
            }

            var treatment = ChTypes.ForRead(type);
            if (treatment.Kind == ReadKind.Refuse)
            {
                throw Unsupported(redactor, context, name, typeText);
            }

            columns.Add(new ChColumn(name, type, treatment));
        }

        return columns;
    }

    public static ChReadPlan Build(ChReadConfig config, DatasetSpec spec, ReadHints hints, IReadOnlyList<ChColumn> columns,
        bool pushPredicate, bool limitZero, ChRedactor redactor)
    {
        var parameters = new Dictionary<string, string>(StringComparer.Ordinal);
        var anyCast = columns.Any(c => c.Treatment.Kind == ReadKind.Cast);

        if (config.Query is { } query)
        {
            // Watermark and window bounds apply here too: they are correctness, not optimization.
            // Engine hints (columns, predicate, limit) are never pushed into a user's query.
            var bounds = WatermarkTerms(config, spec, columns, redactor, parameters);
            if (bounds.Count == 0 && !limitZero && !anyCast)
            {
                return new ChReadPlan(query, parameters);
            }

            var inner = Inner(query);
            var queryWhere = bounds.Count == 0 ? "" : " WHERE " + string.Join(" AND ", bounds);
            return new ChReadPlan(
                $"SELECT {(anyCast ? Projection(columns) : "*")} FROM ({inner}) AS pz_query{queryWhere}{(limitZero ? " LIMIT 0" : "")}", parameters);
        }

        var projected = Pruned(hints, columns) ?? (anyCast ? columns : null);
        var terms = new List<string>();
        if (pushPredicate && !string.IsNullOrEmpty(hints.PredicateSql))
        {
            terms.Add($"({hints.PredicateSql})");
        }

        terms.AddRange(WatermarkTerms(config, spec, columns, redactor, parameters));

        var select = projected is null ? "*" : Projection(projected);
        var where = terms.Count == 0 ? "" : " WHERE " + string.Join(" AND ", terms);
        var limit = limitZero ? " LIMIT 0" : hints.Limit is { } n ? $" LIMIT {n.ToString(CultureInfo.InvariantCulture)}" : "";
        return new ChReadPlan($"SELECT {select} FROM {config.Table!.Value.Quoted}{where}{limit}", parameters);
    }

    /// <summary>Settings every read sends. The wire format is fixed here, not by a <c>FORMAT</c>
    /// clause, so a user's query stays verbatim. <c>prefer_column_name_to_alias</c> makes WHERE terms
    /// see the stored column, not the same-named cast in the projection. Block size follows the
    /// engine's batch target at a 64-byte-per-row floor, capped by its row limit.</summary>
    public static Dictionary<string, string> Settings(ChReadConfig config, BatchOptions? options)
    {
        var s = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["default_format"] = "ArrowStream",
            ["output_format_arrow_compression_method"] = "none",
            ["output_format_arrow_string_as_string"] = "1",
            ["output_format_arrow_low_cardinality_as_dictionary"] = "0",
            ["date_time_input_format"] = "best_effort",
            ["prefer_column_name_to_alias"] = "1",
            ["enable_http_compression"] = "1",
        };

        if (options is not null)
        {
            var rows = Math.Clamp(options.TargetBatchBytes / 64, 1, Math.Max(1, options.MaxRowsPerBatch));
            s["max_block_size"] = rows.ToString(CultureInfo.InvariantCulture);
            s["preferred_block_size_bytes"] = options.TargetBatchBytes.ToString(CultureInfo.InvariantCulture);
        }

        foreach (var (k, v) in config.Settings)
        {
            s[k] = v;
        }

        return s;
    }

    private static List<string> WatermarkTerms(ChReadConfig config, DatasetSpec spec, IReadOnlyList<ChColumn> columns,
        ChRedactor redactor, Dictionary<string, string> parameters)
    {
        var terms = new List<string>();
        if (spec.WatermarkCursor is not { } cursor || (spec.WatermarkValue is null && spec.WatermarkUpperBound is null))
        {
            return terms;
        }

        var column = columns.FirstOrDefault(c => c.Name == cursor)
            ?? columns.FirstOrDefault(c => string.Equals(c.Name, cursor, StringComparison.OrdinalIgnoreCase))
            ?? throw Cursor(redactor, $"watermark cursor column '{cursor}' is not in {config.Label}");
        var type = ChTypes.CursorParameterType(column.Type)
            ?? throw Cursor(redactor, $"watermark cursor column '{cursor}' has type {column.Type.Render()}; a cursor must be an integer, Decimal, Date, Date32, DateTime, DateTime64 or String column");

        if (spec.WatermarkValue is { } lower)
        {
            terms.Add($"({ChSql.Quote(column.Name)} {(spec.WatermarkLowerInclusive ? ">=" : ">")} {{{LowerParameter}:{type}}})");
            parameters[LowerParameter] = lower;
        }

        if (spec.WatermarkUpperBound is { } upper)
        {
            terms.Add($"({ChSql.Quote(column.Name)} <= {{{UpperParameter}:{type}}})");
            parameters[UpperParameter] = upper;
        }

        return terms;
    }

    private static string Source(ChReadConfig config) =>
        config.Query is { } q ? Inner(q) : $"SELECT * FROM {config.Table!.Value.Quoted}";

    /// <summary>A user's query as it can appear inside a derived table: a trailing terminator is
    /// legal on its own but a parse error once parenthesized.</summary>
    private static string Inner(string query) => query.Trim().TrimEnd(';').TrimEnd();

    private static IReadOnlyList<ChColumn>? Pruned(ReadHints hints, IReadOnlyList<ChColumn> columns)
    {
        if (hints.Columns is not { Count: > 0 } names)
        {
            return null;
        }

        var picked = new List<ChColumn>();
        foreach (var name in names)
        {
            var match = columns.FirstOrDefault(c => c.Name == name) ?? columns.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
            if (match is null)
            {
                // A hint is best-effort; a name this table does not have means the hint is not about
                // it, and reading everything is always correct.
                return null;
            }

            picked.Add(match);
        }

        return picked;
    }

    private static string Projection(IEnumerable<ChColumn> columns) => string.Join(", ", columns.Select(c =>
        c.Treatment.Kind == ReadKind.Cast
            ? $"{string.Format(CultureInfo.InvariantCulture, c.Treatment.CastTemplate!, ChSql.Quote(c.Name))} AS {ChSql.Quote(c.Name)}"
            : ChSql.Quote(c.Name)));

    private static PzConnectorException Unsupported(ChRedactor redactor, string context, string column, string type) =>
        new(ChCodes.Message(ChCodes.Read_UnsupportedType, redactor,
            $"{context}: column '{column}' has type {type}, which cannot be read; cast it in a 'query:' read (e.g. toString(...))"), isTransient: false);

    private static PzConnectorException Cursor(ChRedactor redactor, string text) =>
        new(ChCodes.Message(ChCodes.Read_UnsupportedCursorType, redactor, text), isTransient: false);
}
