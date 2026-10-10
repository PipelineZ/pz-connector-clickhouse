using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.ClickHouse;

/// <summary>One ClickHouse data type as <c>DESCRIBE</c>/<c>system.columns</c> print it, parsed into a
/// tree. A parameter is either a nested type (optionally named, inside <c>Tuple</c>) or a literal
/// (<c>3</c>, <c>'UTC'</c>, <c>'a' = 1</c>). Parameters the grammar below does not model (the
/// <c>JSON(...)</c> hint list, an aggregate function call) are kept verbatim as literals: the
/// connector only ever needs to recognise such a type by name.</summary>
internal sealed record ChType(string Name, IReadOnlyList<ChTypeParam> Params, string? FieldName = null)
{
    public ChType Unwrapped => Name is "Nullable" or "LowCardinality" && TypeArgs.FirstOrDefault() is { } inner ? inner.Unwrapped : this;

    public IEnumerable<ChType> TypeArgs => Params.Where(p => p.Type is not null).Select(p => p.Type!);

    public string Render() => Params.Count == 0
        ? Name
        : $"{Name}({string.Join(", ", Params.Select(p => p.Type is { } t ? (t.FieldName is { } f ? $"{ChSql.Quote(f)} {t.Render()}" : t.Render()) : p.Literal))})";
}

internal sealed record ChTypeParam(ChType? Type, string? Literal);

internal enum ReadKind { AsIs, Cast, Refuse }

/// <summary>How a column is projected on read. <see cref="CastTemplate"/> takes the quoted column
/// as <c>{0}</c>; the cast keeps the column's name (<c>AS `c`</c>) at the call site.</summary>
internal readonly record struct ReadTreatment(ReadKind Kind, string? CastTemplate)
{
    public static readonly ReadTreatment AsIs = new(ReadKind.AsIs, null);
    public static readonly ReadTreatment Refuse = new(ReadKind.Refuse, null);
    public static ReadTreatment Cast(string template) => new(ReadKind.Cast, template);
}

internal static partial class ChTypes
{
    // Arrow output of these is unusable as-is (raw day/second counts) or absent (no Arrow type wide
    // enough, no Arrow equivalent): the server stringifies or widens them before they leave.
    private static readonly HashSet<string> Stringified = new(StringComparer.Ordinal)
    {
        "Enum8", "Enum16", "Enum", "UUID", "IPv4", "IPv6", "JSON", "Object", "Dynamic", "Variant",
        "Int128", "UInt128", "Int256", "UInt256",
    };

    private static readonly HashSet<string> Integers = new(StringComparer.Ordinal)
    {
        "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
    };

    /// <summary>The ClickHouse column type a created table declares for one Arrow field. A container
    /// (Array/Tuple/Map) is never wrapped in Nullable -- ClickHouse forbids it -- so a null list, map or
    /// struct lands as an empty/default value (documented in the README); nullability inside a
    /// container is kept. Time, interval, duration, dictionary, union and null types are refused:
    /// ClickHouse has no stable equivalent (Time64 is experimental) and a silent widening would
    /// change values.</summary>
    public static string? ToDdl(Field field, bool forceNotNull, out string? error)
    {
        var inner = Inner(field.DataType, out error);
        if (inner is null)
        {
            error = $"column '{field.Name}': {error}";
            return null;
        }

        var isContainer = field.DataType is ListType or LargeListType or StructType or MapType;
        return field.IsNullable && !forceNotNull && !isContainer ? $"Nullable({inner})" : inner;
    }

    private static string? Inner(IArrowType type, out string? error)
    {
        error = null;
        switch (type)
        {
            case BooleanType: return "Bool";
            case Int8Type: return "Int8";
            case Int16Type: return "Int16";
            case Int32Type: return "Int32";
            case Int64Type: return "Int64";
            case UInt8Type: return "UInt8";
            case UInt16Type: return "UInt16";
            case UInt32Type: return "UInt32";
            case UInt64Type: return "UInt64";
            case HalfFloatType or FloatType: return "Float32";
            case DoubleType: return "Float64";
            case Decimal128Type d: return $"Decimal({d.Precision}, {d.Scale})";
            case Decimal256Type d when d.Precision <= 76: return $"Decimal({d.Precision}, {d.Scale})";
            case StringType or LargeStringType or BinaryType or LargeBinaryType: return "String";
            case FixedSizeBinaryType f: return $"FixedString({f.ByteWidth})";
            case Date32Type or Date64Type: return "Date32";
            case TimestampType ts:
            {
                var precision = ts.Unit switch { TimeUnit.Second => 0, TimeUnit.Millisecond => 3, TimeUnit.Microsecond => 6, _ => 9 };
                return string.IsNullOrEmpty(ts.Timezone) ? $"DateTime64({precision})" : $"DateTime64({precision}, {ChSql.Literal(ts.Timezone)})";
            }
            case ListType l:
                return Child(l.ValueField, out error) is { } item ? $"Array({item})" : null;
            case LargeListType l:
                return Child(l.ValueField, out error) is { } largeItem ? $"Array({largeItem})" : null;
            case StructType s:
            {
                var parts = new List<string>();
                foreach (var f in s.Fields)
                {
                    if (Child(f, out error) is not { } child)
                    {
                        return null;
                    }

                    parts.Add($"{ChSql.Quote(f.Name)} {child}");
                }

                return $"Tuple({string.Join(", ", parts)})";
            }
            case MapType m:
            {
                // Map keys are never null in Arrow and ClickHouse forbids Nullable keys.
                var key = Inner(m.KeyField.DataType, out error);
                var value = key is null ? null : Child(m.ValueField, out error);
                return key is null || value is null ? null : $"Map({key}, {value})";
            }
            case DictionaryType:
                error = "dictionary-encoded columns are not supported; decode the column (cast it to its value type) before writing";
                return null;
            default:
                error = $"Arrow type '{type.Name}' has no ClickHouse column mapping; cast it in the pipeline (e.g. to varchar or timestamp)";
                return null;
        }
    }

    private static string? Child(Field f, out string? error)
    {
        var inner = Inner(f.DataType, out error);
        if (inner is null)
        {
            return null;
        }

        var isContainer = f.DataType is ListType or LargeListType or StructType or MapType;
        return f.IsNullable && !isContainer ? $"Nullable({inner})" : inner;
    }

    public static ChType Parse(string text) => new Parser(text).ParseTop();

    public static ReadTreatment ForRead(ChType type)
    {
        var t = type.Unwrapped;
        if (IsRefused(t.Name))
        {
            return ReadTreatment.Refuse;
        }

        if (t.Name is "Date" or "Date32")
        {
            return ReadTreatment.Cast("toDate32({0})");
        }

        if (t.Name == "DateTime")
        {
            return t.Params.FirstOrDefault()?.Literal is { } tz
                ? ReadTreatment.Cast($"toDateTime64({{0}}, 0, {tz})")
                : ReadTreatment.Cast("toDateTime64({0}, 0)");
        }

        if (Stringified.Contains(t.Name))
        {
            return ReadTreatment.Cast("toString({0})");
        }

        if (t.Name is "Array" or "Map" or "Tuple" or "Nested")
        {
            var inner = t.TypeArgs.Select(ForRead).ToList();
            if (inner.Any(i => i.Kind == ReadKind.Refuse))
            {
                return ReadTreatment.Refuse;
            }

            // A cast cannot reach inside a container without rebuilding it element by element, so a
            // container holding anything that needs one is serialized whole.
            return inner.Any(i => i.Kind == ReadKind.Cast) ? ReadTreatment.Cast("toString({0})") : ReadTreatment.AsIs;
        }

        return ReadTreatment.AsIs;
    }

    /// <summary>The type a watermark bound is bound as: the cursor column's own type, unwrapped.
    /// Null when a bound of that type cannot be a cursor.</summary>
    public static string? CursorParameterType(ChType type)
    {
        var t = type.Unwrapped;
        return Integers.Contains(t.Name) || t.Name.StartsWith("Decimal", StringComparison.Ordinal)
            || t.Name is "Date" or "Date32" or "DateTime" or "DateTime64" or "String"
            ? t.Render()
            : null;
    }

    private static bool IsRefused(string name) =>
        name.StartsWith("Interval", StringComparison.Ordinal)
        || name is "Time" or "Time64" or "Nothing" or "AggregateFunction" or "SimpleAggregateFunction";

    private sealed class Parser(string s)
    {
        private int _i;

        public ChType ParseTop()
        {
            var t = ParseType();
            SkipWs();
            if (_i != s.Length)
            {
                throw Fail("unexpected trailing text");
            }

            return t;
        }

        private ChType ParseType()
        {
            SkipWs();
            var name = ReadIdent() ?? throw Fail("a type name was expected");
            SkipWs();
            var ps = new List<ChTypeParam>();
            if (Peek() != '(')
            {
                return new ChType(name, ps);
            }

            _i++;
            SkipWs();
            if (Peek() == ')')
            {
                _i++;
                return new ChType(name, ps);
            }

            while (true)
            {
                ps.Add(ParseParam());
                SkipWs();
                var c = Next();
                if (c == ')')
                {
                    return new ChType(name, ps);
                }

                if (c != ',')
                {
                    throw Fail("',' or ')' was expected");
                }
            }
        }

        private ChTypeParam ParseParam()
        {
            SkipWs();
            var start = _i;
            try
            {
                var c = Peek();
                if (c == '\'' || c == '-' || char.IsAsciiDigit(c))
                {
                    var literal = ReadLiteral();
                    EnsureParamEnd();
                    return new ChTypeParam(null, literal);
                }

                if (c == '`')
                {
                    var field = ReadBackquoted();
                    var named = ParseType();
                    EnsureParamEnd();
                    return new ChTypeParam(named with { FieldName = field }, null);
                }

                var save = _i;
                var ident = ReadIdent() ?? throw Fail("a parameter was expected");
                SkipWs();
                if (IsIdentStart(Peek()) || Peek() == '`')
                {
                    var named = ParseType();
                    EnsureParamEnd();
                    return new ChTypeParam(named with { FieldName = ident }, null);
                }

                _i = save;
                var type = ParseType();
                EnsureParamEnd();
                return new ChTypeParam(type, null);
            }
            catch (FormatException)
            {
                _i = start;
                return new ChTypeParam(null, ReadRaw());
            }
        }

        /// <summary>A parameter outside the modelled grammar, kept verbatim up to the next top-level
        /// ',' or ')'. Quotes and brackets are honoured so a comma inside them does not end it.</summary>
        private string ReadRaw()
        {
            var start = _i;
            var depth = 0;
            while (_i < s.Length)
            {
                var c = s[_i];
                if (c is '\'' or '`' or '"')
                {
                    SkipQuoted(c);
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
                        break;
                    }

                    depth--;
                }
                else if (c == ',' && depth == 0)
                {
                    break;
                }

                _i++;
            }

            if (_i >= s.Length)
            {
                throw Fail("an unterminated parameter list");
            }

            return s[start.._i].Trim();
        }

        private string ReadLiteral()
        {
            var start = _i;
            if (Peek() == '\'')
            {
                SkipQuoted('\'');
            }
            else
            {
                _i++;
                while (_i < s.Length && (char.IsAsciiDigit(s[_i]) || s[_i] is '.' or 'e' or 'E' or '+' or '-'))
                {
                    _i++;
                }
            }

            var literal = s[start.._i];
            SkipWs();
            if (Peek() != '=')
            {
                return literal;
            }

            _i++;
            SkipWs();
            var valueStart = _i;
            if (Peek() == '-')
            {
                _i++;
            }

            while (_i < s.Length && char.IsAsciiDigit(s[_i]))
            {
                _i++;
            }

            if (_i == valueStart)
            {
                throw Fail("an enum value was expected after '='");
            }

            return $"{literal} = {s[valueStart.._i]}";
        }

        private void SkipQuoted(char quote)
        {
            _i++;
            while (_i < s.Length)
            {
                if (s[_i] == '\\')
                {
                    _i += 2;
                    continue;
                }

                if (s[_i] == quote)
                {
                    if (_i + 1 < s.Length && s[_i + 1] == quote)
                    {
                        _i += 2;
                        continue;
                    }

                    _i++;
                    return;
                }

                _i++;
            }

            throw Fail("an unterminated quoted literal");
        }

        private string ReadBackquoted()
        {
            var start = _i;
            SkipQuoted('`');
            return s[(start + 1)..(_i - 1)].Replace("``", "`", StringComparison.Ordinal);
        }

        private string? ReadIdent()
        {
            if (!IsIdentStart(Peek()))
            {
                return null;
            }

            var start = _i;
            while (_i < s.Length && (char.IsAsciiLetterOrDigit(s[_i]) || s[_i] == '_'))
            {
                _i++;
            }

            return s[start.._i];
        }

        private void EnsureParamEnd()
        {
            SkipWs();
            if (Peek() is not (',' or ')'))
            {
                throw Fail("',' or ')' was expected after a parameter");
            }
        }

        private static bool IsIdentStart(char c) => char.IsAsciiLetter(c) || c == '_';

        private char Peek() => _i < s.Length ? s[_i] : '\0';

        private char Next() => _i < s.Length ? s[_i++] : '\0';

        private void SkipWs()
        {
            while (_i < s.Length && char.IsWhiteSpace(s[_i]))
            {
                _i++;
            }
        }

        private FormatException Fail(string why) => new($"cannot parse ClickHouse type '{s}' at offset {_i}: {why}");
    }
}
