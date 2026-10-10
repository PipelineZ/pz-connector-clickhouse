using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChReadPlanTests
{
    private static readonly ChReadConfig Table = new(new ChTableRef("db", "events"), null, new Dictionary<string, string>());

    private static IReadOnlyList<ChColumn> Cols(params (string Name, string Type)[] cols) =>
        ChReadPlan.Columns(cols.Select(c => (IReadOnlyList<string?>)[c.Name, c.Type]).ToList(), ChRedactor.None, "ctx");

    private static DatasetSpec Spec() => new("clickhouse", "events", new Dictionary<string, object?>());

    [Fact]
    public void Describe_wraps_the_table_or_the_query()
    {
        Assert.Equal("DESCRIBE (SELECT * FROM `db`.`events`)", ChReadPlan.DescribeSql(Table));
        Assert.Equal("DESCRIBE (select 1)", ChReadPlan.DescribeSql(new ChReadConfig(null, " select 1; ", new Dictionary<string, string>())));
    }

    [Fact]
    public void Plain_columns_read_star()
    {
        var plan = ChReadPlan.Build(Table, Spec(), ReadHints.None, Cols(("id", "UInt64"), ("s", "String")), false, false, ChRedactor.None);
        Assert.Equal("SELECT * FROM `db`.`events`", plan.Sql);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void A_cast_column_makes_the_projection_explicit_and_keeps_names()
    {
        var plan = ChReadPlan.Build(Table, Spec(), ReadHints.None, Cols(("id", "UInt64"), ("d", "Date"), ("e", "Enum8('a' = 1)")), false, false, ChRedactor.None);
        Assert.Equal("SELECT `id`, toDate32(`d`) AS `d`, toString(`e`) AS `e` FROM `db`.`events`", plan.Sql);
    }

    [Fact]
    public void Hinted_columns_prune_in_hint_order_matching_case_insensitively()
    {
        var hints = new ReadHints(["S", "id"], null, null);
        var plan = ChReadPlan.Build(Table, Spec(), hints, Cols(("id", "UInt64"), ("s", "String"), ("x", "String")), false, false, ChRedactor.None);
        Assert.Equal("SELECT `s`, `id` FROM `db`.`events`", plan.Sql);
    }

    [Fact]
    public void A_hint_naming_an_unknown_column_prunes_nothing()
    {
        var plan = ChReadPlan.Build(Table, Spec(), new ReadHints(["nope"], null, null), Cols(("id", "UInt64")), false, false, ChRedactor.None);
        Assert.Equal("SELECT * FROM `db`.`events`", plan.Sql);
    }

    [Fact]
    public void Predicate_and_limit_push_down_parenthesized()
    {
        var hints = new ReadHints(null, "a = 1 OR b = 2", 10);
        var plan = ChReadPlan.Build(Table, Spec(), hints, Cols(("a", "Int32"), ("b", "Int32")), pushPredicate: true, false, ChRedactor.None);
        Assert.Equal("SELECT * FROM `db`.`events` WHERE (a = 1 OR b = 2) LIMIT 10", plan.Sql);
    }

    [Fact]
    public void Predicate_is_omitted_when_not_pushed()
    {
        var plan = ChReadPlan.Build(Table, Spec(), new ReadHints(null, "a = 1", null), Cols(("a", "Int32")), pushPredicate: false, false, ChRedactor.None);
        Assert.Equal("SELECT * FROM `db`.`events`", plan.Sql);
    }

    [Fact]
    public void Watermark_bounds_are_typed_parameters_from_the_cursor_column()
    {
        var spec = Spec() with { WatermarkCursor = "ts", WatermarkValue = "2024-05-01 10:00:00+00", WatermarkUpperBound = "2024-05-02 00:00:00", WatermarkLowerInclusive = true };
        var plan = ChReadPlan.Build(Table, spec, new ReadHints(null, "x > 1", null), Cols(("ts", "Nullable(DateTime)"), ("x", "Int32")), true, false, ChRedactor.None);
        Assert.Equal("SELECT toDateTime64(`ts`, 0) AS `ts`, `x` FROM `db`.`events` WHERE (x > 1) AND (`ts` >= {pz_lower:DateTime}) AND (`ts` <= {pz_upper:DateTime})", plan.Sql);
        Assert.Equal("2024-05-01 10:00:00+00", plan.Parameters["pz_lower"]);
        Assert.Equal("2024-05-02 00:00:00", plan.Parameters["pz_upper"]);
    }

    [Fact]
    public void Exclusive_lower_bound_uses_gt()
    {
        var spec = Spec() with { WatermarkCursor = "id", WatermarkValue = "3" };
        Assert.Contains("(`id` > {pz_lower:UInt64})", ChReadPlan.Build(Table, spec, ReadHints.None, Cols(("id", "UInt64")), false, false, ChRedactor.None).Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Unsupported_cursor_type_is_0202()
    {
        var spec = Spec() with { WatermarkCursor = "f", WatermarkValue = "1.5" };
        var ex = Assert.Throws<PzConnectorException>(() => ChReadPlan.Build(Table, spec, ReadHints.None, Cols(("f", "Float64")), false, false, ChRedactor.None));
        Assert.Contains("PZCH0202", ex.Message);
    }

    [Fact]
    public void Missing_cursor_is_0202()
    {
        var spec = Spec() with { WatermarkCursor = "nope", WatermarkValue = "1" };
        Assert.Contains("PZCH0202", Assert.Throws<PzConnectorException>(() => ChReadPlan.Build(Table, spec, ReadHints.None, Cols(("id", "UInt64")), false, false, ChRedactor.None)).Message);
    }

    [Fact]
    public void Limit_zero_probe_replaces_any_limit()
    {
        var plan = ChReadPlan.Build(Table, Spec(), new ReadHints(null, null, 10), Cols(("id", "UInt64")), false, limitZero: true, ChRedactor.None);
        Assert.Equal("SELECT * FROM `db`.`events` LIMIT 0", plan.Sql);
    }

    [Fact]
    public void Query_mode_is_verbatim_unless_a_cast_is_needed()
    {
        var q = new ChReadConfig(null, "select * from t;", new Dictionary<string, string>());
        Assert.Equal("select * from t;", ChReadPlan.Build(q, Spec(), new ReadHints(["id"], "id > 1", 5), Cols(("id", "UInt64")), true, false, ChRedactor.None).Sql);
        Assert.Equal("SELECT toDate32(`d`) AS `d` FROM (select * from t) AS pz_query",
            ChReadPlan.Build(q, Spec(), ReadHints.None, Cols(("d", "Date")), false, false, ChRedactor.None).Sql);
        Assert.Equal("SELECT * FROM (select * from t) AS pz_query LIMIT 0",
            ChReadPlan.Build(q, Spec(), ReadHints.None, Cols(("id", "UInt64")), false, limitZero: true, ChRedactor.None).Sql);
    }

    [Fact]
    public void Unreadable_column_is_0204_naming_column_and_type()
    {
        var ex = Assert.Throws<PzConnectorException>(() => Cols(("i", "IntervalDay")));
        Assert.Contains("PZCH0204", ex.Message);
        Assert.Contains("'i'", ex.Message);
        Assert.Contains("IntervalDay", ex.Message);
    }

    [Fact]
    public void Settings_fix_the_wire_format_and_size_blocks_from_batch_options()
    {
        var cfg = Table with { Settings = new Dictionary<string, string> { ["max_threads"] = "8" } };
        var s = ChReadPlan.Settings(cfg, new BatchOptions(TargetBatchBytes: 4096, MaxRowsPerBatch: 122_880));
        Assert.Equal("ArrowStream", s["default_format"]);
        Assert.Equal("none", s["output_format_arrow_compression_method"]);
        Assert.Equal("1", s["output_format_arrow_string_as_string"]);
        Assert.Equal("0", s["output_format_arrow_low_cardinality_as_dictionary"]);
        Assert.Equal("best_effort", s["date_time_input_format"]);
        Assert.Equal("1", s["prefer_column_name_to_alias"]);
        Assert.Equal("64", s["max_block_size"]);
        Assert.Equal("4096", s["preferred_block_size_bytes"]);
        Assert.Equal("8", s["max_threads"]);
        Assert.Equal("122880", ChReadPlan.Settings(Table, BatchOptions.Default)["max_block_size"]);
    }
}
