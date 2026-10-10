using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>An output's connector-owned write options. The engine owns and strips
/// <c>strategy</c>/<c>keys</c>/<c>duplicates</c>/<c>on_delete</c>/<c>schema_policy</c>/<c>retry</c>;
/// the mode and keys arrive as <see cref="OutputSpec.Mode"/>/<see cref="OutputSpec.Keys"/>.
/// <c>engine</c>/<c>order_by</c>/<c>partition_by</c> apply only to a table the sink creates.</summary>
internal sealed record ChWriteConfig(
    ChTableRef Target, string MergeStrategy, string? Engine, IReadOnlyList<string>? OrderBy, string? PartitionBy,
    IReadOnlyDictionary<string, string> Settings)
{
    public const string Replacing = "replacing";
    public const string DeleteInsert = "delete_insert";

    private static readonly string[] KnownOptions = ["entity", "merge_strategy", "engine", "order_by", "partition_by", "settings"];

    public static ChWriteConfig? Parse(OutputSpec spec, ChConnectionConfig cfg, List<string> errors)
    {
        var start = errors.Count;
        var context = $"output '{spec.Output}'";
        foreach (var key in spec.Options.Keys.Where(k => !KnownOptions.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add(Coded(cfg, ChCodes.Write_BadOutputOption, $"{context}: unknown option '{key}'; known options: {string.Join(", ", KnownOptions)}"));
        }

        var strategyText = spec.Options.GetValueOrDefault("merge_strategy")?.ToString();
        if (strategyText is not null && spec.Mode != "merge")
        {
            errors.Add(Coded(cfg, ChCodes.Write_BadWriteMode, $"{context}: 'merge_strategy' only applies to strategy: merge"));
        }

        var strategy = strategyText ?? Replacing;
        if (strategy is not (Replacing or DeleteInsert))
        {
            errors.Add(Coded(cfg, ChCodes.Write_BadOutputOption, $"{context}: 'merge_strategy' must be '{Replacing}' or '{DeleteInsert}'"));
        }

        List<string>? orderBy = null;
        if (spec.Options.TryGetValue("order_by", out var rawOrder) && rawOrder is not null)
        {
            if (rawOrder is IEnumerable<object?> list && list.All(o => o is string { Length: > 0 }))
            {
                orderBy = list.Cast<string>().ToList();
            }
            else
            {
                errors.Add(Coded(cfg, ChCodes.Write_BadOutputOption, $"{context}: 'order_by' must be a list of column names"));
            }
        }

        var settings = ChConnectionConfig.ParseSettings(spec.Options.GetValueOrDefault("settings"), $"{context} settings",
            ChCodes.Write_BadOutputOption, cfg.Redactor, errors);

        var entity = spec.Options.GetValueOrDefault("entity")?.ToString();
        if (!ChTableRef.TryParse(string.IsNullOrEmpty(entity) ? spec.Output : entity, cfg.Database, out var target, out var error))
        {
            errors.Add(Coded(cfg, ChCodes.Config_EntityInvalid, $"{context}: {error}"));
            return null;
        }

        return errors.Count == start
            ? new ChWriteConfig(target, strategy, spec.Options.GetValueOrDefault("engine")?.ToString(), orderBy,
                spec.Options.GetValueOrDefault("partition_by")?.ToString(), settings)
            : null;
    }

    private static string Coded(ChConnectionConfig cfg, string code, string text) => ChCodes.Message(code, cfg.Redactor, text);
}
