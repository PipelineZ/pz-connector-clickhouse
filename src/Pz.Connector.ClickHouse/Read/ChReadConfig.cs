using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>A dataset's read options, parsed once. Exactly one of <see cref="Table"/>/<see cref="Query"/>
/// is set: an <c>entity:</c> read names a table or view, a <c>query:</c> read runs SQL verbatim.</summary>
internal sealed record ChReadConfig(ChTableRef? Table, string? Query, IReadOnlyDictionary<string, string> Settings)
{
    private static readonly string[] KnownOptions = ["entity", "query", "settings"];

    public string Label => Table is { } t ? t.Display : "query";

    public static ChReadConfig? Parse(DatasetSpec spec, ChConnectionConfig cfg, List<string> errors)
    {
        var start = errors.Count;
        foreach (var key in spec.Options.Keys.Where(k => !KnownOptions.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add(ChCodes.Message(ChCodes.Read_BadDatasetOption, cfg.Redactor,
                $"dataset '{spec.Dataset}': unknown option '{key}'; known options: {string.Join(", ", KnownOptions)}"));
        }

        var entity = spec.Options.GetValueOrDefault("entity")?.ToString();
        var query = spec.Options.GetValueOrDefault("query")?.ToString();
        var settings = ChConnectionConfig.ParseSettings(spec.Options.GetValueOrDefault("settings"), $"dataset '{spec.Dataset}' settings",
            ChCodes.Read_BadDatasetOption, cfg.Redactor, errors);

        if (!string.IsNullOrEmpty(entity) && !string.IsNullOrEmpty(query))
        {
            errors.Add(ChCodes.Message(ChCodes.Read_EntityAndQuery, cfg.Redactor,
                $"dataset '{spec.Dataset}': 'entity' and 'query' cannot both be set; use one or the other"));
            return null;
        }

        if (!string.IsNullOrEmpty(query))
        {
            return errors.Count == start ? new ChReadConfig(null, query, settings) : null;
        }

        if (!ChTableRef.TryParse(string.IsNullOrEmpty(entity) ? spec.Dataset : entity, cfg.Database, out var table, out var error))
        {
            errors.Add(ChCodes.Message(ChCodes.Config_EntityInvalid, cfg.Redactor, $"dataset '{spec.Dataset}': {error}"));
            return null;
        }

        return errors.Count == start ? new ChReadConfig(table, null, settings) : null;
    }
}
