using System.Text.Json.Serialization;

namespace Pz.Connector.ClickHouse;

/// <summary>The <c>JSONCompactStrings</c> answer: every value as a string (or null), so one shape
/// serves every metadata query.</summary>
internal sealed class ChCompactStrings
{
    [JsonPropertyName("data")]
    public List<List<string?>> Data { get; set; } = [];
}

[JsonSerializable(typeof(ChCompactStrings))]
internal sealed partial class ChJsonContext : JsonSerializerContext;
