using System.Globalization;
using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>Maps the <c>X-ClickHouse-Summary</c> response header (the server's own account of the
/// query: rows and bytes read and written, elapsed nanoseconds) onto the node's stats. The header is
/// sent before the body, so on a streamed read it holds what the server had counted when the headers
/// went out, not the final totals; the figures are a lower bound there. Telemetry never fails a
/// request: a missing or malformed header reports nothing.</summary>
internal static class ChSummary
{
    private const string Header = "X-ClickHouse-Summary";

    public static void Report(HttpResponseMessage response, IConnectorStats stats)
    {
        if (!response.Headers.TryGetValues(Header, out var values) || values.FirstOrDefault() is not { } json)
        {
            return;
        }

        long readRows = 0, readBytes = 0, writtenRows = 0, elapsedNs = 0;
        try
        {
            var reader = new Utf8JsonReader(System.Text.Encoding.UTF8.GetBytes(json));
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return;
            }

            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                var name = reader.GetString();
                reader.Read();
                var value = reader.TokenType switch
                {
                    JsonTokenType.String => long.Parse(reader.GetString()!, CultureInfo.InvariantCulture),
                    JsonTokenType.Number => reader.GetInt64(),
                    _ => 0L,
                };
                reader.Skip();
                switch (name)
                {
                    case "read_rows": readRows = value; break;
                    case "read_bytes": readBytes = value; break;
                    case "written_rows": writtenRows = value; break;
                    case "elapsed_ns": elapsedNs = value; break;
                    default: break;
                }
            }
        }
        catch (Exception ex) when (ex is JsonException or FormatException or OverflowException or InvalidOperationException)
        {
            return;
        }

        if (readBytes > 0)
        {
            stats.AddCost(readBytes, "bytes_scanned");
        }

        if (elapsedNs > 0)
        {
            stats.AddRemoteMs(elapsedNs / 1_000_000);
        }

        if (readRows > 0)
        {
            stats.AddExtra("clickhouse.read_rows", readRows);
        }

        if (writtenRows > 0)
        {
            stats.AddExtra("clickhouse.written_rows", writtenRows);
        }
    }
}
