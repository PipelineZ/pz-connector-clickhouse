using System.Net;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChSummaryTests
{
    [Fact]
    public void Summary_header_maps_to_scanned_bytes_server_time_and_row_extras()
    {
        var acc = new ConnectorStatsAccumulator();
        var response = new HttpResponseMessage(HttpStatusCode.OK);
        response.Headers.TryAddWithoutValidation("X-ClickHouse-Summary",
            "{\"read_rows\":\"10\",\"read_bytes\":\"2048\",\"written_rows\":\"3\",\"written_bytes\":\"96\",\"total_rows_to_read\":\"10\",\"elapsed_ns\":\"7500000\"}");
        ChSummary.Report(response, acc);
        var s = acc.Snapshot();
        Assert.Equal(2048, s.CostUnits);
        Assert.Equal("bytes_scanned", s.CostUnit);
        Assert.Equal(7, s.RemoteMs);
        Assert.Contains(new KeyValuePair<string, double>("clickhouse.read_rows", 10), s.Extras);
        Assert.Contains(new KeyValuePair<string, double>("clickhouse.written_rows", 3), s.Extras);
    }

    [Fact]
    public void Missing_or_malformed_summary_reports_nothing_and_never_throws()
    {
        var acc = new ConnectorStatsAccumulator();
        ChSummary.Report(new HttpResponseMessage(HttpStatusCode.OK), acc);
        var bad = new HttpResponseMessage(HttpStatusCode.OK);
        bad.Headers.TryAddWithoutValidation("X-ClickHouse-Summary", "{not json");
        ChSummary.Report(bad, acc);
        Assert.True(acc.Snapshot().IsEmpty);
    }
}
