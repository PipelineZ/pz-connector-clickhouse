using System.Runtime.CompilerServices;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>The whole dataset as one streamed query. Every read sends a fresh query id, so a
/// partition the engine reads twice never shares server state with an earlier attempt. On
/// cancellation the response is abandoned (which aborts the query) and a best-effort
/// <c>KILL QUERY</c> follows for a server that keeps going.</summary>
internal sealed class ChPartition(ChHttpClient client, ChReadPlan plan, ChReadConfig config, string context, ILogger logger) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var queryId = ChHttpClient.NewQueryId();
        await using var kill = ct.Register(() => _ = client.KillQueryAsync(queryId));
        using var response = await client.SendAsync(new ChRequest(context, ChCodes.Read_QueryFailed)
        {
            Sql = plan.Sql,
            Parameters = plan.Parameters,
            Settings = ChReadPlan.Settings(config, options),
            QueryId = queryId,
        }, ct).ConfigureAwait(false);
        await using var body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

        long rows = 0;
        await foreach (var batch in ChArrowResponseReader.ReadAsync(body, client.Config.Redactor, context, ct).ConfigureAwait(false))
        {
            rows += batch.Length;
            yield return batch;
        }

        logger.LogDebug("clickhouse: {Context}: read {Rows} row(s) (query_id {QueryId})", context, rows, queryId);
    }
}
