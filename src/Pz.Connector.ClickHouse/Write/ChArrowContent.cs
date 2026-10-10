using System.Net;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Pz.Connector.ClickHouse;

/// <summary>One engine batch as a complete Arrow IPC stream request body. The batch is engine-owned
/// and is only borrowed: the content never disposes it or its arrays. For merge, a <c>_pz_seq</c>
/// column (monotonic per session) is appended so "last row per key wins" is well defined; only that
/// array is the content's own, and it is disposed after serialization. The request completes before
/// <c>WriteBatchAsync</c> returns, inside the engine's ownership window.</summary>
internal sealed class ChArrowContent(RecordBatch batch, long? firstSequence) : HttpContent
{
    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken ct)
    {
        if (firstSequence is not { } first)
        {
            await WriteAsync(stream, batch, ct).ConfigureAwait(false);
            return;
        }

        var builder = new UInt64Array.Builder();
        for (var i = 0; i < batch.Length; i++)
        {
            builder.Append((ulong)(first + i));
        }

        using var sequence = builder.Build();
        var fields = batch.Schema.FieldsList.Append(new Field(ChDdl.SequenceColumn, UInt64Type.Default, false));
        var schema = new Schema(fields, batch.Schema.Metadata);
        // Not disposed: it shares the engine's arrays.
        var combined = new RecordBatch(schema, batch.Arrays.Append(sequence), batch.Length);
        await WriteAsync(stream, combined, ct).ConfigureAwait(false);
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
        SerializeToStreamAsync(stream, context, CancellationToken.None);

    protected override bool TryComputeLength(out long length)
    {
        length = 0;
        return false;
    }

    private static async Task WriteAsync(Stream stream, RecordBatch b, CancellationToken ct)
    {
        using var writer = new ArrowStreamWriter(stream, b.Schema, leaveOpen: true);
        await writer.WriteRecordBatchAsync(b, ct).ConfigureAwait(false);
        await writer.WriteEndAsync(ct).ConfigureAwait(false);
    }
}
