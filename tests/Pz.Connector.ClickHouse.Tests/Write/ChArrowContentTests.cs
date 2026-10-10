using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChArrowContentTests
{
    private static RecordBatch Batch()
    {
        var schema = new Schema([new Field("id", Int64Type.Default, false)], null);
        var ids = new Int64Array.Builder().Append(10).Append(11).Append(12).Build();
        return new RecordBatch(schema, [ids], 3);
    }

    private static async Task<RecordBatch> RoundTrip(HttpContent content)
    {
        var bytes = await content.ReadAsByteArrayAsync();
        using var reader = new ArrowStreamReader(new MemoryStream(bytes));
        return (await reader.ReadNextRecordBatchAsync())!;
    }

    [Fact]
    public async Task Serializes_the_batch_as_one_complete_stream()
    {
        using var batch = Batch();
        var read = await RoundTrip(new ChArrowContent(batch, null));
        Assert.Equal(3, read.Length);
        Assert.Equal(["id"], read.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task Appends_a_monotonic_sequence_column_and_leaves_the_engine_batch_usable()
    {
        using var batch = Batch();
        var read = await RoundTrip(new ChArrowContent(batch, 100));
        Assert.Equal(["id", "_pz_seq"], read.Schema.FieldsList.Select(f => f.Name));
        var seq = (UInt64Array)read.Column("_pz_seq");
        Assert.Equal([100UL, 101UL, 102UL], Enumerable.Range(0, 3).Select(i => seq.GetValue(i)!.Value));
        // The engine still owns the original batch: it must not have been disposed by the content.
        Assert.Equal(12, ((Int64Array)batch.Column(0)).GetValue(2));
    }
}
