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

    [Fact]
    public async Task Fixed_size_lists_are_sent_as_variable_lists_with_nulls_emptied()
    {
        var type = new FixedSizeListType(new Field("item", Int32Type.Default, true), 2);
        var values = new Int32Array.Builder().Append(1).Append(2).Append(0).Append(0).Append(5).Append(6).Build();
        var pair = new FixedSizeListArray(new ArrayData(type, 3, 1, 0,
            [new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build()], [values.Data]));
        var schema = new Schema([new Field("pair", type, true)], null);
        using var batch = new RecordBatch(schema, [pair], 3);

        var read = await RoundTrip(new ChArrowContent(batch, null));

        var list = Assert.IsType<ListArray>(read.Column("pair"));
        Assert.Equal([2, 0, 2], Enumerable.Range(0, 3).Select(i => list.GetValueLength(i)));
        Assert.Same(type, batch.Schema.GetFieldByName("pair").DataType);
    }
}
