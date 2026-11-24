using System.Net;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;

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
        var owned = ListifyFixedSizeLists(batch);
        try
        {
            await SerializeAsync(stream, owned ?? batch, ct).ConfigureAwait(false);
        }
        finally
        {
            owned?.Dispose();
        }
    }

    /// <summary>ClickHouse's Arrow reader has no fixed-size list: it reads one as a variable list and
    /// rejects it, because the offsets buffer is missing. A batch holding one (at any depth) is rebuilt
    /// with plain lists, and a null fixed-size list becomes an empty list, as every null container does
    /// on this sink. Null when the batch needs no rewrite.</summary>
    private static RecordBatch? ListifyFixedSizeLists(RecordBatch source)
    {
        if (!source.Schema.FieldsList.Any(f => HasFixedSizeList(f.DataType)))
        {
            return null;
        }

        var schema = new Schema(source.Schema.FieldsList.Select(f => new Field(f.Name, Listify(f.DataType), f.IsNullable)), source.Schema.Metadata);
        var builder = new ArrowBatchBuilder(schema, int.MaxValue, maxRowsPerBatch: int.MaxValue);
        var row = new object?[source.ColumnCount];
        for (var i = 0; i < source.Length; i++)
        {
            for (var c = 0; c < row.Length; c++)
            {
                row[c] = Boxed(source.Column(c).Data.DataType, ArrowCell.GetValue(source.Column(c), i));
            }

            builder.AppendRow(row);
        }

        return builder.Flush() ?? new RecordBatch(schema, schema.FieldsList.Select(f => ArrowArrayFactory.BuildArray(new ArrayData(f.DataType, 0, 0, 0, []))), 0);
    }

    /// <summary><see cref="ArrowCell.GetValue"/> returns a map as key/value pairs; the builder takes a dictionary.</summary>
    private static object? Boxed(IArrowType type, object? value) => (type, value) switch
    {
        (_, null) => null,
        (MapType m, List<KeyValuePair<object, object?>> pairs) =>
            pairs.ToDictionary(p => p.Key, p => Boxed(m.ValueField.DataType, p.Value)),
        (FixedSizeListType l, List<object?> items) => items.Select(v => Boxed(l.ValueDataType, v)).ToList(),
        (ListType l, List<object?> items) => items.Select(v => Boxed(l.ValueDataType, v)).ToList(),
        (StructType s, Dictionary<string, object?> fields) =>
            fields.ToDictionary(kv => kv.Key, kv => Boxed(s.GetFieldByName(kv.Key).DataType, kv.Value)),
        _ => value,
    };

    private static bool HasFixedSizeList(IArrowType type) => type switch
    {
        FixedSizeListType => true,
        ListType l => HasFixedSizeList(l.ValueDataType),
        LargeListType l => HasFixedSizeList(l.ValueDataType),
        StructType s => s.Fields.Any(f => HasFixedSizeList(f.DataType)),
        MapType m => HasFixedSizeList(m.KeyField.DataType) || HasFixedSizeList(m.ValueField.DataType),
        _ => false,
    };

    private static IArrowType Listify(IArrowType type) => type switch
    {
        FixedSizeListType l => new ListType(new Field(l.ValueField.Name, Listify(l.ValueDataType), l.ValueField.IsNullable)),
        ListType l => new ListType(new Field(l.ValueField.Name, Listify(l.ValueDataType), l.ValueField.IsNullable)),
        StructType s => new StructType(s.Fields.Select(f => new Field(f.Name, Listify(f.DataType), f.IsNullable)).ToList()),
        MapType m => new MapType(new Field(m.KeyField.Name, Listify(m.KeyField.DataType), false), new Field(m.ValueField.Name, Listify(m.ValueField.DataType), m.ValueField.IsNullable)),
        _ => type,
    };

    private async Task SerializeAsync(Stream stream, RecordBatch batch, CancellationToken ct)
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
