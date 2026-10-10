using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChArrowResponseReaderTests
{
    private static readonly Schema Schema = new([new Field("id", Int64Type.Default, false)], null);

    private static byte[] Stream(int batches, bool end = true)
    {
        var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, Schema, leaveOpen: true))
        {
            writer.WriteStart();
            for (var b = 0; b < batches; b++)
            {
                var ids = new Int64Array.Builder();
                for (var i = 0; i < 10; i++) ids.Append(b * 10 + i);
                writer.WriteRecordBatch(new RecordBatch(Schema, [ids.Build()], 10));
            }

            if (end) writer.WriteEnd();
        }

        return ms.ToArray();
    }

    private static async Task<long> Drain(byte[] bytes)
    {
        long rows = 0;
        await foreach (var batch in ChArrowResponseReader.ReadAsync(new MemoryStream(bytes), ChRedactor.None, "reading 'x'", default))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    [Fact]
    public async Task Complete_stream_yields_every_batch() => Assert.Equal(30, await Drain(Stream(3)));

    [Fact]
    public async Task Stream_missing_the_end_marker_fails_0203()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Drain(Stream(3, end: false)));
        Assert.Contains("PZCH0203", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task Exception_text_after_data_fails_0203_with_the_server_message()
    {
        var bytes = Stream(2, end: false).Concat(Encoding.UTF8.GetBytes("Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n")).ToArray();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Drain(bytes));
        Assert.Contains("PZCH0203", ex.Message);
        Assert.Contains("ClickHouse error 395", ex.Message);
    }

    [Fact]
    public async Task Text_where_a_message_prefix_belongs_is_refused_without_a_huge_allocation()
    {
        var before = GC.GetTotalAllocatedBytes(precise: true);
        var bytes = Stream(1, end: false).Concat(Encoding.UTF8.GetBytes("Code: 241. DB::Exception: Memory limit exceeded")).ToArray();
        await Assert.ThrowsAsync<PzConnectorException>(() => Drain(bytes));
        Assert.True(GC.GetTotalAllocatedBytes(precise: true) - before < 64L * 1024 * 1024, "the reader must not allocate for a bogus message length");
    }

    [Fact]
    public async Task Transient_code_in_the_tail_is_transient()
    {
        var bytes = Stream(1, end: false).Concat(Encoding.UTF8.GetBytes("Code: 159. DB::Exception: Timeout exceeded")).ToArray();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Drain(bytes));
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public async Task Garbage_fails_0203_not_a_raw_parser_exception()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Drain(Encoding.UTF8.GetBytes("<html>502 Bad Gateway</html>")));
        Assert.Contains("PZCH0203", ex.Message);
    }

    [Fact]
    public async Task Schema_only_stream_gives_the_schema()
    {
        var schema = await ChArrowResponseReader.ReadSchemaAsync(new MemoryStream(Stream(0)), ChRedactor.None, "probe", default);
        Assert.Equal("id", schema.FieldsList.Single().Name);
    }

    [Fact]
    public async Task Empty_body_is_0203_for_the_schema_probe()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ChArrowResponseReader.ReadSchemaAsync(new MemoryStream([]), ChRedactor.None, "probe", default));
        Assert.Contains("PZCH0203", ex.Message);
    }

    private sealed class ThrowAfterStream(byte[] bytes) : Stream
    {
        private readonly MemoryStream _inner = new(bytes);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            var n = await _inner.ReadAsync(buffer, ct);
            if (n == 0 && buffer.Length > 0) throw new IOException("The response ended prematurely, with at least 1 additional bytes expected.");
            return n;
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public async Task In_band_exception_then_unterminated_body_fails_0203_with_the_server_code()
    {
        var bytes = Stream(2, end: false).Concat(Encoding.UTF8.GetBytes("__exception__\r\nCode: 395. DB::Exception: Value passed to 'throwIf' function is non-zero")).ToArray();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in ChArrowResponseReader.ReadAsync(new ThrowAfterStream(bytes), ChRedactor.None, "reading 'x'", default))
            {
                batch.Dispose();
            }
        });
        Assert.Contains("PZCH0203", ex.Message);
        Assert.Contains("ClickHouse error 395", ex.Message);
    }
}
