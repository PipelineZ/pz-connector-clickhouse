using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse;

/// <summary>Reads an <c>ArrowStream</c> response body. ClickHouse can fail a query after it has sent
/// 200 and some batches; however it signals that, the response then does not end with Arrow's
/// end-of-stream marker. So a read is successful only if the body ends exactly with that marker --
/// a body that ends without it, or that the Arrow reader cannot parse, fails with whatever
/// <c>Code: N. DB::Exception: ...</c> text is in the last bytes. Batches already yielded belong to the
/// engine; the failure tells the engine to discard the node's landing and retry it whole.</summary>
internal static class ChArrowResponseReader
{
    private const int MaxDrainBytes = 64 * 1024;

    public static async IAsyncEnumerable<RecordBatch> ReadAsync(Stream body, ChRedactor redactor, string context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var tail = new TailStream(body);
        using var reader = new ArrowStreamReader(tail, leaveOpen: true);
        while (true)
        {
            RecordBatch? batch = null;
            Exception? failure = null;
            try
            {
                batch = await reader.ReadNextRecordBatchAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failure = ex;
            }

            if (failure is not null)
            {
                await tail.DrainAsync(MaxDrainBytes, ct).ConfigureAwait(false);
                throw Failed(tail, redactor, context, failure);
            }

            if (batch is null)
            {
                break;
            }

            yield return batch;
        }

        await EnsureCompleteAsync(tail, redactor, context, ct).ConfigureAwait(false);
    }

    public static async Task<Schema> ReadSchemaAsync(Stream body, ChRedactor redactor, string context, CancellationToken ct)
    {
        var tail = new TailStream(body);
        using var reader = new ArrowStreamReader(tail, leaveOpen: true);
        Exception? failure = null;
        try
        {
            while (await reader.ReadNextRecordBatchAsync(ct).ConfigureAwait(false) is { } batch)
            {
                batch.Dispose();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            failure = ex;
        }

        if (failure is not null)
        {
            await tail.DrainAsync(MaxDrainBytes, ct).ConfigureAwait(false);
            throw Failed(tail, redactor, context, failure);
        }

        await EnsureCompleteAsync(tail, redactor, context, ct).ConfigureAwait(false);
        return reader.Schema ?? throw Failed(tail, redactor, context, null);
    }

    private static async Task EnsureCompleteAsync(TailStream tail, ChRedactor redactor, string context, CancellationToken ct)
    {
        // Whatever follows the reader's view of the end (exception text) must be seen too.
        await tail.DrainAsync(MaxDrainBytes, ct).ConfigureAwait(false);
        if (!tail.EndsWithEndOfStream)
        {
            throw Failed(tail, redactor, context, null);
        }
    }

    private static PzConnectorException Failed(TailStream tail, ChRedactor redactor, string context, Exception? inner)
    {
        var text = tail.TailText();
        var at = text.IndexOf("Code: ", StringComparison.Ordinal);
        if (at >= 0)
        {
            var server = text[at..];
            return ChErrors.FromServer(200, ChErrors.ParseExceptionCode(server), server, null, redactor, context, ChCodes.Read_QueryFailed);
        }

        var detail = tail.Total == 0 ? "the server returned an empty body" : "the result stream ended without Arrow's end-of-stream marker";
        return new PzConnectorException(ChCodes.Message(ChCodes.Read_QueryFailed, redactor, $"{context}: {detail}"),
            isTransient: false, innerException: inner);
    }

    /// <summary>A read-only pass-through that remembers the last bytes it served and guards Arrow's
    /// message framing. The Arrow reader asks for exactly 4 bytes only when it expects a continuation
    /// marker (<c>FF FF FF FF</c>) or, right after one, a metadata length; message metadata and bodies
    /// are 8-byte aligned, so no other read is ever exactly 4 bytes. Text where a prefix belongs --
    /// a server exception written mid-stream -- would otherwise be read as a gigabyte-sized length
    /// and allocated; the guard refuses it on the spot instead.</summary>
    private sealed class TailStream(Stream inner) : Stream
    {
        private const int Keep = 4096;
        private const int MaxMetadataBytes = 64 * 1024 * 1024;
        private static readonly byte[] EndOfStream = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];
        private readonly byte[] _ring = new byte[Keep];
        private long _total;
        private bool _afterContinuation;

        public long Total => _total;

        public bool EndsWithEndOfStream => _total >= EndOfStream.Length && Last(EndOfStream.Length).SequenceEqual(EndOfStream);

        public string TailText() => Encoding.UTF8.GetString(Last((int)Math.Min(_total, Keep))).Replace('\0', ' ');

        public async Task DrainAsync(int maxBytes, CancellationToken ct)
        {
            var scratch = new byte[8192];
            var drained = 0;
            try
            {
                int n;
                while (drained < maxBytes && (n = await inner.ReadAsync(scratch.AsMemory(0, Math.Min(scratch.Length, maxBytes - drained)), ct).ConfigureAwait(false)) > 0)
                {
                    Record(scratch.AsSpan(0, n));
                    drained += n;
                }
            }
            catch (Exception ex) when (ex is IOException or HttpRequestException)
            {
                // After an in-band exception ClickHouse never terminates the chunked body, so the next
                // read throws instead of returning 0. That is the end of the body; the caller classifies
                // what was recorded.
            }
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (buffer.Length != 4)
            {
                var n = await inner.ReadAsync(buffer, ct).ConfigureAwait(false);
                Record(buffer.Span[..n]);
                _afterContinuation = false;
                return n;
            }

            var total = 0;
            while (total < 4)
            {
                var n = await inner.ReadAsync(buffer[total..], ct).ConfigureAwait(false);
                if (n == 0)
                {
                    break;
                }

                total += n;
            }

            Record(buffer.Span[..total]);
            if (total == 4)
            {
                GuardPrefix(buffer.Span);
            }

            return total;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
            ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

        private void GuardPrefix(ReadOnlySpan<byte> four)
        {
            var value = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(four);
            if (value == -1)
            {
                _afterContinuation = true;
                return;
            }

            var wasAfterContinuation = _afterContinuation;
            _afterContinuation = false;
            if (wasAfterContinuation && value is >= 0 and <= MaxMetadataBytes)
            {
                return;
            }

            throw new InvalidDataException("the response is not a well-formed Arrow IPC stream at this point");
        }

        private byte[] Last(int n)
        {
            var result = new byte[n];
            for (var i = 0; i < n; i++)
            {
                result[i] = _ring[(int)((_total - n + i) % Keep)];
            }

            return result;
        }

        private void Record(ReadOnlySpan<byte> data)
        {
            foreach (var b in data)
            {
                _ring[(int)(_total % Keep)] = b;
                _total++;
            }
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => _total; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
