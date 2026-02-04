namespace MyPgsql;

using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

using static MyPgsql.PgTypes;

#pragma warning disable CA1010
public sealed class PgDataReader : DbDataReader
{
    private readonly PgProtocolHandler protocol;
    private readonly PgConnection connection;
    private readonly CommandBehavior behavior;
    private readonly CancellationToken cancellation;

    private PgColumnInfo[] columns = default!;
    private bool isClosed;
    private int fieldCount;
    private bool hasRows;
    private bool firstRowRead;
    private bool completed;

    // Row data buffer
    private byte[] rowBuffer = default!;
    private int rowBaseOffset;
    private int[] offsets = default!;
    private int[] lengths = default!;

    //--------------------------------------------------------------------------------
    // Properties
    //--------------------------------------------------------------------------------

    public override bool IsClosed => isClosed;

    public override int Depth => 0;

    public override int FieldCount => fieldCount;

    public override int RecordsAffected => -1;

    public override bool HasRows => hasRows;

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    //--------------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------------

    internal PgDataReader(PgProtocolHandler protocol, PgConnection connection, CommandBehavior behavior, CancellationToken cancellation)
    {
        this.protocol = protocol;
        this.connection = connection;
        this.behavior = behavior;
        this.cancellation = cancellation;
    }

    public override async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    public override async Task CloseAsync()
    {
        if (isClosed)
        {
            return;
        }
        isClosed = true;

        // If not completed, drain messages until ReadyForQuery
        if (!completed)
        {
            await DrainToReadyForQueryAsync().ConfigureAwait(false);
        }

        // Return buffers to pool
        // ReSharper disable ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (columns is not null)
        {
            ArrayPool<PgColumnInfo>.Shared.Return(columns);
            columns = default!;
        }
        if (offsets is not null)
        {
            ArrayPool<int>.Shared.Return(offsets);
            offsets = default!;
        }
        if (lengths is not null)
        {
            ArrayPool<int>.Shared.Return(lengths);
            lengths = default!;
        }
        // ReSharper restore ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract

        rowBuffer = default!;

        if ((behavior & CommandBehavior.CloseConnection) != 0)
        {
            await connection.CloseAsync().ConfigureAwait(false);
        }
    }

    //--------------------------------------------------------------------------------
    // GetEnumerator
    //--------------------------------------------------------------------------------

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, closeReader: false);
    }

    //--------------------------------------------------------------------------------
    // NextResult
    //--------------------------------------------------------------------------------

    public override bool NextResult()
    {
        return false;
    }

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        return Task.FromResult(false);
    }

    //--------------------------------------------------------------------------------
    // Read
    //--------------------------------------------------------------------------------

    public override bool Read()
    {
        return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        return ReadAsyncCore(cancellationToken).AsTask();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<bool> ReadAsyncCore(CancellationToken cancellationToken)
    {
        if (completed)
        {
            return new ValueTask<bool>(false);
        }

        // Synchronous path: if enough data is already buffered
        var result = TryReadSync();
        if (result.HasValue)
        {
            return new ValueTask<bool>(result.Value);
        }

        // Asynchronous path
        return ReadAsyncCoreInternal(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool? TryReadSync()
    {
        var buffer = protocol.StreamBuffer;
        ref var pos = ref protocol.StreamBufferPos;
        var len = protocol.StreamBufferLen;

        while (true)
        {
            var available = len - pos;

            // Check if we can read the header (5 bytes)
            if (available < 5)
            {
                return null;
            }

            var messageType = (char)buffer[pos];
            var payloadLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(pos + 1)) - 4;

            // Check if we can read the entire payload
            var totalMessageSize = 5 + payloadLength;
            if (available < totalMessageSize)
            {
                return null;
            }

            pos += 5;
            var payloadOffset = pos;

            switch (messageType)
            {
                case 'D': // DataRow
                    ParseDataRow(buffer.AsSpan(payloadOffset, payloadLength), payloadOffset);
                    pos += payloadLength;
                    if (!firstRowRead)
                    {
                        hasRows = true;
                        firstRowRead = true;
                    }
                    return true;

                case 'T': // RowDescription
                    ParseRowDescription(buffer.AsSpan(payloadOffset, payloadLength));
                    pos += payloadLength;
                    break;

                case 'C': // CommandComplete
                    pos += payloadLength;
                    break;

                case 'Z': // ReadyForQuery
                    pos += payloadLength;
                    completed = true;
                    return false;

                case 'E': // Error
                    var error = ParseErrorMessage(buffer.AsSpan(payloadOffset, payloadLength));
                    pos += payloadLength;
                    throw new PgException($"Query error: {error}");

                case '1': // ParseComplete (Extended Query Protocol)
                case '2': // BindComplete (Extended Query Protocol)
                case 'n': // NoData (Extended Query Protocol)
                    pos += payloadLength;
                    break;

                default:
                    pos += payloadLength;
                    break;
            }
        }
    }

    // ReSharper disable once ParameterHidesMember
    private async ValueTask<bool> ReadAsyncCoreInternal(CancellationToken cancellationToken)
    {
        while (true)
        {
            // Read header
            await protocol.EnsureBufferedAsync(5, cancellationToken).ConfigureAwait(false);

            var buffer = protocol.StreamBuffer;
            var pos = protocol.StreamBufferPos;

            var messageType = (char)buffer[pos];
            var payloadLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(pos + 1)) - 4;

            // Ensure header + payload at once
            await protocol.EnsureBufferedAsync(5 + payloadLength, cancellationToken).ConfigureAwait(false);

            // Re-fetch buffer since EnsureBufferedAsync may change it
            buffer = protocol.StreamBuffer;
            pos = protocol.StreamBufferPos;

            protocol.StreamBufferPos = pos + 5;
            var payloadOffset = pos + 5;
            var payload = buffer.AsSpan(payloadOffset, payloadLength);

            switch (messageType)
            {
                case 'D': // DataRow
                    ParseDataRow(payload, payloadOffset);
                    protocol.StreamBufferPos += payloadLength;
                    if (!firstRowRead)
                    {
                        hasRows = true;
                        firstRowRead = true;
                    }
                    return true;

                case 'T': // RowDescription
                    ParseRowDescription(payload);
                    protocol.StreamBufferPos += payloadLength;
                    // Try to continue with synchronous path
                    var syncResult = TryReadSync();
                    if (syncResult.HasValue)
                    {
                        return syncResult.Value;
                    }
                    break;

                case 'C': // CommandComplete
                    protocol.StreamBufferPos += payloadLength;
                    // Try to continue with synchronous path
                    syncResult = TryReadSync();
                    if (syncResult.HasValue)
                    {
                        return syncResult.Value;
                    }
                    break;

                case 'Z': // ReadyForQuery
                    protocol.StreamBufferPos += payloadLength;
                    completed = true;
                    return false;

                case 'E': // Error
                    var error = ParseErrorMessage(payload);
                    protocol.StreamBufferPos += payloadLength;
                    throw new PgException($"Query error: {error}");

                case '1': // ParseComplete (Extended Query Protocol)
                case '2': // BindComplete (Extended Query Protocol)
                case 'n': // NoData (Extended Query Protocol)
                    protocol.StreamBufferPos += payloadLength;
                    // Try to continue with synchronous path
                    syncResult = TryReadSync();
                    if (syncResult.HasValue)
                    {
                        return syncResult.Value;
                    }
                    break;

                default:
                    protocol.StreamBufferPos += payloadLength;
                    // Try to continue with synchronous path
                    syncResult = TryReadSync();
                    if (syncResult.HasValue)
                    {
                        return syncResult.Value;
                    }
                    break;
            }
        }
    }

    private void ParseRowDescription(ReadOnlySpan<byte> payload)
    {
        var count = BinaryPrimitives.ReadInt16BigEndian(payload);
        fieldCount = count;
        columns = ArrayPool<PgColumnInfo>.Shared.Rent(count);
        offsets = ArrayPool<int>.Shared.Rent(count);
        lengths = ArrayPool<int>.Shared.Rent(count);

        var offset = 2;
        for (var i = 0; i < count; i++)
        {
            var nameEnd = payload[offset..].IndexOf((byte)0);
            var name = Encoding.UTF8.GetString(payload.Slice(offset, nameEnd));
            offset += nameEnd + 1;

            // Table OID (4), Column number (2), Type OID (4), Type size (2), Type modifier (4), Format (2)
            var typeOid = BinaryPrimitives.ReadInt32BigEndian(payload[(offset + 6)..]);
            var formatCode = BinaryPrimitives.ReadInt16BigEndian(payload[(offset + 16)..]);
            offset += 18;

            columns[i] = new PgColumnInfo(name, typeOid, formatCode);
        }
    }

    private void ParseDataRow(ReadOnlySpan<byte> payload, int payloadOffset)
    {
        var columnCount = BinaryPrimitives.ReadInt16BigEndian(payload);
        rowBuffer = protocol.StreamBuffer;
        rowBaseOffset = payloadOffset;

        var currentOffset = 2;
        for (var i = 0; i < columnCount; i++)
        {
            var len = BinaryPrimitives.ReadInt32BigEndian(payload[currentOffset..]);
            currentOffset += 4;

            offsets[i] = currentOffset;
            lengths[i] = len;

            if (len > 0)
            {
                currentOffset += len;
            }
        }
    }

    /// <summary>
    /// Drain all messages until ReadyForQuery ('Z') message.
    /// </summary>
    private async ValueTask DrainToReadyForQueryAsync()
    {
        while (true)
        {
            await protocol.EnsureBufferedAsync(5, cancellation).ConfigureAwait(false);

            var buffer = protocol.StreamBuffer;
            var pos = protocol.StreamBufferPos;

            var messageType = (char)buffer[pos];
            var payloadLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(pos + 1)) - 4;

            await protocol.EnsureBufferedAsync(5 + payloadLength, cancellation).ConfigureAwait(false);

            protocol.StreamBufferPos += 5 + payloadLength;

            if (messageType == 'Z')
            {
                completed = true;
                return;
            }

            if (messageType == 'E')
            {
                // Ignore errors and continue (already closing)
            }
        }
    }

    private static string ParseErrorMessage(ReadOnlySpan<byte> payload)
    {
        var offset = 0;
        while (offset < payload.Length && payload[offset] != 0)
        {
            var fieldType = (char)payload[offset++];
            var end = payload[offset..].IndexOf((byte)0);
            if (fieldType == 'M')
            {
                return Encoding.UTF8.GetString(payload.Slice(offset, end));
            }
            offset += end + 1;
        }
        return "Unknown error";
    }

    //--------------------------------------------------------------------------------
    // Helpers
    //--------------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> GetValueSpan(int ordinal)
    {
        var length = lengths[ordinal];
        if (length == -1)
        {
            throw new InvalidCastException("Value is NULL");
        }
        return rowBuffer.AsSpan(rowBaseOffset + offsets[ordinal], length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsBinaryFormat(int ordinal) => columns[ordinal].FormatCode == FormatBinary;

    //--------------------------------------------------------------------------------
    // Field metadata
    //--------------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsDBNull(int ordinal) => lengths[ordinal] == -1;

    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        return Task.FromResult(IsDBNull(ordinal));
    }

    public override string GetDataTypeName(int ordinal)
    {
        var typeOid = columns[ordinal].TypeOid;
        return OidToTypeNameMap.GetValueOrDefault(typeOid, "unknown");
    }

    public override Type GetFieldType(int ordinal)
    {
        var typeOid = columns[ordinal].TypeOid;
        return OidToTypeMap.TryGetValue(typeOid, out var type) ? type : typeof(object);
    }

    public override string GetName(int ordinal)
    {
        return columns[ordinal].Name;
    }

    public override int GetOrdinal(string name)
    {
        var cols = columns;
        for (var i = 0; i < fieldCount; i++)
        {
            if (cols[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
#pragma warning disable CA2201
        throw new IndexOutOfRangeException($"Column '{name}' not found");
#pragma warning restore CA2201
    }

    //--------------------------------------------------------------------------------
    // Get values
    //--------------------------------------------------------------------------------

    public override object GetValue(int ordinal)
    {
        if (IsDBNull(ordinal))
        {
            return DBNull.Value;
        }

        var typeOid = columns[ordinal].TypeOid;
        return typeOid switch
        {
            OidBool => GetBoolean(ordinal),
            OidInt2 => GetInt16(ordinal),
            OidInt4 or OidOid => GetInt32(ordinal),
            OidInt8 => GetInt64(ordinal),
            OidFloat4 => GetFloat(ordinal),
            OidFloat8 => GetDouble(ordinal),
            OidNumeric => GetDecimal(ordinal),
            OidText or OidVarchar or OidChar => GetString(ordinal),
            OidBytea => GetBytea(ordinal),
            OidDate or OidTimestamp or OidTimestampTz => GetDateTime(ordinal),
            OidUuid => GetGuid(ordinal),
            _ => GetString(ordinal) // Fallback to string for unknown types
        };
    }

    private byte[] GetBytea(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        return span.ToArray();
    }

    public override int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, fieldCount);
        for (var i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }
        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool GetBoolean(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            return span[0] != 0;
        }
        return span.Length > 0 && (span[0] == 't' || span[0] == '1');
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte GetByte(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            return span[0];
        }
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out byte value, out _);
        return value;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var span = GetValueSpan(ordinal);
        if (buffer is null)
        {
            return span.Length;
        }

        var copyLength = Math.Min(length, span.Length - (int)dataOffset);
        if (copyLength <= 0)
        {
            return 0;
        }

        span.Slice((int)dataOffset, copyLength).CopyTo(buffer.AsSpan(bufferOffset));
        return copyLength;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override char GetChar(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        return (char)span[0];
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var str = GetString(ordinal);
        if (buffer is null)
        {
            return str.Length;
        }

        var copyLength = Math.Min(length, str.Length - (int)dataOffset);
        str.CopyTo((int)dataOffset, buffer, bufferOffset, copyLength);
        return copyLength;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetDateTime(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            // PostgreSQL binary format: microseconds since 2000-01-01 (Int64)
            var microseconds = BinaryPrimitives.ReadInt64BigEndian(span);
            return PostgresEpoch.AddTicks(microseconds * 10); // 1 microsecond = 10 ticks
        }
        // Text format
        Span<char> chars = stackalloc char[span.Length];
        var charCount = Encoding.UTF8.GetChars(span, chars);
        return DateTime.Parse(chars[..charCount], CultureInfo.InvariantCulture);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override decimal GetDecimal(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        // Postgre SQL numeric binary format is complex, so use text parsing
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out decimal value, out _);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override double GetDouble(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            var bits = BinaryPrimitives.ReadInt64BigEndian(span);
            return BitConverter.Int64BitsToDouble(bits);
        }
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out double value, out _);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override float GetFloat(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            var bits = BinaryPrimitives.ReadInt32BigEndian(span);
            return BitConverter.Int32BitsToSingle(bits);
        }
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out float value, out _);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetGuid(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            return new Guid(span);
        }
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out Guid value, out _);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override short GetInt16(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            return BinaryPrimitives.ReadInt16BigEndian(span);
        }
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out short value, out _);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetInt32(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            return BinaryPrimitives.ReadInt32BigEndian(span);
        }
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out int value, out _);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override long GetInt64(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        if (IsBinaryFormat(ordinal))
        {
            return BinaryPrimitives.ReadInt64BigEndian(span);
        }
        _ = System.Buffers.Text.Utf8Parser.TryParse(span, out long value, out _);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetString(int ordinal)
    {
        var span = GetValueSpan(ordinal);
        return Encoding.UTF8.GetString(span);
    }
}
#pragma warning restore CA1010
