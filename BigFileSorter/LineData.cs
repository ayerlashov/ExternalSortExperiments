using System.Buffers.Binary;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BigFileSorter
{
    internal readonly struct LineData : IComparable<LineData>
    {
        public LineData(ReadOnlySpan<byte> line)
        {
            var dotIndex = line.IndexOf((byte)'.');
            Num = long.Parse(line[..dotIndex]);

            Words = line[(dotIndex + 2)..].ToArray();

            _firstWordBytes = ReadUInt32(Words.Span);
        }

        public readonly ReadOnlyMemory<byte> Words;
        public readonly long Num;
        private readonly uint _firstWordBytes;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareTo(LineData other)
        {
            uint a = _firstWordBytes;
            uint b = other._firstWordBytes;

            if (a < b) return -1;
            if (a > b) return 1;

            int cmp = Words.Span.SequenceCompareTo(other.Words.Span);
            if (cmp != 0) return cmp;

            long n1 = Num, n2 = other.Num;
            if (n1 < n2) return -1;
            if (n1 > n2) return 1;
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteTo<TStream>(TStream stream, in Span<byte> numberBuffer)
            where TStream : Stream
        {
            Utf8Formatter.TryFormat(Num, numberBuffer, out int bytesWritten);

            stream.Write(numberBuffer[..bytesWritten]);
            stream.WriteByte((byte)'.');
            stream.WriteByte((byte)' ');
            stream.Write(Words.Span);
            stream.WriteByte((byte)'\n');
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        internal void WriteTo<TStream>(TStream stream)
            where TStream : Stream
        {
            Span<byte> numberBuffer = stackalloc byte[20];
            WriteTo(stream, numberBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint ReadUInt32(ReadOnlySpan<byte> sourceBytes)
        {
            uint result;

            if (sourceBytes.Length >= 4)
            {
                result = MemoryMarshal.Read<uint>(sourceBytes);
            }
            else
            {
                Span<byte> tmp = stackalloc byte[4];
                sourceBytes.CopyTo(tmp);

                result = MemoryMarshal.Read<uint>(tmp);
            }

            if (!BitConverter.IsLittleEndian)
            {
                result = BinaryPrimitives.ReverseEndianness(result);
            }

            return result;
        }
    }
}