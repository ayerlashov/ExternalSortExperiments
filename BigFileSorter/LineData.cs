using BigFileSorter.GeneralComponents;
using System.Buffers.Text;
using System.Runtime.CompilerServices;

namespace BigFileSorter
{
    internal readonly struct LineData : IComparable<LineData>, IDisposable
    {
        public LineData(ReadOnlySpan<byte> line)
        {
            var dotIndex = line.IndexOf((byte)'.');
            Num = long.Parse(line[..dotIndex]);
            
            line = line[(dotIndex + 2)..];

            FirstWordBytes.Bytes.Clear();

            var firstWordBytesLimit = Math.Min(line.Length, (ushort)FirstWordBytes.Bytes.Length);
            line[..firstWordBytesLimit].CopyTo(FirstWordBytes.Bytes);

            _words = SlabArrayPool.Shared.From(line[firstWordBytesLimit..]);
            RemainingWordBytes = _words.Memory;

            line[firstWordBytesLimit..].CopyTo(RemainingWordBytes.Span);
        }

        private readonly SlabSegment _words;
        public readonly Memory<byte> RemainingWordBytes;
        public readonly long Num;
        public readonly InlineBytes FirstWordBytes;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int CompareTo(LineData other)
        {
            var a = FirstWordBytes;
            var b = other.FirstWordBytes;
            int cmp = a.Bytes.SequenceCompareTo(b.Bytes);
            if (cmp != 0) return cmp;

            cmp = RemainingWordBytes.Span.SequenceCompareTo(other.RemainingWordBytes.Span);
            if (cmp != 0) return cmp;

            long n1 = Num, n2 = other.Num;
            if (n1 < n2) return -1;
            if (n1 > n2) return 1;
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareWithoutPrefix(LineData other)
        {
            int cmp = RemainingWordBytes.Span.SequenceCompareTo(other.RemainingWordBytes.Span);
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
            var i = FirstWordBytes.Bytes.IndexOf((byte)0);
            stream.Write(FirstWordBytes.Bytes[..(i < 0 ? FirstWordBytes.Bytes.Length : i)]);
            stream.Write(RemainingWordBytes.Span);
            stream.WriteByte((byte)'\n');
        }

        public override string ToString()
        {
            using var m = new MemoryStream(1050);
            using var r = new StreamReader(m);
            Span<byte> bytes = stackalloc byte[20];
            WriteTo(m, bytes);
            m.Position = 0;
            return r.ReadToEnd();
        }

        public void Dispose()
        {
            _words.Dispose();
        }
    }
}