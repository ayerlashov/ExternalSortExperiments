using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BigFileSorter.GeneralComponents
{
    internal struct Chunk(long byteThreshold) : IDisposable
    {
        private readonly long _byteThreshold = byteThreshold;

        private LineData[] _chunkLines = new LineData[1 << 10];
        private int _itemCount = 0;
        private long _bytesRead;

        private readonly Span<LineData> LineSpan => _chunkLines.AsSpan(.._itemCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ReadOnlySpan<byte> line)
        {
            if (_itemCount == _chunkLines.Length - 1)
            {
                int newSize;
                checked
                {
                    var bytesPerItem = _bytesRead / _itemCount + 1;
                    newSize = (int)((_byteThreshold + _byteThreshold / 10) / bytesPerItem);
                }

                Array.Resize(ref _chunkLines, newSize);
            }

            _chunkLines[_itemCount++] = new LineData(line);
            _bytesRead += line.Length;
        }

        public bool TryAdd(ReadOnlySpan<byte> line)
        {
            if (_bytesRead > _byteThreshold)
                return false;

            Add(line);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void SortChunk()
        {
            CustomLineSort.Sort(LineSpan);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        public readonly void WriteChunk<TStream>(TStream stream)
            where TStream : Stream
        {
            Span<byte> numberBuffer = stackalloc byte[20];

            var lines = LineSpan;
            var lineCount = lines.Length;

            for (int i = 0; i < lineCount; i++)
            {
                var line = lines[i];

                line.WriteTo(stream, in numberBuffer);
            }
        }

        public void Dispose()
        {
            foreach (LineData item in LineSpan)
            {
                item.Dispose();
            }
            _chunkLines = null!;
        }
    }
}