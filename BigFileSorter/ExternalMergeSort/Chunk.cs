using System.Runtime.CompilerServices;

namespace BigFileSorter.ExternalMergeSort
{
    internal struct Chunk(long byteThreshold)
    {
        private readonly long _byteThreshold = byteThreshold;
        private readonly List<LineData> _chunkLines = [];

        private long _bytesRead;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ReadOnlySpan<byte> line)
        {
            _chunkLines.Add(new LineData(line));
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
        [SkipLocalsInit]
        public readonly void WriteSortedChunk<TStream>(TStream stream)
            where TStream : Stream
        {
            Span<byte> numberBuffer = stackalloc byte[20];

            _chunkLines.Sort();

            var lineCount = _chunkLines.Count;
            for (int i = 0; i < lineCount; i++)
            {
                _chunkLines[i].WriteTo(stream, in numberBuffer);
            }
        }
    }
}