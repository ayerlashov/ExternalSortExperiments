using System.Runtime.CompilerServices;

namespace BigFileSorter.GeneralComponents
{
    internal class BufferedLineSource : IDataSource<LineData>
    {
        private readonly AsciiLineBytesFileStreamReader _reader;
        private readonly int _maxLineBufferSize;
        private bool _isDisposed = false;
        private bool _finished = false;
        private Queue<LineData> _queue;
        private Queue<LineData> _nextQueue;
        private int _lineBufferSize = 10;
        private Task<bool> _nextQueueTask;

        public BufferedLineSource(AsciiLineBytesFileStreamReader reader, int maxLineBufferSize = 5 << 10)
        {
            _reader = reader;
            _maxLineBufferSize = maxLineBufferSize;
            _queue = new(_maxLineBufferSize);
            _nextQueue = new(_maxLineBufferSize);
            _nextQueueTask = Task.Run(() => PopulateQueue(reader, _lineBufferSize, _nextQueue));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (bool isEmpty, LineData line) Next()
        {
            if (_finished)
                return default;

            if (_queue.TryDequeue(out var line))
                return (true, line);

            SwapQueue();

            if (_queue.TryDequeue(out line))
                return (true, line);

            _finished = true;
            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SwapQueue()
        {
            _lineBufferSize = _lineBufferSize < _maxLineBufferSize
                ? _lineBufferSize << 1
                : _maxLineBufferSize;

            _finished = _nextQueueTask.GetAwaiter().GetResult();

            (_queue, _nextQueue) = (_nextQueue, _queue);
            _nextQueueTask = Task.Run(() => PopulateQueue(_reader, _lineBufferSize, _nextQueue));
        }

        private static bool PopulateQueue(AsciiLineBytesFileStreamReader reader, int lineBufferSize, Queue<LineData> queue)
        {
            while (queue.Count < lineBufferSize)
            {
                if (reader.ReadLine() is not [_, ..] lineBytes)
                    return true;

                queue.Enqueue(new LineData(lineBytes));
            }

            return false;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, true))
                return;

            _reader.Dispose();
        }
    }
}