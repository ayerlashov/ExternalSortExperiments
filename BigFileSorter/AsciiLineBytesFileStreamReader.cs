using System.Buffers;

namespace BigFileSorter
{
    internal class AsciiLineBytesFileStreamReader : FileStream
    {
        public const int DefaultBufferSize = 1 << 20;
        private readonly int _bufferSize;
        private byte[] _rentedBuffer;
        private ReadOnlyMemory<byte> _unreadBytes = Array.Empty<byte>();
        private bool _endOfStreamReached;

        public AsciiLineBytesFileStreamReader(string path, int bufferSize = DefaultBufferSize)
            : base(
                  path,
                  FileMode.Open,
                  FileAccess.Read,
                  FileShare.Read,
                  bufferSize,
                  FileOptions.SequentialScan)
        {
            _rentedBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            _bufferSize = bufferSize;
        }

        public ReadOnlySpan<byte> ReadLine()
        {
            if (_endOfStreamReached)
                return [];

            bool secondLoop = false;

            while (true)
            {
                int newLineCharIndex;
                int offset = 0;

                if ((newLineCharIndex = _unreadBytes.Span.IndexOf((byte)'\n')) > -1)
                {
                    var result = _unreadBytes.Span[..newLineCharIndex];
                    _unreadBytes = _unreadBytes[(newLineCharIndex + 1)..];
                    return result;
                }
                else
                {
                    if (_unreadBytes.Length > 0)
                    {
                        if (_unreadBytes.Length == _bufferSize)
                            throw new Exception($"Line length exceeded buffer size. File path: {Name}");

                        _unreadBytes.CopyTo(_rentedBuffer);
                        offset = _unreadBytes.Length;
                    }

                    var bytesRead = Read(_rentedBuffer, offset, _bufferSize - offset);
                    _unreadBytes = _rentedBuffer.AsMemory()[..(bytesRead + offset)];

                    if (bytesRead == 0)
                    {
                        _endOfStreamReached = true;

                        return _unreadBytes.Span;
                    }

                }
                if (secondLoop)
                    throw new Exception("This loop should never execute more than twice");

                secondLoop = true;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            ArrayPool<byte>.Shared.Return(_rentedBuffer);
            _rentedBuffer = null!;
        }
    }
}
