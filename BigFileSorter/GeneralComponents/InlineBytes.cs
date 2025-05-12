using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BigFileSorter.GeneralComponents
{
    [InlineArray(SIZE)]
    struct InlineBytes
    {
        public const int SIZE = 8;
        private byte _element0;
        public Span<byte> Bytes => MemoryMarshal.CreateSpan(ref _element0, SIZE);
    }
}