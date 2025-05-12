using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BigFileSorter.GeneralComponents
{
    internal readonly struct ByteMem : IDisposable
    {
        private readonly unsafe byte* _ptr;
        private readonly int length;

        public readonly Span<byte> Span
        {
            get
            {
                unsafe
                {
                    return new(_ptr, length);
                }
            }
        }

        public ByteMem(int length)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(length);

            unsafe
            {
                _ptr = (byte*)NativeMemory.Alloc((uint)length);
            }

            this.length = length;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public readonly void Dispose()
        {
            unsafe
            {
                if (_ptr == null)
                    return;
                NativeMemory.Free(_ptr);
            }
        }

        public static ByteMem From(ReadOnlySpan<byte> sourceBytes)
        {
            var res = new ByteMem(sourceBytes.Length);
            sourceBytes.CopyTo(res.Span);
            return res;
        }
    }
}