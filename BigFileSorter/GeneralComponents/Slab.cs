using System.Runtime.CompilerServices;

namespace BigFileSorter.GeneralComponents
{
    internal class Slab(int size, SlabArrayPool owner)
    {
        private readonly Memory<byte> _memory = GC.AllocateUninitializedArray<byte>(size);
        private readonly int _size = size;
        private int _refCount;
        private volatile int _freeIndex;
        private volatile bool _full;
        private readonly Lock _lock = new();
        private readonly SlabArrayPool _owner = owner;

        public bool TryRent(int requestedLength, out SlabSegment segment)
        {
            if (_full)
            {
                segment = default;
                return false;
            }

            int newLength = Interlocked.Add(ref _freeIndex, requestedLength);

            if (newLength > _size)
            {
                _full = true;
                Interlocked.Add(ref _freeIndex, -requestedLength);
                segment = default;
                return false;
            }

            Interlocked.Increment(ref _refCount);

            segment = new SlabSegment(this, _memory[(newLength - requestedLength)..newLength]);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release()
        {
            var count = Interlocked.Decrement(ref _refCount);

            if (count == 0 && _full)
            {
                lock (_lock)
                {
                    if (_refCount == 0 && _full)
                    {
                        Reset();

                        _owner.MakeAvailable(this);
                    }
                }
            }
            else if (count < 0)
            {
                throw new Exception($"Corrupt state. {_refCount} should never be less than zero.");
            }
        }

        public void Reset()
        {
            _freeIndex = 0;
            _full = false;
        }
    }
}