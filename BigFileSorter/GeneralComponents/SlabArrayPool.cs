using System.Collections.Concurrent;

namespace BigFileSorter.GeneralComponents
{
    internal class SlabArrayPool
    {
        public static SlabArrayPool Shared = new();

        private readonly ConcurrentStack<Slab> _available = new();
        private readonly int _slabSize;
        private readonly Lock _lock = new();
        private volatile Slab _current;

        public SlabArrayPool(int slabSize = 128 << 20)
        {
            _slabSize = slabSize;
            _current = new(slabSize, this);
        }

        public SlabSegment Rent(int size)
        {
            if (size == 0)
                return default;

            if (size < 0 || size > _slabSize)
                throw new ArgumentOutOfRangeException(nameof(size));

            while (true)
            {
                var slab = _current;

                if (slab.TryRent(size, out var seg))
                    return seg;

                SetFreshSlab(slab);
            }
        }

        public SlabSegment From(ReadOnlySpan<byte> source)
        {
            var result = Rent(source.Length);

            source.CopyTo(result.Span);

            return result;
        }

        private void SetFreshSlab(Slab slab)
        {
            if (slab != _current)
                return;

            if (_lock.TryEnter())
            {
                try
                {
                    if (slab != _current)
                        return;

                    if (!_available.TryPop(out var next))
                        next = new Slab(_slabSize, this);

                    _current = next;
                }
                finally
                {
                    _lock.Exit();
                }
            }
        }

        internal void MakeAvailable(Slab slab)
        {
            _available.Push(slab);
        }
    }
}