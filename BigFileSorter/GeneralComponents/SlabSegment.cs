namespace BigFileSorter.GeneralComponents
{
    internal struct SlabSegment : IDisposable
    {
        internal SlabSegment(Slab owner, Memory<byte> memory) { _owner = owner; Memory = memory; }

        public readonly Span<byte> Span => Memory.Span;
        public Memory<byte> Memory;
        private Slab? _owner;

        public void Dispose()
        {
            if (_owner == null)
                return;

            _owner.Release();

            _owner = null;
            Memory = default;
        }
    }
}