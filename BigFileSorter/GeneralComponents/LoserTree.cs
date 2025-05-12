using System.Runtime.CompilerServices;

namespace BigFileSorter.GeneralComponents
{
    class LoserTree<T, TSource>
        where T : notnull, IComparable<T>
        where TSource : IDataSource<T>
    {
        public TSource[] _sources;
        private readonly TournamentTreeNode[] _tree;
        public short _lastRowIndex;
        private TournamentTreeNode _winner;

        public LoserTree(TSource[] sources)
        {
            _sources = sources;

            var pow = Math.Ceiling(Math.Log(_sources.Length, 2));
            var lastRowLength = (int)Math.Pow(2, pow);
            var totalLength = (int)(Math.Pow(2, pow + 1) - 1);

            _lastRowIndex = (short)(totalLength - lastRowLength);

            _tree = new TournamentTreeNode[totalLength];
            Array.Fill(_tree, new TournamentTreeNode());

            //populate last row
            for (short i = 0; i < sources.Length; i++)
            {
                var source = _sources[i];

                if (source.Next() is (true, { } current))
                {
                    var player = new TournamentTreeNode(i, current);

                    _tree[_lastRowIndex + i] = player;
                }
            }

            //set winners
            for (int i = _tree.Length - 2; i > 0; i -= 2)
            {
                var a = _tree[i];
                var b = _tree[i + 1];

                _tree[i / 2] = a.DoesWin(b) ? a : b;
            }

            _winner = _tree[0];

            //set losers
            for (int i = 1; i < _tree.Length; i += 2)
            {
                var a = _tree[i];
                var b = _tree[i + 1];

                _tree[i / 2] = a.DoesWin(b) ? b : a;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (bool success, T? value) TryGetMin()
        {
            if (_winner.SourceIndex == -1)
                return default;

            var (sourceIndex, result) = _winner;

            TournamentTreeNode nextNode = _sources[sourceIndex].Next() is (true, { } current)
                ? new (sourceIndex, current)
                : new ();

            Insert(nextNode, sourceIndex + _lastRowIndex);

            return (true, result);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Insert(TournamentTreeNode newNode, int targetIndex)
        {
            int advanceeIndex = targetIndex;

            _tree[advanceeIndex] = newNode;

            var winner = newNode;

            while(advanceeIndex > 0)
            {
                advanceeIndex = (advanceeIndex - 1) / 2;

                ref TournamentTreeNode oldLooser = ref _tree[advanceeIndex];

                if (!winner.DoesWin(oldLooser))
                    (winner, oldLooser) = (oldLooser, winner);
            }

            _winner = winner;
        }

        readonly record struct TournamentTreeNode(short SourceIndex, T? Value)
        {
            public TournamentTreeNode() : this(-1, default) { }

            public bool DoesWin(TournamentTreeNode opponent) =>
                SourceIndex >= 0 && (opponent.SourceIndex < 0 || Value!.CompareTo(opponent.Value) < 0);
        }
    }
}
