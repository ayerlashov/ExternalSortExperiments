using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BigFileSorter.GeneralComponents
{
    internal static class CustomLineSort
    {
        public static void Sort(Span<LineData> data)
        {
            SortByFirstWordBytes(data);
            SortByRanges(data);
        }

        public static void SortByRanges(Span<LineData> data)
        {
            Span<byte> lastN = stackalloc byte[InlineBytes.SIZE];
            int from = 0;
            data[from].FirstWordBytes.Bytes.CopyTo(lastN);

            for (int i = 1; i < data.Length; i++)
            {
                var current = data[i].FirstWordBytes.Bytes;

                if (!current.SequenceEqual(lastN))
                {
                    current.CopyTo(lastN);

                    data[from..i]
                        .Sort((l, r) => l.CompareWithoutPrefix(r));

                    from = i;
                }
            }

            data[from..].Sort();
        }

        public static void SortByFirstWordBytes(Span<LineData> data)
        {
            const int KeyLength = InlineBytes.SIZE;

            var auxArray = ArrayPool<LineData>.Shared.Rent(data.Length);
            Span<LineData> aux = auxArray.AsSpan(0, data.Length);
            Span<int> counts = stackalloc int[KeyLength * 256];

            try
            {
                for (int i = 0; i < data.Length; i++)
                {
                    ref var line = ref data[i];
                    var bytes = line.FirstWordBytes;
                    for (int b = 0, offset = 0; b < KeyLength; b++, offset += 256)
                    {
                        counts[offset + bytes[b]]++;
                    }
                }

                for (int b = 0; b < KeyLength; b++)
                {
                    int acc = 0;
                    ref int byteCounts = ref counts[b * 256];
                    for (int byteIndex = 0; byteIndex < 256; byteIndex++)
                    {
                        int count = byteCounts;
                        byteCounts = acc;
                        acc += count;
                        byteCounts = ref Unsafe.Add(ref byteCounts, 1);
                    }
                }

                Span<LineData> source = data;
                Span<LineData> destination = aux;
                for (int pass = KeyLength - 1; pass >= 0; pass--)
                {
                    RadixPass(source, destination, counts.Slice(pass * 256, 256), pass);

                    var temp = destination;
                    destination = source;
                    source = temp;
                }

                if ((KeyLength & 1) == 1)
                    source.CopyTo(data);
            }
            finally
            {
                ArrayPool<LineData>.Shared.Return(auxArray);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void RadixPass(
                Span<LineData> source,
                Span<LineData> target,
                Span<int> countsForThisByte,
                int byteIndex)
            {
                for (int i = 0; i < source.Length; i++)
                {
                    ref var line = ref source[i];
                    byte key = line.FirstWordBytes[byteIndex];
                    target[countsForThisByte[key]++] = line;
                }
            }
        }
    }
}