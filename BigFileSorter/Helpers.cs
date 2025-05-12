using BigFileSorter.GeneralComponents;
using System.Runtime.CompilerServices;
using System.Text;

namespace BigFileSorter
{
    internal static class Helpers
    {
        public static IEnumerable<(long ln, string previous, string line)> Verify(string path)
        {
            using var reader = new AsciiLineBytesFileStreamReader(path);

            foreach(var n in Verify(reader.EnumerateLines()))
                yield return n;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IEnumerable<LineData> EnumerateLines(this AsciiLineBytesFileStreamReader reader, int maxLineBufferSize = 5<<10)
        {
            bool finished = false;
            var queue = new Queue<LineData>(maxLineBufferSize);
            var nextQueue = new Queue<LineData>(maxLineBufferSize);
            int lineBufferSize = 10;

            Task<bool> nextQueueTask = Task.Run(() => PopulateQueue(reader, lineBufferSize, nextQueue));

            while (!finished)
            {
                lineBufferSize = lineBufferSize < maxLineBufferSize ? lineBufferSize << 1 : maxLineBufferSize;
                finished = nextQueueTask.GetAwaiter().GetResult();

                (queue, nextQueue) = (nextQueue, queue);
                nextQueueTask = Task.Run(() => PopulateQueue(reader, lineBufferSize, nextQueue));

                while (queue.TryDequeue(out var line))
                {
                    yield return line;
                }
            }

            static bool PopulateQueue(AsciiLineBytesFileStreamReader reader, int lineBufferSize, Queue<LineData> queue)
            {
                while (queue.Count < lineBufferSize)
                {
                    if (reader.ReadLine() is not [_, ..] lineBytes)
                        return true;

                    queue.Enqueue(new LineData(lineBytes));
                }

                return false;
            }
        }

        public static IEnumerable<(long ln, string previous, string line)> Verify(this IEnumerable<LineData> lineEnumeration)
        {
            string? previous = default;
            long lineNumber = 0;

            foreach (var line in lineEnumeration)
            {
                var current = line.ToString();
                current = current[(current.IndexOf(' ') + 1)..];

                if (string.CompareOrdinal(previous, current) > 0)
                {
                    yield return (lineNumber, previous ?? "", current);
                }

                previous = current;
                lineNumber++;
                line.Dispose();
            }

            yield return (-1, "", "");
        }

        public static int Verify(this Span<LineData> data)
        {
            var previous = data[0];
            for (int i = 1; i < data.Length; i++)
            {
                var current = data[i];
                if (previous.CompareTo(current) > 0)
                {
                    return i;
                }
            }
            return -1;
        }

        public static List<string> GetStrings(this Span<LineData> data)
        {
            var res = new List<string>();

            for (int i = 0; i < data.Length; i++)
            {
                res.Add(Encoding.ASCII.GetString(data[i].RemainingWordBytes.Span));
            }

            return res;
        }

        public static void Log(string message)
        {
            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {message}");
        }
    }
}