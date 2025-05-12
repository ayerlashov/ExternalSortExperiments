using BigFileSorter.GeneralComponents;
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

        public static IEnumerable<LineData> EnumerateLines(this AsciiLineBytesFileStreamReader reader, int? maxLineBufferSize = null)
        {
            var source = reader.GetBufferedLineSource(maxLineBufferSize);

            while (source.Next() is (true, { } line))
            {
                yield return line;
            }
        }

        public static BufferedLineSource GetBufferedLineSource(this AsciiLineBytesFileStreamReader reader, int? maxLineBufferSize = null)
        {
            return (maxLineBufferSize == null)
                ? new BufferedLineSource(reader)
                : new BufferedLineSource(reader, maxLineBufferSize.Value);
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