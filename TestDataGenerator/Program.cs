namespace TestDataGenerator
{
    public class Program
    {
        static void Main(string[] args)
        {
            const int defaultMebibyteThreshold = 1_000;

            Console.WriteLine("Expected args: \n" +
                "<output file path> - Where to write the file.\n" +
                $"<mebibyte size threshold> - Must be greater than 0. Default: {defaultMebibyteThreshold}");

            if (args.Length is < 1 or > 2)
            {
                Console.WriteLine("Incorrect arg count.");
                return;
            }

            var path = args[0];
            var directory = Path.GetDirectoryName(path);
            if (string.IsNullOrWhiteSpace(directory))
            {
                Console.WriteLine("Path must have correct directory.");
                return;
            }

            Directory.CreateDirectory(directory);

            long mebibyteThreshold = defaultMebibyteThreshold;
            if (args is [ _, { } byteCountString ])
            {
                if (!long.TryParse(byteCountString, out mebibyteThreshold))
                {
                    Console.WriteLine("Can't parse the byteCount.");
                    return;
                }

                if (mebibyteThreshold < 1)
                {
                    Console.WriteLine("The mebibyte threshold must be greater than 0.");
                    return;
                }
            }

            long byteThreshold = mebibyteThreshold * 1_000_000;

            Console.WriteLine("Setting up!");

            var gen = new TestDataGenerator(
                new TestDataGeneratorConfiguration()
                { 
                    DataLimit = new (byteThreshold, LimitType.Bytes),
                    DictionaryWords = LoadWordListFromFile()
                });

            Console.WriteLine("Generating!");

            gen.Generate(path);

            Console.WriteLine("Done!");
        }

        private static List<string> LoadWordListFromFile()
        {
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "words_alpha.txt");
            return File.ReadAllLines(path)
                .Where(l => !string.IsNullOrWhiteSpace(l))
                .ToList();
        }
    }
}
