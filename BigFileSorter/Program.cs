using BigFileSorter.ExternalMergeSort;

namespace BigFileSorter
{
    public class Program
    {
        public static void Main(string[] args)
        {
            const int DefaultMegabyteThreshold = 100;
            const int DefaultOpenChunkFileCount = 10;

            Console.WriteLine("Expected positional args: \n" +
                "<source file path> - Which file to sort.\n" +
                "<output file path> - Where to write the sorted file.\n" +
                "<temp files directory path> - Where to write the temp files.\n" +
                $"<megabyte block size threshold> - How big first pass blocks can be. Must be greater than 0. Default: {DefaultMegabyteThreshold}.\n" +
                $"<open chunk file count limit> - How many chunk files can be worked on at the same time. Must be greater than 2. Default: {DefaultOpenChunkFileCount}.\n");

            if (args.Length is < 1 or > 5)
            {
                Console.WriteLine("Incorrect arg count.");
                return;
            }

            var sourceFilePath = args[0];
            if (!File.Exists(sourceFilePath))
            {
                Console.WriteLine("Source file does not exist.");
                return;
            }

            var outputFilePath = args[1];
            var outputFileDirectory = Path.GetDirectoryName(outputFilePath);
            if (string.IsNullOrWhiteSpace(outputFileDirectory))
            {
                Console.WriteLine("Output file path must have correct directory.");
                return;
            }

            Directory.CreateDirectory(outputFileDirectory);

            var tempFilesPath = args[2];

            Directory.CreateDirectory(tempFilesPath);

            long megabyteBlockSizeThreshold = DefaultMegabyteThreshold;
            if (args is [_, _, _, { } megabyteCountString, ..])
            {
                if (!long.TryParse(megabyteCountString, out megabyteBlockSizeThreshold))
                {
                    Console.WriteLine("Can't parse the megabyte block size threshold.");
                    return;
                }

                if (megabyteBlockSizeThreshold < 1)
                {
                    Console.WriteLine("The megabyte threshold must be greater than 0.");
                    return;
                }
            }

            int openChunkFileCountLimit = DefaultOpenChunkFileCount;
            if (args is [_, _, _, _, { } chunkCountString])
            {
                if (!int.TryParse(chunkCountString, out openChunkFileCountLimit))
                {
                    Console.WriteLine("Can't parse the chunk count limit.");
                    return;
                }

                if (openChunkFileCountLimit < 3)
                {
                    Console.WriteLine("The chunk count must be greater than 2.");
                    return;
                }
            }

            Console.WriteLine("Starting.");

            new ExternalMergeSorter(tempFilesPath, megabyteBlockSizeThreshold << 20, 1 << 20, openChunkFileCountLimit)
                .Sort(sourceFilePath, outputFilePath);

            Console.WriteLine("Completed!");
        }
    }
}