using BigFileSorter.GeneralComponents;
using System.Runtime.CompilerServices;

namespace BigFileSorter.ExternalMergeSort
{
    internal class ExternalMergeSorter(
        string tempFolderPath,
        long blockByteThreshold,
        int mergeFactor = 40,
        int maxChunksInMemory = 4,
        int streamBufferSize = ExternalMergeSorter.DefaultBufferSize)
    {
        public const int DefaultBufferSize = 4 << 20;

        private readonly string _tempFilePrefix = Guid.NewGuid().ToString();
        private readonly string _tempFolderPath = tempFolderPath;
        private readonly long _blockByteThreshold = blockByteThreshold;
        private readonly int _streamBufferSize = streamBufferSize;
        private readonly int _mergeFactor = mergeFactor;
        private readonly int _maxChunksInMemory = maxChunksInMemory;

        public void Sort(
            string inputFilePath,
            string outputFilePath)
        {
            try
            {
                var chunkFilePaths = FirstPass(inputFilePath);

                Helpers.Log("Completed first pass.");
                Helpers.Log("Starting k-way merge.");

                var finalFilePath = MergeAllSortedChunks([..chunkFilePaths]);

                File.Move(finalFilePath, outputFilePath, true);
            }
            finally
            {
                foreach (var file in Directory.EnumerateFiles(_tempFolderPath, _tempFilePrefix + "*"))
                {
                    try { File.Delete(file); } catch { /* ignore */ }
                }
            }
        }

        private string[] FirstPass(string inputFilePath)
        {
            using var reader = new AsciiLineBytesFileStreamReader(inputFilePath);
            var currentChunk = new Chunk(_blockByteThreshold);
            var chunkTasks = new List<Task<string>>();
            var chunkWriteSemaphore = new SemaphoreSlim(_maxChunksInMemory);

            ReadOnlySpan<byte> line;
            while ((line = reader.ReadLine()) is not [])
            {
                if (!currentChunk.TryAdd(line))
                {
                    CompleteChunk(currentChunk);
                    chunkWriteSemaphore.Wait();

                    currentChunk = new Chunk(_blockByteThreshold);
                    currentChunk.Add(line);
                }
            }

            CompleteChunk(currentChunk);

            return Task.WhenAll(chunkTasks).GetAwaiter().GetResult();

            void CompleteChunk(Chunk chunk)
            {
                chunkTasks.Add(Task.Run(() => WriteChunkToFile(chunk, chunkWriteSemaphore)));
            }
        }

        private string WriteChunkToFile(Chunk chunk, SemaphoreSlim chunkWriteSemaphore)
        {
            try
            {
                chunk.SortChunk();

                FileStream outputStream;
                using (outputStream = GetTempFileStream())
                {
                    chunk.WriteChunk(outputStream);
                }

                return outputStream.Name;
            }
            finally
            {
                chunk.Dispose();
                chunkWriteSemaphore.Release();
            }
        }

        private string MergeAllSortedChunks(List<string> sortedChunkFilePaths)
        {
            List<string> currentMergeFiles = sortedChunkFilePaths;

            var chunkSize = (int)Math.Ceiling(currentMergeFiles.Count / (double)_mergeFactor);
            if (chunkSize == 1)
                chunkSize = _mergeFactor;
            int pass = 1;

            while (currentMergeFiles.Count > 1)
            {
                Helpers.Log($"Starting K-way merge pass {pass++}. File count: {currentMergeFiles.Count}. " +
                    $"Chunk size: {chunkSize}. Merge factor: {_mergeFactor}. " +
                    $"Processor count: {Environment.ProcessorCount}");
                currentMergeFiles = currentMergeFiles
                    .Chunk(chunkSize)
                    .AsParallel()
                    .WithDegreeOfParallelism(Math.Min(_mergeFactor, Environment.ProcessorCount))
                    .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                    .Select(MergeSortedFiles)
                    .AsUnordered()
                    .ToList();

                chunkSize = _mergeFactor;
            }

            return currentMergeFiles.First();
        }

        private string MergeSortedFiles(string[] sortedChunkFilePaths)
        {
            if (sortedChunkFilePaths.Length == 1)
                return sortedChunkFilePaths[0];

            var readers = new List<AsciiLineBytesFileStreamReader>(sortedChunkFilePaths.Length);

            try
            {
                foreach (var file in sortedChunkFilePaths)
                {
                    readers.Add(new AsciiLineBytesFileStreamReader(file));
                }

                var queue = new LoserTree<LineData>(readers.Select(r => r.EnumerateLines()).ToArray());

                using var writer = GetTempFileStream();
                
                MergeChunks(readers, queue, writer);

                return writer.Name;
            }
            finally
            {
                foreach (var reader in readers)
                {
                    reader.Dispose();
                }

                foreach (var filePath in readers.Select(r => r.Name))
                {
                    Task.Run(() => File.Delete(filePath));
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        private static void MergeChunks(List<AsciiLineBytesFileStreamReader> readers, LoserTree<LineData> queue, FileStream writer)
        {
            Span<byte> numberBuffer = stackalloc byte[20];

            while (queue.TryGetMin() is (true, { } value))
            {
                value.WriteTo(writer, numberBuffer);
                value.Dispose();
            }
        }

        private string GetRandomFilePath() =>
            Path.Combine(_tempFolderPath, $"{_tempFilePrefix}_{Guid.NewGuid()}.txt");

        private FileStream GetTempFileStream() =>
            GetOutputStream(GetRandomFilePath());

        private FileStream GetOutputStream(string outputFilePath) =>
            new(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read, _streamBufferSize, FileOptions.None);
    }
}