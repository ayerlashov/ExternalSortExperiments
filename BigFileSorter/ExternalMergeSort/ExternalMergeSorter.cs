using System.Runtime.CompilerServices;

namespace BigFileSorter.ExternalMergeSort
{
    internal class ExternalMergeSorter(
        string tempFolderPath,
        long blockByteThreshold,
        int streamBufferSize = ExternalMergeSorter.DefaultBufferSize,
        int openChunkFileCountLimit = 20)
    {
        public const int DefaultBufferSize = 1 << 20;
        private readonly string _tempFilePrefix = Guid.NewGuid().ToString();
        private readonly string _tempFolderPath = tempFolderPath;
        private readonly long _blockByteThreshold = blockByteThreshold;
        private readonly int _streamBufferSize = streamBufferSize;
        private readonly int _openChunkFileCountLimit = openChunkFileCountLimit;

        public void Sort(
            string inputFilePath,
            string outputFilePath)
        {
            try
            {
                var chunkFilePaths = FirstPass(inputFilePath);
                
                Console.WriteLine("Completed first pass.");

                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true, true);

                Console.WriteLine("Starting k-way merge.");

                var finalFilePath = MergeAllSortedChunks(chunkFilePaths);

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
            var chunkTaskSemaphore = new SemaphoreSlim(_openChunkFileCountLimit);

            ReadOnlySpan<byte> line;
            while ((line = reader.ReadLine()) is not [])
            {
                if (!currentChunk.TryAdd(line))
                {
                    CompleteChunk(currentChunk);

                    currentChunk = new Chunk(_blockByteThreshold);
                    currentChunk.Add(line);
                }
            }

            CompleteChunk(currentChunk);

            return Task.WhenAll(chunkTasks).GetAwaiter().GetResult();

            void CompleteChunk(Chunk chunk)
            {
                chunkTasks.Add(Task.Run(() => WriteChunkToFile(chunk, chunkTaskSemaphore)));
            }
        }

        private string WriteChunkToFile(Chunk chunk, SemaphoreSlim chunkTaskSemaphore)
        {
            try
            {
                chunkTaskSemaphore.Wait();

                FileStream outputStream;
                using (outputStream = GetRandomFileStream())
                {
                    chunk.WriteSortedChunk(outputStream);
                }

                return outputStream.Name;
            }
            finally
            {
                chunkTaskSemaphore.Release();
            }
        }

        private string MergeAllSortedChunks(IReadOnlyCollection<string> sortedChunkFilePaths)
        {
            var readerStreamLimit = _openChunkFileCountLimit - 1;
            IReadOnlyCollection<string> currentMergeFiles = sortedChunkFilePaths;

            while (currentMergeFiles.Count > 1)
            {
                int remainingFileCount = currentMergeFiles.Count;
                var nextFiles = new List<string>();

                foreach (var set in sortedChunkFilePaths.Chunk(readerStreamLimit))
                {
                    remainingFileCount -= set.Length;

                    if (remainingFileCount + nextFiles.Count <= readerStreamLimit)
                    {
                        return MergeSortedFiles([..nextFiles, ..set]);
                    }

                    nextFiles.Add(MergeSortedFiles(set));
                }

                currentMergeFiles = nextFiles;
            }

            return currentMergeFiles.First();
        }

        private string MergeSortedFiles(string[] sortedChunkFilePaths)
        {
            var readers = new List<AsciiLineBytesFileStreamReader>(sortedChunkFilePaths.Length);

            try
            {
                foreach (var file in sortedChunkFilePaths)
                {
                    readers.Add(new AsciiLineBytesFileStreamReader(file, _streamBufferSize));
                }

                var queue = InitQueue(readers);

                using var writer = GetRandomFileStream();
                
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
                    try { File.Delete(filePath); } catch { /* ignore */ }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        private static void MergeChunks(List<AsciiLineBytesFileStreamReader> readers, PriorityQueue<(LineData Line, int Index), LineData> queue, FileStream writer)
        {
            Span<byte> numberBuffer = stackalloc byte[20];
            while (queue.Count > 0)
            {
                var (smallestLine, sourceIndex) = queue.Dequeue();
                smallestLine.WriteTo(writer, numberBuffer);
                ReadInLine(readers, queue, sourceIndex);
            }
        }

        private static PriorityQueue<(LineData Line, int Index), LineData>  InitQueue(List<AsciiLineBytesFileStreamReader> readers)
        {
            var queue = new PriorityQueue<(LineData Line, int Index), LineData>();

            for (int i = 0; i < readers.Count; i++)
            {
                ReadInLine(readers, queue, i);
            }

            return queue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ReadInLine(List<AsciiLineBytesFileStreamReader> readers, PriorityQueue<(LineData Line, int Index), LineData> queue, int sourceIndex)
        {
            var lineBytes = readers[sourceIndex].ReadLine();
            if (lineBytes is not [])
            {
                var line = new LineData(lineBytes);
                queue.Enqueue((line, sourceIndex), line);
            }
        }

        private string GetRandomFilePath() =>
            Path.Combine(_tempFolderPath, $"{_tempFilePrefix}_{Guid.NewGuid()}.txt");

        private FileStream GetRandomFileStream() =>
            GetOutputStream(GetRandomFilePath());

        private FileStream GetOutputStream(string outputFilePath) =>
            new(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read, _streamBufferSize, FileOptions.None);
    }
}