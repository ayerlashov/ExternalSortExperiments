using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace TestDataGenerator
{
    internal class TestDataGenerator
    {
        private const int MaxNumberLength = 20;
        private const int SeparatorLength = 2;
        private const int MaxWordsPartLength = 1024;
        private const int NewLineLength = 1;
        private const int TotalMaxLength =
            MaxNumberLength
            + SeparatorLength
            + MaxWordsPartLength
            + NewLineLength;

        private readonly Random _random = new();
        private readonly TestDataGeneratorConfiguration _config;
        private readonly (byte[] bytes, int len)[] _wordsAsBytes;
        private readonly RandomSentenceLengthGenerator _sentenceLengthGenerator;

        public TestDataGenerator(TestDataGeneratorConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration, nameof(configuration));

            _config = configuration;

            ThrowIfConfigIsInvalid();

            _wordsAsBytes = _config.DictionaryWords
                .Select(Encoding.UTF8.GetBytes)
                .Select(w => (w, w.Length))
                .ToArray();

            var averageWordLength = (int)double.Round(_config.DictionaryWords.Average(w => w.Length));
            var maxSentenceLength = Math.Min(100, 1024 / (averageWordLength + 1));

            _sentenceLengthGenerator = new RandomSentenceLengthGenerator(maxSentenceLength);
        }

        public void Generate(string filePath)
        {
            using var fs = new FileStream(
                filePath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 1 << 19,
                FileOptions.None);

            Fill(fs);
        }

        public void Fill(Stream targetStream)
        {
            Span<byte> buffer = stackalloc byte[TotalMaxLength];

            if (_config.DataLimit.Type == LimitType.RowCount)
            {
                var rowCount = _config.DataLimit.Value;

                for (long i = 0; i < rowCount; i++)
                {
                    var index = WriteRow(buffer);

                    targetStream.Write(buffer[..index]);
                }
            }
            else if (_config.DataLimit.Type == LimitType.Bytes)
            {
                var byteLimit = _config.DataLimit.Value;

                while (targetStream.Position < byteLimit)
                {
                    var index = WriteRow(buffer);

                    targetStream.Write(buffer[..index]);
                }
            }
        }

        private int WriteRow(Span<byte> buffer)
        {
            int index = WriteNumberPart(buffer);
            index += WriteSeparator(buffer[index ..]);
            index += WriteWordsPart(buffer[index ..]);
            buffer[index++] = (byte)'\n';

            return index;
        }

        [SkipLocalsInit]
        private int WriteNumberPart(Span<byte> buffer)
        {
            Span<byte> numberBuffer = stackalloc byte[8];

            _random.NextBytes(numberBuffer);
            var number = MemoryMarshal.Read<long>(numberBuffer);
            Utf8Formatter.TryFormat(number, buffer, out int bytesWritten);

            return bytesWritten;
        }

        private static int WriteSeparator(Span<byte> buffer)
        {
            buffer[0] = (byte)'.';
            buffer[1] = (byte)' ';

            return SeparatorLength;
        }

        private int WriteWordsPart(Span<byte> buffer)
        {
            var sentenceLength = _sentenceLengthGenerator.Next();
            var words = _config.DictionaryWords;
            var wordCount = words.Count;

            var randNum = _random.Next(wordCount);

            var (wordBytes, writtenBytes) = _wordsAsBytes[randNum];
            wordBytes.CopyTo(buffer);
            buffer[0] ^= (byte)(randNum & (1 << 5));
            buffer = buffer[writtenBytes..];

            for (int i = 1; i < sentenceLength; i++)
            {
                randNum = _random.Next(wordCount);
                (wordBytes, var length) = _wordsAsBytes[randNum];
                int addedLength = length + 1;

                writtenBytes += addedLength;
                if (writtenBytes > MaxWordsPartLength)
                {
                    writtenBytes -= addedLength;

                    break;
                }

                buffer[0] = (byte)' ';
                wordBytes.CopyTo(buffer[1..]);
                buffer[1] ^= (byte)(randNum & (1 << 5));
                buffer = buffer[addedLength..];
            }

            return writtenBytes;
        }

        internal void ThrowIfConfigIsInvalid()
        {
            const long MinByteLength = TotalMaxLength * 2;

            switch (_config)
            {
                case { DataLimit: { Type: LimitType.RowCount, Value: < 2 } }:
                    throw new ArgumentOutOfRangeException(
                        nameof(TestDataGeneratorConfiguration.DataLimit),
                        "Row count limit may not be less than 2.");

                case { DataLimit: { Type: LimitType.Bytes, Value: < MinByteLength } }:
                    throw new ArgumentOutOfRangeException(
                        nameof(TestDataGeneratorConfiguration.DataLimit),
                        $"Byte count limit may not be less than {TotalMaxLength} * 2 = {MinByteLength}.");

                case { DataLimit.Type: { } type }
                when type is not LimitType.Bytes or LimitType.RowCount:
                    throw new ArgumentOutOfRangeException(
                        nameof(TestDataGeneratorConfiguration.DataLimit),
                        $"Encountered unsupported data limit type: {type}."
                        );

                case { DictionaryWords: [] }:
                    throw new ArgumentException("Set must have at least one word.", nameof(_config.DictionaryWords));

                case { DictionaryWords: { } words }
                when words.Any(string.IsNullOrWhiteSpace):
                    throw new ArgumentException(
                        "Set must not have null, empty of whitespace entries.",
                        nameof(_config.DictionaryWords));

                case { DictionaryWords: { } words }
                when words.Any(w => w.Length > MaxWordsPartLength):
                    throw new ArgumentException(
                        $"No word should be larger than ({MaxWordsPartLength})",
                        nameof(_config.DictionaryWords));

                default:
                    break;
            }
        }
    }
}