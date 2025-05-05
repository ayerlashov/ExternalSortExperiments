namespace TestDataGenerator
{
    internal class TestDataGeneratorConfiguration
    {
        public required Limit DataLimit { get; init; }
        public required IReadOnlyList<string> DictionaryWords { get; init => field = [.. value]; }
    }
}