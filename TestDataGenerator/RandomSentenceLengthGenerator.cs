namespace TestDataGenerator
{
    /// <remarks>
    /// CDF formula via https://math.wvu.edu/~hdiamond/Math222F17/Sigurd_et_al-2004-Studia_Linguistica.pdf
    /// </remarks>
    internal class RandomSentenceLengthGenerator
    {
        private const double _c = 0.78; // 0.9 gives the peak in the paper
        private readonly double[] _cdf;
        private readonly Random _random = new();

        public RandomSentenceLengthGenerator(int maxWordCount = 150)
        {
            var weights = GetWeights(maxWordCount);
            _cdf = NormalizeToCdf(weights);
        }

        private double[] NormalizeToCdf(double[] weights)
        {
            var length = weights.Length;
            var cdf = new double[length];
            var total = weights.Sum();
            var acc = 0.0;

            for (int i = 0; i < length; i++)
            {
                acc += weights[i];
                cdf[i] = acc / total;
            }

            return cdf;
        }

        private static double[] GetWeights(int maxWordCount)
        {
            var weights = new double[maxWordCount];

            for (int L = 1; L <= maxWordCount; L++)
            {
                weights[L - 1] = L * Math.Pow(_c, L);
            }

            return weights;
        }

        public int Next()
        {
            var randVal = _random.NextDouble();

            var i = Array.BinarySearch(_cdf, randVal);

            var length = (i < 0 ? ~i : i) + 1;

            return length;
        }
    }
}