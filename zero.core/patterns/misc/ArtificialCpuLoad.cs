using System;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// A simple spin wait that generates CPU load.
    /// </summary>
    class ArtificialCpuLoad
    {
        private static Random _random = new Random((int) DateTime.Now.Ticks);

        /// <summary>
        /// Randoms the load.
        /// </summary>
        /// <param name="probability">The probability load would be generated</param>
        /// <param name="ms">The time the load lasts.</param>
        public static void RandomLoad(int probability, int ms)
        {
            if( _random.Next(1000) < probability )
                Load(ms);
        }

        /// <summary>
        /// Generates a spin load for a period of time
        /// </summary>
        /// <param name="ms">The time to spin for</param>
        public static void Load(int ms)
        {
            var end = DateTime.Now + TimeSpan.FromMilliseconds(ms);
            while (DateTime.Now < end){}
        }
    }
}
