using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.patterns.misc
{
    class ArtificialCpuLoad
    {
        private static Random _random = new Random((int) DateTime.Now.Ticks);

        public static void RandomLoad(int probability, int ms)
        {
            if( _random.Next(1000) < probability )
                Load(ms);
        }

        public static void Load(int ms)
        {
            var end = DateTime.Now + TimeSpan.FromMilliseconds(ms);
            while (DateTime.Now < end){}
        }
    }
}
