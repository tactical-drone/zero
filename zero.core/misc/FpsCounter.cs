using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Xml.Schema;

namespace zero.core.misc
{
    /// <summary>
    /// A basic fps counter
    /// </summary>
    public class FpsCounter
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="range">The initial range hysteresis</param>
        /// <param name="time">The time hysteresis</param>
        public FpsCounter(int range = 250, int time = 5000)
        {
            _range = range;
            _time = time;
        }

        private readonly int[] _count = new int[2];
        private readonly DateTime[] _timeStamp = { DateTime.Now, DateTime.Now };
        private volatile int _index = 0;
        private int _range;
        private long _total;
        private int _time;

        /// <summary>
        /// Increment count
        /// </summary>
        public void Inc()
        {
            Interlocked.Increment(ref _count[_index]);
            Interlocked.Increment(ref _total);

            if (Volatile.Read(ref _count[_index]) < _range) return;

            _range = (int) (_time * Fps());

            Interlocked.Increment(ref _index);            
            _index %= 2;
            Volatile.Write(ref _count[_index], 0);
            _timeStamp[_index] = DateTime.Now;
        }

        /// <summary>
        /// Return current fps
        /// </summary>
        /// <returns></returns>
        public double Fps()
        {
            return  Volatile.Read(ref _count[_index]) / (DateTime.Now - _timeStamp[_index]).TotalSeconds * Volatile.Read(ref _count[_index]) / (Volatile.Read(ref _count[_index]) + Volatile.Read(ref _count[(_index + 1) % 2]))
                  + Volatile.Read(ref _count[(_index + 1) % 2]) / (DateTime.Now - _timeStamp[(_index + 1) % 2]).TotalSeconds * Volatile.Read(ref _count[(_index + 1) % 2]) / (_count[_index] + _count[(_index + 1) % 2]);
        }

        public long Total => _total;

    }
}
