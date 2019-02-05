using System;
using System.Threading;

namespace zero.core.misc
{
    /// <summary>
    /// A basic fps counter
    /// </summary>
    public class IoFpsCounter
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="range">The initial range hysteresis</param>
        /// <param name="time">The time in ms hysteresis</param>
        public IoFpsCounter(int range = 250, int time = 5000)
        {
            _range = new[] {range, range};
            _time = time;
        }

        private readonly int[] _count = new int[2];
        private readonly DateTime[] _timeStamp = { DateTime.Now, DateTime.Now };
        private volatile int _index = 0;
        private readonly int[] _range;
        private long _total;
        private readonly int _time;

        /// <summary>
        /// Increment count
        /// </summary>
        public void Tick()
        {
            Interlocked.Increment(ref _count[_index]);
            Interlocked.Increment(ref _total);

            if (Volatile.Read(ref _count[_index]) < Volatile.Read(ref _range[_index])) return;
            
            lock (this)
            {
                _range[_index] = (int)(_time * _time * Fps() / 1000000);
                Interlocked.Increment(ref _index);
                _index %= 2;
                Volatile.Write(ref _count[_index], 0);
                _timeStamp[_index] = DateTime.Now;
            }            
        }

        /// <summary>
        /// Return current fps
        /// </summary>
        /// <returns></returns>
        public double Fps()
        {
            lock(this)
                return  Volatile.Read(ref _count[_index]) / (DateTime.Now - _timeStamp[_index]).TotalSeconds * Volatile.Read(ref _count[_index]) / (Volatile.Read(ref _count[_index]) + Volatile.Read(ref _count[(_index + 1) % 2]))
                      + Volatile.Read(ref _count[(_index + 1) % 2]) / (DateTime.Now - _timeStamp[(_index + 1) % 2]).TotalSeconds * Volatile.Read(ref _count[(_index + 1) % 2]) / (Volatile.Read(ref _count[_index]) + Volatile.Read(ref _count[(_index + 1) % 2]));
        }

        /// <summary>
        /// Total frames
        /// </summary>
        public long Total => _total;

    }
}
