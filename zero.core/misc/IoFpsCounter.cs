using System;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

namespace zero.core.misc
{
    /// <summary>
    /// A basic fps counter
    /// </summary>
    public struct IoFpsCounter
    {
        /// <summary>
        /// ConstructAsync
        /// </summary>
        /// <param name="range">The initial range hysteresis</param>
        /// <param name="time">The time in ms hysteresis</param>
        public IoFpsCounter(int range = 250, int time = 5000, int maxConcurrency = 3)
        {
            _range = new[] {range, range};
            _time = time;
            _count = new int[2];
            _timeStamp = new []{ DateTime.Now, DateTime.Now };
            _index = 0;
            _total = 0;
            _asyncTasks = new CancellationTokenSource();
            _mutex = new IoZeroSemaphore($"{nameof(IoFpsCounter)}", maxConcurrency, 1);
            _mutex.ZeroRef(ref _mutex, _asyncTasks.Token);
        }

        private readonly int[] _count;
        private readonly DateTime[] _timeStamp;
        private volatile int _index;
        private readonly int[] _range;
        private long _total;
        private readonly int _time;
        private readonly IIoZeroSemaphore _mutex;
        private readonly CancellationTokenSource _asyncTasks;

        /// <summary>
        /// Increment count
        /// </summary>
        public async ValueTask TickAsync()
        {
            Interlocked.Increment(ref _count[_index]);
            Interlocked.Increment(ref _total);

            if (Volatile.Read(ref _count[_index]) < Volatile.Read(ref _range[_index])) return;

            var m = _mutex.WaitAsync();
            if(await m.FastPath().ConfigureAwait(false))
            {
                _range[_index] = (int)(_time * _time * Fps() / 1000000);
                Interlocked.Increment(ref _index);
                _index %= 2;
                Volatile.Write(ref _count[_index], 0);
                _timeStamp[_index] = DateTime.Now;
                _mutex.Release();
            }            
        }

        /// <summary>
        /// ReturnAsync current fps
        /// </summary>
        /// <returns></returns>
        public double Fps()
        {
            var fps =  Volatile.Read(ref _count[_index]) / (DateTime.Now - _timeStamp[_index]).TotalSeconds * Volatile.Read(ref _count[_index]) / (Volatile.Read(ref _count[_index]) + Volatile.Read(ref _count[(_index + 1) % 2]))
                       + Volatile.Read(ref _count[(_index + 1) % 2]) / (DateTime.Now - _timeStamp[(_index + 1) % 2]).TotalSeconds * Volatile.Read(ref _count[(_index + 1) % 2]) / (Volatile.Read(ref _count[_index]) + Volatile.Read(ref _count[(_index + 1) % 2]));
                if (double.IsNaN(fps))
                    return 0;
                return fps;
        }

        /// <summary>
        /// Total frames
        /// </summary>
        public long Total => _total;

        public void Dispose()
        {
            _asyncTasks.Cancel();
        }
    }
}
