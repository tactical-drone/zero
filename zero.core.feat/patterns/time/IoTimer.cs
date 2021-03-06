using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;

namespace zero.core.feat.patterns.time
{

    public class IoTimer : IoNanoprobe, IIoTimer
    {
        static IoTimer()
        {
            _make = static (delta, signal, token) =>
            {
#pragma warning disable VSTHRD101
                var t = new Thread(static async state =>
                {
                    var (delta, signal, token) = (ValueTuple<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken>)state;

                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay((int)delta.TotalMilliseconds, token);
                            signal.SetResult(Environment.TickCount);
                        }
                        catch 
                        {
                            signal.Reset();
                        }
                    }
                });
#pragma warning restore VSTHRD101

                t.Start((delta, signal, token));
            };
        }

        public static void Make(Action<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken> make)
        {
            Volatile.Write(ref _make, make);
        }

        public IIoTimer Shared;
        private static Action<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken> _make;
        private readonly IIoManualResetValueTaskSourceCore<int> _signal;

        public IoTimer(TimeSpan timeout, CancellationToken token = default)
        {
            _signal = new IoZeroResetValueTaskSource<int>();
            IoTimer._make(timeout, _signal, token);
        }

        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync();
        }

        public ValueTask<int> TickAsync() => _signal.WaitAsync();


        public void Reset() => _signal.Reset();
        
    }
}
