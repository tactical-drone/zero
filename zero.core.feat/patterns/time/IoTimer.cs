﻿using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

namespace zero.core.feat.patterns.time
{

    public class IoTimer : IIoTimer
    {
        static IoTimer()
        {
            _make = static (delta, signal, token) =>
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    try
                    {
                        var (delta, signal, token) =
                            (ValueTuple<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken>)state;
                        signal.RunContinuationsAsynchronouslyAlways = true;
                        while (!token.IsCancellationRequested)
                        {
                            try
                            {
                                await Task.Delay((int)delta.TotalMilliseconds, token);
                                signal.SetResult(Environment.TickCount);
                            }
                            catch
                            {
                                // ignored
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoTimer)}:");
                    }

                }, (delta, signal, token), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
#pragma warning disable VSTHRD101 // Avoid unsupported async delegates
//                var t = new Thread(static state =>
//                {
//                    try
//                    {
//                        var (delta, signal, token) = (ValueTuple<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken>)state;
//                        signal.RunContinuationsAsynchronouslyAlways = true;
//                        while (!token.IsCancellationRequested)
//                        {
//                            try
//                            {
//                                await Task.Delay((int)delta.TotalMilliseconds, token);
//                                signal.SetResult(Environment.TickCount);
//                            }
//                            catch
//                            {
//                                // ignored
//                            }
//                        }
//                    }
//                    catch (Exception e)
//                    {
//                        LogManager.GetCurrentClassLogger().Error(e,$"{nameof(IoTimer)}:");
//                    }
                    
//                });
//#pragma warning restore VSTHRD101 // Avoid unsupported async delegates

//                t.Start((delta, signal, token));
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
            _signal = new IoZeroResetValueTaskSource<int>(true);
            _make(timeout, _signal, token);
        }

        public ValueTask<int> TickAsync() => new(_signal, 0);
        

        public void Reset() => _signal.Reset();
        
    }
}
