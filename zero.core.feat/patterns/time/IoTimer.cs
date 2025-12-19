using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

namespace zero.core.feat.patterns.time;

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

                        while (!token.IsCancellationRequested)
                            try
                            {
                                await Task.Delay((int)delta.TotalMilliseconds, token);

                                // Only set result if not cancelled and if signal is ready for a new result
                                if (!token.IsCancellationRequested)
                                {
                                    // Only set result if the signal is in a pending state (waiting for result)
                                    // This prevents setting result when there's no waiter or when already completed
                                    var status = signal.GetStatus(0);
                                    if (status == ValueTaskSourceStatus.Pending)
                                        signal.SetResult(Environment.TickCount);
                                    // If status is not pending, skip this tick - the consumer hasn't consumed the previous result yet
                                    // AutoReset will handle the reset when GetResult is called
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // Expected when cancellation is requested, don't try to set exception
                                break;
                            }
                            catch (Exception e)
                            {
                                // Only set exception if the signal is in a pending state
                                if (signal.GetStatus(0) == ValueTaskSourceStatus.Pending) signal.SetException(e);
                                // Log the error but continue the timer loop
                                LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoTimer)}: Timer loop error");
                            }
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoTimer)}: Fatal timer error");
                    }
                }, (delta, signal, token), CancellationToken.None, TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);
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

    public ValueTask<int> TickAsync()
    {
        return new ValueTask<int>(_signal, 0);
    }
}