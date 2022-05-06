using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.runtime.scheduler;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>Provides the core logic for implementing a manual-reset <see cref="IValueTaskSource"/> or <see cref="IValueTaskSource{TResult}"/>.</summary>
    /// <typeparam name="TResult"></typeparam>
    [StructLayout(LayoutKind.Auto)]
    public struct IoManualResetValueTaskSourceCore<TResult>: IIoManualResetValueTaskSourceCore<TResult>
    {
        private static readonly InvalidOperationException _invalidOperationException = new();

        /// <summary>
        /// The callback to invoke when the operation completes if <see cref="OnCompleted"/> was called before the operation completed,
        /// or <see cref="ManualResetValueTaskSourceCoreShared.SSentinel"/> if the operation completed before a callback was supplied,
        /// or null if a callback hasn't yet been provided and the operation hasn't yet completed.
        /// </summary>
        private volatile Action<object> _continuation;
        /// <summary>State to pass to <see cref="_continuation"/>.</summary>
        private volatile object _continuationState;
        /// <summary><see cref="ExecutionContext"/> to flow to the callback, or null if no flowing is required.</summary>
        private volatile ExecutionContext _executionContext;
        /// <summary>
        /// A "captured" <see cref="SynchronizationContext"/> or <see cref="TaskScheduler"/> with which to invoke the callback,
        /// or null if no special context is required.
        /// </summary>
        private volatile object _capturedContext;
        /// <summary>Whether the current operation has completed.</summary>
        private volatile bool _completed;
        /// <summary>The result with which the operation succeeded, or the default value if it hasn't yet completed or failed.</summary>
        private TResult _result;
        /// <summary>The exception with which the operation failed, or null if it hasn't yet completed or completed successfully.</summary>
        private volatile ExceptionDispatchInfo _error;

#if DEBUG
        /// <summary>The current version of this value, used to help prevent misuse.</summary>
        private volatile int _version;
#endif

        /// <summary>Gets or sets whether to force continuations to run asynchronously.</summary>
        /// <remarks>Continuations may run asynchronously if this is false, but they'll never run synchronously if this is true.</remarks>
        public bool RunContinuationsAsynchronously { get; set; }

        /// <summary>
        /// Run continuations on the flowing scheduler, else on the default one
        /// </summary>
        public bool RunContinuationsNatively { get; set; }

        /// <summary>Resets to prepare for the next operation.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            // Reset/update state for the next use/await of this instance.
            _result = default;
            _error = null;
            _executionContext = null;
            _capturedContext = null;
            _continuationState = null;
            _continuation = null;
            _completed = false;

#if DEBUG
            if (Interlocked.Increment(ref _version) == short.MaxValue)
                _version = 0;
#endif
        }


        /// <summary>
        /// If this primitive has been cocked
        /// </summary>
        /// <returns>True if cocked, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Set(bool reset = false)
        {
            var cocked = _continuation == null || !_completed;

            if (!reset) return cocked;

            if (!cocked) Reset();

            return true;
        }

        /// <summary>Completes with a successful result.</summary>
        /// <param name="result">The result.</param>
        public void SetResult(TResult result)
        {
            if (_completed)
                throw _invalidOperationException;

            _result = result;
            SignalCompletion();
        }

        /// <summary>Completes with an error.</summary>
        /// <param name="error"></param>
        public void SetException(Exception error)
        {
            _error = ExceptionDispatchInfo.Capture(error);
            SignalCompletion();
        }

#if DEBUG 
        /// <summary>Gets the operation version.</summary>
        public int Version => _version;
#else
        /// <summary>Gets the operation version.</summary>
        public int Version => 9;
#endif

        /// <summary>Gets the status of the operation.</summary>
        /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
#if DEBUG
            ValidateToken(token);   
#endif
            return
                _continuation == null || !_completed ? ValueTaskSourceStatus.Pending :
                _error == null ? ValueTaskSourceStatus.Succeeded : 
                _error.SourceException is OperationCanceledException ? ValueTaskSourceStatus.Canceled : ValueTaskSourceStatus.Faulted;
        }

        /// <summary>Gets the result of the operation.</summary>
        /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TResult GetResult(short token)
        {
#if DEBUG
            ValidateToken(token);   
#endif
            if (!_completed)
            {
                throw _invalidOperationException;
            }

            _error?.Throw();
            return _result;
        }

        /// <summary>Schedules the continuation action for this operation.</summary>
        /// <param name="continuation">The continuation to invoke when the operation has completed.</param>
        /// <param name="state">The state object to pass to <paramref name="continuation"/> when it's invoked.</param>
        /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
        /// <param name="flags">The flags describing the behavior of the continuation.</param>
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
#if DEBUG
            if (continuation == null)
            {
                throw new ArgumentNullException(nameof(continuation));
            }

            try//TODO: is this a good idea?
            {
                ValidateToken(token);
            }
            catch
            {
                return;
            }
#endif

            try
            {
                if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
                {
                    _executionContext = ExecutionContext.Capture();
                }

                if (RunContinuationsNatively && (flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0)
                {
                    var sc = SynchronizationContext.Current;
                    if (sc != null && sc.GetType() != typeof(SynchronizationContext))
                    {
                        _capturedContext = sc;
                    }
                    else
                    { 
                        var ts = TaskScheduler.Current;
                        if (ts != TaskScheduler.Default)
                        {
                            _capturedContext = ts;
                        }
                    }
                }

                // We need to set the continuation state before we swap in the delegate, so that
                // if there's a race between this and SetResult/Exception and SetResult/Exception
                // sees the _continuation as non-null, it'll be able to invoke it with the state
                // stored here.  However, this also means that if this is used incorrectly (e.g.
                // awaited twice concurrently), _continuationState might get erroneously overwritten.
                // To minimize the chances of that, we check preemptively whether _continuation
                // is already set to something other than the completion sentinel.

                object oldContinuation = _continuation;
                if (oldContinuation == null)
                {
                    _continuationState = state;
                    oldContinuation = Interlocked.CompareExchange(ref _continuation, continuation, null);
                }

                if (oldContinuation != null)
                {
                    // Operation already completed, so we need to queue the supplied callback.
                    if (!ReferenceEquals(oldContinuation, ManualResetValueTaskSourceCoreShared.SSentinel))
                    {
                        //Console.WriteLine($"{oldContinuation}, {continuation}, {state}");
                        throw _invalidOperationException;
                    }

                    switch (_capturedContext)
                    {
                        case null:
                        {
                            Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Current);
                            break;
                        }
                        case SynchronizationContext sc:
                            sc.Post(s =>
                            {
                                var tuple = ((Action<object>, object))s;
                                tuple.Item1(tuple.Item2);
                            }, (continuation, state));
                            break;

                        case TaskScheduler ts:
                            Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
                            break;
                    }
                }
            }
            catch //(Exception e)
            {
                //Console.WriteLine(e);
                //throw;
            }
        }

#if DEBUG
        /// <summary>Ensures that the specified token matches the current version.</summary>
        /// <param name="token">The token supplied by <see cref="ValueTask"/>.</param>
        private void ValidateToken(int token)
        {
            if (token != _version)
            {
                throw _invalidOperationException;
            }
        }
#endif

        /// <summary>Signals that the operation has completed.  Invoked after the result or error has been set.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SignalCompletion()
        {
            if (_completed)
            {
                throw _invalidOperationException;
            }
            _completed = true;

            if (_continuation == null && Interlocked.CompareExchange(ref _continuation, ManualResetValueTaskSourceCoreShared.SSentinel, null) == null) 
                return;

            if (_executionContext != null)
            {
                ExecutionContext.Run(
                    _executionContext,
                    s => ((IoManualResetValueTaskSourceCore<TResult>)s).InvokeContinuation(),
                    this);
            }
            else
            {
                InvokeContinuation();
            }
        }

        /// <summary>
        /// Invokes the continuation with the appropriate captured context / scheduler.
        /// This assumes that if <see cref="_executionContext"/> is not null we're already
        /// running within that <see cref="ExecutionContext"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InvokeContinuation()
        {
            switch (_capturedContext)
            {
                case null:
                    if (RunContinuationsAsynchronously)
                    {
                        Task.Factory.StartNew(_continuation, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                    }
                    else
                    {
                        _continuation(_continuationState);
                    }
                    break;

                case SynchronizationContext sc:
                    sc.Post(s =>
                    {
                        var state = ((Action<object>, object))s;
                        state.Item1(state.Item2);
                    }, (_continuation, _continuationState));
                    break;

                case IoZeroScheduler zs:
                    if (RunContinuationsAsynchronously)
                    {
                        IoZeroScheduler.Zero.QueueCallback(_continuation, _continuationState);
                    }
                    else
                    {
                        try
                        {
                            _continuation(_continuationState);
                        }
                        catch (Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Error(e, $"InvokeContinuation.callback(): {_continuationState}");
                        }
                    }
                    break;
                case TaskScheduler ts:
                    Task.Factory.StartNew(_continuation, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
                    break;
            }
        }
    }

    internal static class ManualResetValueTaskSourceCoreShared // separated out of generic to avoid unnecessary duplication
    {
        internal static readonly Action<object> SSentinel = CompletionSentinel;
        private static void CompletionSentinel(object _) // named method to aid debugging
        {
            throw new InvalidOperationException($"{nameof(CompletionSentinel)} executed!");
        }
    }
}
