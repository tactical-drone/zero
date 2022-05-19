using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
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
        private Action<object> _continuation;

        /// <summary>State to pass to <see cref="_continuation"/>.</summary>
        private object _continuationState;

        /// <summary><see cref="ExecutionContext"/> to flow to the callback, or null if no flowing is required.</summary>
        private ExecutionContext _executionContext;

        /// <summary>
        /// A "captured" <see cref="SynchronizationContext"/> or <see cref="TaskScheduler"/> with which to invoke the callback,
        /// or null if no special context is required.
        /// </summary>
        private object _capturedContext;

        /// <summary>The exception with which the operation failed, or null if it hasn't yet completed or completed successfully.</summary>
        private ExceptionDispatchInfo _error;

        public Action<bool, object> _burnResult;
        public object _burnContext;

        /// <summary>The result with which the operation succeeded, or the default value if it hasn't yet completed or failed.</summary>
        private TResult _result;

        /// <summary>Whether the current operation has completed.</summary>
        private bool _completed;

        /// <summary>The current version of this value, used to help prevent misuse.</summary>
        private int _version;

        /// <summary>
        /// Whether this core has been burned?
        /// </summary>
        private int _burned;

        /// <summary>Gets or sets whether to force continuations to run asynchronously.</summary>
        /// <remarks>Continuations may run asynchronously if this is false, but they'll never run synchronously if this is true.</remarks>
        public bool RunContinuationsAsynchronously { get; set; }

        /// <summary>
        /// Run continuations on the flowing scheduler, else on the default one
        /// </summary>
        public bool RunContinuationsNatively { get; set; }

        /// <summary>
        /// AutoReset
        /// </summary>
        public bool AutoReset { get; set; }

        /// <summary>
        /// Is this core primed with a sentinel?
        /// </summary>
        public bool Primed => _continuation != null && _continuation == ManualResetValueTaskSourceCoreShared.SSentinel;

        /// <summary>
        /// Is this core blocking?
        /// </summary>
        public bool Blocking => _continuation != null && _continuation != ManualResetValueTaskSourceCoreShared.SSentinel && !_completed;

        /// <summary>
        /// Is this core Burned?
        /// </summary>
        public bool Burned => _burned > 0;

        public object BurnContext
        {
            get => _burnContext;
            set => _burnContext = value;
        }

#if DEBUG
        public TResult Result => _result;
#endif

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
            _completed = false;
            _continuation = null;
            _burned = 0;
            
#if DEBUG
            //if (Interlocked.Increment(ref _version) == short.MaxValue)
            //    _version = 0;
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(short index)
        {
            Reset();
            _version = index;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Prime(short index)
        {
            _version = index;
        }

        /// <summary>
        /// If this primitive is blocking
        /// </summary>
        /// <returns>True if currently blocking, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsBlocking(bool reset = false)
        {
            var blocked = _continuation != null && _continuation != ManualResetValueTaskSourceCoreShared.SSentinel && !_completed;

            if (!reset) return blocked;

            if (blocked) Reset((short)Version);

            return true;
        }


        public void SetResult(TResult result) => SetResult<object>(result);
        
        /// <summary>Completes with a successful result.</summary>
        /// <param name="result">The result.</param>
        /// <param name="async"></param>
        /// <param name="context"></param>
        public void SetResult<TContext>(TResult result, Action<bool, TContext> async = null, TContext context = default)
        {
            if (_completed)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}]: primed = {Primed}, blocking = {Blocking}, burned = {Burned}, completed = {_completed}, {GetStatus((short)Version)}");

            
            _result = result;
            SignalCompletion(async, context);
        }

        //public void SetResult(IIoManualResetValueTaskSourceCore<TResult> source)
        //{
        //    if (_completed)
        //        throw _invalidOperationException;

        //    SignalCompletion(source);
        //}

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
            if (Interlocked.CompareExchange(ref _burned, 1, 0) != 0)
                throw _invalidOperationException;

#if DEBUG
            ValidateToken(token);   
#endif
            if (!_completed)
            {
                throw _invalidOperationException;
            }

            _error?.Throw();

            var r = _result;

            if (AutoReset)
                Reset();

            return r;
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
                return;
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
            object org = _continuation;
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
                    Console.WriteLine($"{oldContinuation}, {continuation}, {state}");
                    throw _invalidOperationException;
                }

                _burnResult?.Invoke(false, _burnContext);

                switch (_capturedContext)
                {
                    case null:
                    {
                            _ = Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                            //if (_executionContext != null)
                            //{
                            //    ThreadPool.QueueUserWorkItem(continuation, state, preferLocal: true);
                            //}
                            //else
                            //{
                            //    if (!ThreadPool.UnsafeQueueUserWorkItem(static delegate (object s)
                            //        {
                            //            var (callback, state) = (ValueTuple<Action<object>, object>)s;
                            //            callback(state);
                            //        }, (continuation, state)))
                            //    {
                            //        _ = Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                            //    };
                            //}
                            break;
                    }
                    case SynchronizationContext sc:
#pragma warning disable VSTHRD001 // Avoid legacy thread switching APIs
                        sc.Post(s =>
                        {
                            var tuple = ((Action<object>, object))s;
                            tuple.Item1(tuple.Item2);
                        }, (continuation, state));
#pragma warning restore VSTHRD001 // Avoid legacy thread switching APIs
                        break;
                    case IoZeroScheduler zs:
                        if (IoZeroScheduler.Zero.QueueCallback(_continuation, _continuationState))
                        {
                            _ = Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.Zero);
                        }
                        break;
                    case TaskScheduler ts:
                        _ = Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
                        break;
                }
            }

            _burnResult?.Invoke(true, _burnContext);
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

        private void SignalCompletion() => SignalCompletion<object>();

        /// <summary>Signals that the operation has completed.  Invoked after the result or error has been set.</summary>
        /// <param name="async">Signals callback completion asynchronously status back to the caller</param>
        /// <param name="context">Caller context</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SignalCompletion<TContext>(Action<bool, TContext> async = null, TContext context = default)
        {
            if (_completed)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}]: primed = {Primed}, blocking = {Blocking}, burned = {Burned}, completed = {_completed}, {GetStatus((short)Version)}");

            _completed = true;

            //if(source != null)
            //    _result = source.GetResult((short)source.Version);

            if (_continuation == null &&
                Interlocked.CompareExchange(ref _continuation, ManualResetValueTaskSourceCoreShared.SSentinel, null) ==
                null)
            {
                async?.Invoke(false, context);

                return;
            }

            async?.Invoke(true, context);

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
#if ZERO_CORE
                        _ = Task.Factory.StartNew(_continuation, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
#else
                        if (!ThreadPool.UnsafeQueueUserWorkItem(static delegate (object s)
                            {
                                var (callback, state) = (ValueTuple<Action<object>, object>)s;
                                callback(state);
                            }, (_continuation, _continuationState)))
                        {
                            _ = Task.Factory.StartNew(_continuation, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                        }
#endif
                    }
                    else
                    {
                        _continuation(_continuationState);
                    }
                    break;

                case SynchronizationContext sc:
#pragma warning disable VSTHRD001 // Avoid legacy thread switching APIs
                    sc.Post(s =>
                    {
                        var state = ((Action<object>, object))s;
                        state.Item1(state.Item2);
                    }, (_continuation, _continuationState));
#pragma warning restore VSTHRD001 // Avoid legacy thread switching APIs
                    break;

                case IoZeroScheduler zs:
                    //async
                    if (RunContinuationsAsynchronously)
                    {
                        //_ = Task.Factory.StartNew(_continuation, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.Zero);
                        IoZeroScheduler.Zero.QueueCallback(_continuation, _continuationState);
                    }
                    else //sync
                    {
                        _continuation(_continuationState);
                    }
                    break;
                case TaskScheduler ts:
                    _ = Task.Factory.StartNew(_continuation, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
                    break;
            }
        }


        public override string ToString()
        {
            try
            {
                if (_completed)
                    return $" {_version} - {_result}";
                return $" {_version} - {GetStatus((short)_version).ToString()}";
            }
            catch (Exception e)
            {
                return e.Message;
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
