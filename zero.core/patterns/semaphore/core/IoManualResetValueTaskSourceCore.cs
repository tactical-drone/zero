using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.misc;
using zero.core.runtime.scheduler;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>Provides the core logic for implementing a manual-reset <see cref="IValueTaskSource"/> or <see cref="IValueTaskSource{TResult}"/>.</summary>
    /// <typeparam name="TResult"></typeparam>
    [StructLayout(LayoutKind.Auto)]
    public struct IoManualResetValueTaskSourceCore<TResult>: IIoManualResetValueTaskSourceCore<TResult>
    {
        /// <summary>
        /// The callback to invoke when the operation completes if <see cref="OnCompleted"/> was called before the operation completed,
        /// or <see cref="ManualResetValueTaskSourceCoreShared.SSentinel"/> if the operation completed before a callback was supplied,
        /// or null if a callback hasn't yet been provided and the operation hasn't yet completed.
        /// </summary>
#pragma warning disable CS8632 // The annotation for nullable reference types should only be used in code within a '#nullable' annotations context.
        private Action<object?>? _continuation;

        /// <summary>State to pass to <see cref="_continuation"/>.</summary>
        private object? _continuationState;

        /// <summary><see cref="ExecutionContext"/> to flow to the callback, or null if no flowing is required.</summary>
        private ExecutionContext _executionContext;

        /// <summary>
        /// A "captured" <see cref="SynchronizationContext"/> or <see cref="TaskScheduler"/> with which to invoke the callback,
        /// or null if no special context is required.
        /// </summary>
        private object? _capturedContext;

        /// <summary>The exception with which the operation failed, or null if it hasn't yet completed or completed successfully.</summary>
        private ExceptionDispatchInfo? _error;

        /// <summary>The result with which the operation succeeded, or the default value if it hasn't yet completed or failed.</summary>
        [AllowNull, MaybeNull] private TResult _result;

        private Action<object> _heapAction;
        private object _heapContext;

        //public object? _burnContext;

        //public Action<bool, object>? _burnResult;

#pragma warning restore CS8632 // The annotation for nullable reference types should only be used in code within a '#nullable' annotations context.

        /// <summary>Whether the current operation has completed.</summary>
        private int _version;
        private volatile int _completeTime;
        private volatile int _burnTime;
        private volatile bool _completed;
        
        private bool _runContinuationsAsync;
        private volatile bool _runContinuationsAsyncAlways;
        private volatile bool _autoReset;
#if DEBUG
        /// <summary>
        /// Whether this core has been burned?
        /// </summary>
        private int _burned;
#endif
        /// <summary>
        /// Substitute for <see cref="RunContinuationsAsynchronously"/> used internally
        /// </summary>
        public bool RunContinuationsAsynchronouslyAlways { get => _runContinuationsAsyncAlways; set => _runContinuationsAsyncAlways = value; }
        //public bool RunContinuationsAsynchronouslyAlways { get; set; }

        /// <summary>Gets or sets whether to force continuations to run asynchronously.</summary>
        /// <remarks>Continuations may run asynchronously if this is false, but they'll never run synchronously if this is true.</remarks>
        public bool RunContinuationsAsynchronously { get => _runContinuationsAsync || _runContinuationsAsyncAlways; set => Volatile.Write(ref _runContinuationsAsync,value); }
        //public bool RunContinuationsAsynchronously { get; set; }

        /// <summary>
        /// Run continuations on the flowing scheduler, else on the default one
        /// </summary>
        public bool RunContinuationsUnsafe { get; set; }

        /// <summary>
        /// AutoReset
        /// </summary>
        public bool AutoReset { get => _autoReset; set => _autoReset = value; }
        //public bool AutoReset { get; set; }

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
#if DEBUG
        public bool Burned => _burned > 0;
#endif

        /// <summary>
        /// Whether this core is busy relaying
        /// </summary>
        public int Relay { get; set; }
//#if ZERO_CHECK
//        [MethodImpl(MethodImplOptions.AggressiveInlining)]
//        public bool Lock() => Interlocked.CompareExchange(ref _heapItemLock, 1, 0) == 0;


//        [MethodImpl(MethodImplOptions.AggressiveInlining)]
//        public IIoManualResetValueTaskSourceCore<TResult> Free()
//        {
//            Interlocked.CompareExchange(ref _heapItemLock, 0, 1);
//            return this;
//        }
//#else
//        [MethodImpl(MethodImplOptions.AggressiveInlining)]
//        public bool Lock() => true;


//        [MethodImpl(MethodImplOptions.AggressiveInlining)]
//        public IIoManualResetValueTaskSourceCore<TResult> Free() => this;
        
//#endif

        //public object BurnContext
        //{
        //    get => _burnContext;
        //    set => _burnContext = value;
        //}

#if DEBUG
        public TResult Result => _result;
        public int Completed => _completeTime;
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
            _completeTime = 0;
            Relay = 0;
            _completed = false;
            _continuation = null;

#if DEBUG
            if(_burned > 0)
                _heapAction?.Invoke(_heapContext);
#else
            _heapAction?.Invoke(_heapContext);
#endif

            Interlocked.MemoryBarrier();

#if DEBUG
            _burned = 0;   
#endif

        }

        public void Reset(int version)
        {
            Reset();
            _version = version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Prime(int version)
        {
            _version = version;
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

        //public void SetResult(TResult result) => SetResult<object>(result);

        /// <summary>Completes with a successful result.</summary>
        /// <param name = "result" > The result.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public void SetResult<TContext>(TResult result, Action<bool, TContext> async = null, TContext context = default)
        public void SetResult(TResult result)
        {
#if DEBUG
            if (_completed)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}]: set => v = [{_version}], primed = {Primed}, blocking = {Blocking}, burned = {Burned}, completed = {_completed}, {GetStatus((short)Version)}");   
#else
            if (_completed)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}]: set => v = [{_version}], primed = {Primed}, blocking = {Blocking}, completed = {_completed}, {GetStatus((short)Version)}");
#endif

            _result = result;
            _completeTime = Environment.TickCount;
            _burnTime = 0;
            //SignalCompletion(async, context);
            SignalCompletion();
        }

        /// <summary>Completes with an error.</summary>
        /// <param name="error"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetException(Exception error)
        {
            _error = ExceptionDispatchInfo.Capture(error);
            SignalCompletion();
        }

        public void Reset(Action<object> index, object context)
        {
            _heapAction = index;
            _heapContext = context;
        }

        public ValueTask<TResult> WaitAsync() => new(this, (short)Version);
        
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
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public TResult GetResult(short token)
        {
#if DEBUG
            if (Interlocked.CompareExchange(ref _burned, 1, 0) != 0)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}] {nameof(GetResult)}: core #{_version} already burned {_completeTime.ElapsedMs()} ms, burned = {_burnTime.ElapsedMs()} ms");
            ValidateToken(token);   
#endif
            if (!_completed)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}] {nameof(GetResult)}: core  #{_version} not completed {_completeTime.ElapsedMs()} ms, burned = {_burnTime.ElapsedMs()} ms");
            
            var r = _result;

            if (_autoReset)
                Reset();

            _error?.Throw();

            _burnTime = Environment.TickCount;
            return r;
        }

        /// <summary>Schedules the continuation action for this operation.</summary>
        /// <param name="continuation">The continuation to invoke when the operation has completed.</param>
        /// <param name="state">The state object to pass to <paramref name="continuation"/> when it's invoked.</param>
        /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
        /// <param name="flags">The flags describing the behavior of the continuation.</param>
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
#if DEBUG
            Debug.Assert(continuation != null && state != null);
            ValidateToken(token);
#endif
            if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
            {
                _executionContext = ExecutionContext.Capture();
            }

            if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0 && !RunContinuationsUnsafe)
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
#if DEBUG
                var ts = Environment.TickCount;
#endif

                oldContinuation = Interlocked.CompareExchange(ref _continuation, continuation, null);

#if DEBUG
                if (ts.ElapsedMs() > 16)
                    LogManager.GetCurrentClassLogger().Fatal($"OnComplete CAS took => {ts.ElapsedMs()}");
                //Debug.Assert(ts.ElapsedMs() <= 16);       
#endif
            }

            if (oldContinuation != null)
            {
                // Operation already completed, so we need to queue the supplied callback.
#pragma warning disable CS0252 // Possible unintended reference comparison; left hand side needs cast
#if DEBUG
                if (oldContinuation != ManualResetValueTaskSourceCoreShared.SSentinel)
                    throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}] // => had = {oldContinuation}, // => has = {continuation}, {state}, v = [{_version}], primed = {Primed}, blocking = {Blocking}, burned = {Burned}, completed = {_completed}, {GetStatus((short)Version)}");       
#else
                if (oldContinuation != ManualResetValueTaskSourceCoreShared.SSentinel)
                    throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}] // => had = {oldContinuation}, // => has = {continuation}, {state}, v = [{_version}], primed = {Primed}, blocking = {Blocking}, completed = {_completed}, {GetStatus((short)Version)}");       
#endif

#pragma warning restore CS0252 // Possible unintended reference comparison; left hand side needs cast

                //try
                //{
                //    _burnResult?.Invoke(false, _burnContext);
                //}
                //catch
                //{
                //    // ignored
                //}

                switch (_capturedContext)
                {
                    case null:
                        _ = Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                        //if(RunContinuationsAsynchronously)
                        //    _ = Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                        //else
                        //    continuation(state);
                        break;
                    case SynchronizationContext sc:
#pragma warning disable VSTHRD001 // Avoid legacy thread switching APIs
                        sc.Post(s =>
                        {
                            var tuple = ((Action<object>, object))s;
                            tuple.Item1(tuple.Item2);
                        }, (continuation, state));
#pragma warning restore VSTHRD001 // Avoid legacy thread switching APIs
                        break;
                    //case IoZeroScheduler tz when TaskScheduler.Current is IoZeroScheduler && !RunContinuationsAsynchronously:
                    //    continuation(state);
                    //    break;
                    case TaskScheduler ts:
                        _ = Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
                        break;
                }
            }


            //try
            //{
            //    _burnResult?.Invoke(true, _burnContext);
            //}
            //catch
            //{
            //    // ignored
            //}
        }

#if DEBUG
        /// <summary>Ensures that the specified token matches the current version.</summary>
        /// <param name="token">The token supplied by <see cref="ValueTask"/>.</param>
        private void ValidateToken(int token)
        {
            if (token != _version)
            {
                throw new InvalidOperationException($"{nameof(ValidateToken)}: Invalid core token detected: wants = {token}, has = {_version}");
            }
        }
#endif

        //private void SignalCompletion() => SignalCompletion<object>();

        /// <summary>Signals that the operation has completed.  Invoked after the result or error has been set.</summary>
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        //private void SignalCompletion<TContext>(Action<bool, TContext> async = null, TContext context = default)
        private void SignalCompletion()
        {
#if DEBUG
            if (_completed)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}]: set => v = [{_version}], primed = {Primed}, blocking = {Blocking}, burned = {Burned}, completed = {_completed}, {GetStatus((short)Version)}");
#else
            if (_completed)
                throw new InvalidOperationException($"[{Thread.CurrentThread.ManagedThreadId}]: set => v = [{_version}], primed = {Primed}, blocking = {Blocking}, completed = {_completed}, {GetStatus((short)Version)}");
#endif
            _completed = true;

#if DEBUG
            var ts = Environment.TickCount;
#endif
            try
            {
                if (_continuation == null &&
                    Interlocked.CompareExchange(ref _continuation, ManualResetValueTaskSourceCoreShared.SSentinel, null) ==
                    null)
                {
                    //async?.Invoke(false, context);
                    return;
                }
            }
            finally
            {
#if DEBUG
                if (ts.ElapsedMs() > 16)
                {
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(SignalCompletion)}: CAS took => {ts.ElapsedMs()} ms");
                }
#endif
            }

            //async?.Invoke(true, context);

            if (_executionContext != null)
            {
                ExecutionContext.Run(
                    _executionContext,
                    static s => ((IoManualResetValueTaskSourceCore<TResult>)s).InvokeContinuation(),
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
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private void InvokeContinuation()
        {
            Debug.Assert(_continuation != null && _continuationState != null);
            switch (_capturedContext)
            {
                case null:
                    if (RunContinuationsAsynchronously)
                        _ = Task.Factory.StartNew(_continuation!, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                    else
                        _continuation!(_continuationState);
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
                case IoZeroScheduler tz when TaskScheduler.Current is IoZeroScheduler && !RunContinuationsAsynchronously:
                    _continuation!(_continuationState);
                    break;
                case TaskScheduler ts:
                    _ = Task.Factory.StartNew(_continuation!, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts); 
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
