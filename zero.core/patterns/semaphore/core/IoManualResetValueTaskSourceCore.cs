using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.misc;
using zero.core.runtime.scheduler;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>Provides the core logic for implementing a manual-reset <see cref="IValueTaskSource"/> or <see cref="IValueTaskSource{TResult}"/>.
    ///
    /// This version is a slightly modified version of the runtime (.net 8), allowing a special case where if a waiter is blocking it can be un-blocked asynchronously,
    /// while if a result is already ready, it is unblocked synchronously. This should boost performance.
    ///
    /// Caution: Highly experimental!
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    [StructLayout(LayoutKind.Auto)]
    public struct IoManualResetValueTaskSourceCore<TResult>: IIoManualResetValueTaskSourceCore<TResult>
    {
        /// <summary>
        /// The callback to invoke when the operation completes if <see cref="OnCompleted"/> was called before the operation completed,
        /// or <see cref="ManualResetValueTaskSourceCoreShared.s_sentinel"/> if the operation completed before a callback was supplied,
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

        /// <summary>The result with which the operation succeeded, or the default value if it hasn't yet completed or failed.</summary>
        [AllowNull, MaybeNull] private TResult _result;

        private Action<object> _heapAction;
        private object _heapContext;

        //public object? _burnContext;
        //public Action<bool, object>? _burnResult;

        /// <summary>Whether the current operation has completed.</summary>
        private bool _completed;
        private int _runContinuationsAsync; //Options on just in time async/sync calls

#if DEBUG
        private int _completeTime;
        private int _burnTime;
        private int _burned;
#endif
        /// <summary>
        /// Substitute for <see cref="RunContinuationsAsynchronously"/> used internally
        /// </summary>
        public bool RunContinuationsAsynchronouslyAlways { get; set; }

        /// <summary>Gets or sets whether to force continuations to run asynchronously.</summary>
        /// <remarks>Continuations may run asynchronously if this is false, but they'll never run synchronously if this is true.</remarks>
        public bool RunContinuationsAsynchronously { readonly get => _runContinuationsAsync > 0 || RunContinuationsAsynchronouslyAlways; set => Interlocked.Exchange(ref _runContinuationsAsync, value ? 1 : 0); }

        /// <summary>
        /// Run continuations on the flowing scheduler, else on the default one
        /// </summary>
        public bool RunContinuationsUnsafe { get; set; }

        /// <summary>
        /// AutoReset true always prepares core for reuse on completion
        /// </summary>
        public bool AutoReset { get; set; }

        private int _syncRoot;
        /// <summary>
        /// Allows the core to be synchronized. Useful when splitting the results and blockers into different queues 
        /// </summary>
        public int SyncRoot { readonly get => _syncRoot; set => Interlocked.Exchange(ref _syncRoot, value); }

        /// <summary>
        /// Is this core primed with a sentinel?
        /// </summary>
        public readonly bool Primed => _continuation != null && _continuation == ManualResetValueTaskSourceCoreShared.s_sentinel;

        /// <summary>
        /// Is this core blocking?
        /// </summary>
        public readonly bool Blocking => _continuation != null && _continuation != ManualResetValueTaskSourceCoreShared.s_sentinel && !_completed;

        
        /// <summary>
        /// Is this core Burned?
        /// </summary>
#if DEBUG
        public readonly bool Burned => _burned > 0;
#endif

        //public object BurnContext
        //{
        //    get => _burnContext;
        //    set => _burnContext = value;
        //}

        /// <summary>Resets to prepare for the next operation.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            // Reset/update state for the next use/await of this instance.
            _result = default;
            _error = null;
            _executionContext = null;
            SyncRoot = 0;
            _runContinuationsAsync = 0;
#if DEBUG
            _completeTime = 0;
            _burned = 0;
#endif
            _capturedContext = _continuationState = null;
            _completed = false;
            Interlocked.Exchange(ref _continuation, null);

            //allows for this core to be placed back into a heap once completed
            _heapAction?.Invoke(_heapContext);
        }

        /// <summary>
        /// If this primitive is blocking
        /// </summary>
        /// <returns>True if currently blocking, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsBlocking(bool reset = false)
        {
            var blocked = _continuation != null && _continuation != ManualResetValueTaskSourceCoreShared.s_sentinel && !_completed;

            if (!reset) return blocked;

            if (blocked) Reset();

            return true;
        }

        /// <summary>Completes with a successful result.</summary>
        /// <param name = "result" > The result.</param>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        //public void SetResult<TContext>(TResult result, Action<bool, TContext> async = null, TContext context = default)
        public void SetResult(TResult result)
        {
            if (_completed)
                throw new InvalidOperationException($"[{Environment.CurrentManagedThreadId}]: set => primed = {Primed}, blocking = {Blocking}, completed = {_completed}, {GetStatus(0)}");   

            _result = result;
#if DEBUG
            _completeTime = Environment.TickCount;
            _burnTime = 0;
#endif
            //SignalCompletion(async, context);
            SignalCompletion();
        }

        /// <summary>Completes with an error.</summary>
        /// <param name="error"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetException(Exception error)
        {
            Volatile.Write(ref _error, ExceptionDispatchInfo.Capture(error));
            SignalCompletion();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(Action<object> resetAction, object context)
        {
            Interlocked.Exchange(ref _heapAction, resetAction);
            Interlocked.Exchange(ref _heapContext, context);
        }

        /// <summary>Gets the status of the operation.</summary>
        /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public readonly ValueTaskSourceStatus GetStatus(short token = 0)
        {
#if DEBUG
            //ValidateToken(token);   
#endif
            return
                _continuation == null || !_completed ? ValueTaskSourceStatus.Pending :
                _error == null ? ValueTaskSourceStatus.Succeeded : 
                _error.SourceException is OperationCanceledException ? ValueTaskSourceStatus.Canceled : ValueTaskSourceStatus.Faulted;
        }

        /// <summary>Gets the result of the operation.</summary>
        /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public TResult GetResult(short token)
        {
#if DEBUG
            if (Interlocked.CompareExchange(ref _burned, 1, 0) != 0)
                throw new InvalidOperationException($"[{Environment.CurrentManagedThreadId}] {nameof(GetResult)}: core already burned completed {_completeTime.ElapsedMs()} ms ago, burned = {_burnTime.ElapsedMs()} ms ago");

            _burnTime = Environment.TickCount;
#endif

            if (!_completed)
                throw new InvalidOperationException($"[{Environment.CurrentManagedThreadId}] {nameof(GetResult)}: core not completed!");

            try
            {
                _error?.Throw();
                return _result;
            }
            finally
            {
                if (AutoReset)
                    Reset();
            }
        }

        /// <summary>Schedules the continuation action for this operation.</summary>
        /// <param name="continuation">The continuation to invoke when the operation has completed.</param>
        /// <param name="state">The state object to pass to <paramref name="continuation"/> when it's invoked.</param>
        /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
        /// <param name="flags">The flags describing the behavior of the continuation.</param>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            Debug.Assert(continuation != null);

            if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
            {
                _capturedContext = ExecutionContext.Capture();
            }

            if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0)
            {
                if (SynchronizationContext.Current is SynchronizationContext sc &&
                    sc.GetType() != typeof(SynchronizationContext))
                {
                    _capturedContext = _capturedContext is null ?
                        sc :
                        new CapturedSchedulerAndExecutionContext(sc, (ExecutionContext)_capturedContext);
                }
                else
                {
                    TaskScheduler ts = TaskScheduler.Current;
                    if (ts != TaskScheduler.Default)
                    {
                        _capturedContext = _capturedContext is null ?
                            ts :
                            new CapturedSchedulerAndExecutionContext(ts, (ExecutionContext)_capturedContext);
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
            object storedContinuation = _continuation;
            if (storedContinuation is null)
            {
                _continuationState = state;
                storedContinuation = Interlocked.CompareExchange(ref _continuation, continuation, null);
                if (storedContinuation is null)
                {
                    // Operation hadn't already completed, so we're done. The continuation will be
                    // invoked when SetResult/Exception is called at some later point.
                    return;
                }
            }

            // Operation already completed, so we need to queue the supplied callback.
            // At this point the storedContinuation should be the sentinal; if it's not, the instance was misused.
            Debug.Assert(storedContinuation is not null, $"{nameof(storedContinuation)} is null");
            if (!ReferenceEquals(storedContinuation, ManualResetValueTaskSourceCoreShared.s_sentinel))
            {
                throw new InvalidOperationException();
            }

            object capturedContext = _capturedContext;
            switch (capturedContext)
            {
                case null:
                    static void WaitCallback(object context)
                    {
                        var (continuation, state) = (ValueTuple<Action<object>, object>)context;
                        continuation(state);
                    }

                    ThreadPool.UnsafeQueueUserWorkItem(WaitCallback, (continuation, state));
                    break;

                case ExecutionContext:
                    ThreadPool.QueueUserWorkItem(continuation, state, preferLocal: true);
                    break;

                //case IoZeroScheduler when !RunContinuationsAsynchronously && TaskScheduler.Current is IoZeroScheduler:
                //    continuation(state);
                //    break;

                default:
                    ManualResetValueTaskSourceCoreShared.ScheduleCapturedContext(capturedContext, continuation, state, RunContinuationsAsynchronously);
                    break;
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

        /// <summary>Signals that the operation has completed.  Invoked after the result or error has been set.</summary>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        //private void SignalCompletion<TContext>(Action<bool, TContext> async = null, TContext context = default)
        private void SignalCompletion()
        {
            if (_completed)
            {
                throw new InvalidOperationException($"[{Environment.CurrentManagedThreadId}]: set => primed = {Primed}, blocking = {Blocking}, completed = {_completed}, status = {GetStatus()}");
            }
            _completed = true;

            Action<object> continuation =
                Volatile.Read(ref _continuation) ??
                Interlocked.CompareExchange(ref _continuation, ManualResetValueTaskSourceCoreShared.s_sentinel, null);

            if (continuation == null)
                return;

            object context = _capturedContext;
            if (context is null)
            {
                if (RunContinuationsAsynchronously)
                {
                    static void WaitCallback(object context)
                    {
                        var (continuation, state) = (ValueTuple<Action<object>, object>)context;
                        continuation(state);
                    }
                    
                    ThreadPool.UnsafeQueueUserWorkItem(WaitCallback, (_continuation, _continuationState));
                }
                else
                {
                    _continuation(_continuationState);
                }
            }
            else if (context is ExecutionContext or CapturedSchedulerAndExecutionContext)
            {
                ManualResetValueTaskSourceCoreShared.InvokeContinuationWithContext(context, _continuation, _continuationState, RunContinuationsAsynchronously);
            }
            else
            {
                //Debug.Assert(context is TaskScheduler or SynchronizationContext, $"context is {context}");
                Debug.Assert(context is TaskScheduler or SynchronizationContext);
                ManualResetValueTaskSourceCoreShared.ScheduleCapturedContext(context, _continuation, _continuationState, RunContinuationsAsynchronously);
            }

            //async?.Invoke(true, context);
        }

        /// <summary>
        /// Invokes the continuation with the appropriate captured context / scheduler.
        /// This assumes that if <see cref="_executionContext"/> is not null we're already
        /// running within that <see cref="ExecutionContext"/>.
        /// </summary>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private readonly void InvokeContinuation()
        {
            var currentContext = ExecutionContext.Capture();
            //Debug.Assert(_continuation != null && _continuationState != null);
            switch (_capturedContext)
            {
                case null:
                    if (RunContinuationsAsynchronously)
                    {
                        //Task.Factory.StartNew(_continuation, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                        if (_executionContext != null)
                        {
                            ThreadPool.QueueUserWorkItem(_continuation, _continuationState, preferLocal: true);
                        }
                        else
                        {
                            static void WaitCallback(object context)
                            {
                                var (continuation, state) = (ValueTuple<Action<object>, object>)context;
                                continuation(state);
                            }
                            ThreadPool.UnsafeQueueUserWorkItem(WaitCallback, (_continuation, _continuationState));
                        }
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
                case IoZeroScheduler tz when !RunContinuationsAsynchronously && TaskScheduler.Current is IoZeroScheduler:
                    _continuation!(_continuationState);
                    break;
                case TaskScheduler ts:
                    //new Task(_continuation, _continuationState).Start(IoZeroScheduler.ZeroDefault);
                    _ = Task.Factory.StartNew(_continuation!, _continuationState, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts); 
                    break;
            }
        }

        public override string ToString()
        {
            try
            {
                if (_completed)
                    return $"state = {GetStatus()}, result = {_result}";
                return $"state = {GetStatus()}, blocking = {IsBlocking()}, primed = {Primed}";
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
    }

    /// <summary>A tuple of both a non-null scheduler and a non-null ExecutionContext.</summary>
    internal sealed class CapturedSchedulerAndExecutionContext
    {
        internal readonly object _scheduler;
        internal readonly ExecutionContext _executionContext;

        public CapturedSchedulerAndExecutionContext(object scheduler, ExecutionContext executionContext)
        {
            //Debug.Assert(scheduler is SynchronizationContext or TaskScheduler, $"{nameof(scheduler)} is {scheduler}");
            //Debug.Assert(executionContext is not null, $"{nameof(executionContext)} is null");
            Debug.Assert(scheduler is SynchronizationContext or TaskScheduler);
            Debug.Assert(executionContext is not null);

            _scheduler = scheduler;
            _executionContext = executionContext;
        }
    }

    internal static class ManualResetValueTaskSourceCoreShared // separated out of generic to avoid unnecessary duplication
    {
        internal static readonly Action<object> s_sentinel = CompletionSentinel;

        private static void CompletionSentinel(object _) // named method to aid debugging
        {
            //Debug.Fail("The sentinel delegate should never be invoked.");
            throw new InvalidOperationException();
        }

        internal static void ScheduleCapturedContext(object context, Action<object> continuation, object state, bool runContinuationsAsynchronously)
        {
            //Debug.Assert(
            //    context is SynchronizationContext or TaskScheduler or CapturedSchedulerAndExecutionContext,
            //    $"{nameof(context)} is {context}");
            Debug.Assert(
                context is SynchronizationContext or TaskScheduler or CapturedSchedulerAndExecutionContext);

            switch (context)
            {
                case SynchronizationContext sc:
                    ScheduleSynchronizationContext(sc, continuation, state);
                    break;

                case IoZeroScheduler when !runContinuationsAsynchronously && TaskScheduler.Current is IoZeroScheduler:
                    continuation(state);
                    break;

                case TaskScheduler ts:
                    Schedule(ts, continuation, state);
                    break;
                default:
                    var cc = (CapturedSchedulerAndExecutionContext)context;
                    if (cc._scheduler is SynchronizationContext ccsc)
                    {
                        ScheduleSynchronizationContext(ccsc, continuation, state);
                    }
                    else
                    {
                        //Debug.Assert(cc._scheduler is TaskScheduler, $"{nameof(cc._scheduler)} is {cc._scheduler}");
                        Debug.Assert(cc._scheduler is TaskScheduler);
                        Schedule((TaskScheduler)cc._scheduler, continuation, state);
                    }
                    break;
            }

            return;

            static void ScheduleSynchronizationContext(SynchronizationContext sc, Action<object> continuation, object state) =>
                sc.Post(continuation.Invoke, state);

            static void Schedule(TaskScheduler scheduler, Action<object> continuation, object state) =>
                Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler);
        }

        internal static void InvokeContinuationWithContext(object capturedContext, Action<object> continuation, object continuationState, bool runContinuationsAsynchronously)
        {
            // This is in a helper as the error handling causes the generated asm
            // for the surrounding code to become less efficient (stack spills etc)
            // and it is an uncommon path.
            //Debug.Assert(continuation is not null, $"{nameof(continuation)} is null");
            //Debug.Assert(capturedContext is ExecutionContext or CapturedSchedulerAndExecutionContext, $"{nameof(capturedContext)} is {capturedContext}");
            Debug.Assert(continuation is not null);
            Debug.Assert(capturedContext is ExecutionContext or CapturedSchedulerAndExecutionContext);

            // Capture the current EC.  We'll switch over to the target EC and then restore back to this one.

            ExecutionContext currentContext = ExecutionContext.Capture();

            if (capturedContext is ExecutionContext ec)
            {
                
                if (runContinuationsAsynchronously)
                {
                    static void Aa(object state)
                    {
                        var (continuation, continuationState) = (ValueTuple<Action<object>, object>)state;
                        ThreadPool.QueueUserWorkItem(continuation, continuationState, preferLocal: true);
                    }
                    ExecutionContext.Run(ec, Aa, (continuation, continuationState));
                }
                else
                {
                    // Running inline may throw; capture the edi if it does as we changed the ExecutionContext,
                    // so need to restore it back before propagating the throw.
                    ExceptionDispatchInfo edi = null;
                    SynchronizationContext syncContext = SynchronizationContext.Current;

                    static void Cc(object state)
                    {
                        var (continuation, continuationState) = (ValueTuple<Action<object>, object>)state;
                        continuation(continuationState);
                    }

                    try
                    {
                        ExecutionContext.Run(ec, Cc, (continuation, continuationState));
                    }
                    catch (Exception ex)
                    {
                        // Note: we have a "catch" rather than a "finally" because we want
                        // to stop the first pass of EH here.  That way we can restore the previous
                        // context before any of our callers' EH filters run.
                        edi = ExceptionDispatchInfo.Capture(ex);
                    }
                    finally
                    {
                        // Set sync context back to what it was prior to coming in.
                        // Then restore the current ExecutionContext.
                        SynchronizationContext.SetSynchronizationContext(syncContext);
                    }

                    // Now rethrow the exception; if there is one.
                    edi?.Throw();
                }
            }
            else
            {
                static void Cc(object state)
                {
                    var (continuation, continuationState, capturedContext, runContinuationsAsynchronously) = (ValueTuple<Action<object>, object, object, bool>)state;
                    ScheduleCapturedContext(capturedContext, continuation, continuationState, runContinuationsAsynchronously);
                }
                ExecutionContext.Run(((CapturedSchedulerAndExecutionContext)capturedContext)._executionContext, Cc, (continuation, continuationState, capturedContext, runContinuationsAsynchronously));
            }
        }
    }
}
