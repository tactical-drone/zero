using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// Some useful task extensions
    /// </summary>
    public static class TaskExtensions
    {
        /// <summary>
        /// Executes a task but interrupts it manually if cancellation is requested
        /// </summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="asyncTask">The asynchronous task.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The original task, or the cancellation task if this task was canceled</returns>
        //        public static async Task<AggregateException> HandleCancellationAsync<TResult>(
        //            this Task<TResult> asyncTask,
        //            CancellationToken cancellationToken)
        //        {
        //            var tcs = new TaskCompletionSource<TResult>();
        //            var registration = cancellationToken.Register(() =>
        //                tcs.TrySetCanceled(), false);
        //            var cancellationTask = tcs.Task;

        //            var readyTask = await Task.WhenAny(asyncTask, cancellationTask).ConfigureAwait(ZC);
        //            if (readyTask == cancellationTask)


        //                //TODO, what happens here?
        //#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        //                asyncTask.ContinueWith(_ => asyncTask.Exception,

        //                    TaskContinuationOptions.OnlyOnFaulted |
        //                    TaskContinuationOptions.ExecuteSynchronously).ConfigureAwait(ZC);
        //#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

        //            await registration.DisposeAsync();

        //            return await readyTask.ConfigureAwait(ZC);
        //        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="enumerable"></param>
        /// <param name="action"></param>
        /// <param name="nanite"></param>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TN"></typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask ForEachAsync<T,TN>(this List<T> enumerable, Func<T, TN, ValueTask> action, TN nanite = default)
        {
            foreach (var item in enumerable)                
                await Task.Factory.StartNew(static async state =>
                {
                    var (action, item, nanite) = (ValueTuple<Func<T,TN, ValueTask>, T, TN>)state;
                    await action(item,nanite).FastPath().ConfigureAwait(true);
                }, ValueTuple.Create(action, item,nanite)).ConfigureAwait(true);
        }

        /// <summary>
        /// Fast path for value tasks
        /// </summary>
        /// <typeparam name="T">The return type</typeparam>
        /// <param name="task">The value task to be fast pathed</param>
        /// <returns>The result of the async op</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueTask<T> FastPath<T>(this ValueTask<T> task)
        {
            return task.IsCompletedSuccessfully ? new ValueTask<T>(task.Result) : task;
        }

        /// <summary>
        /// Fast path for value tasks
        /// </summary>
        /// <typeparam name="T">The return type</typeparam>
        /// <param name="task">The value task to be fast pathed</param>
        /// <returns>The result of the async op</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueTask FastPath(this ValueTask task)
        {
            return task.IsCompletedSuccessfully ? default : task;
        }

        /// <summary>
        /// Block on a token until cancelled (wait one causes problems)
        /// </summary>
        /// <param name="token">The token to block on</param>
        /// <returns>A ValueTask</returns>
        public static async ValueTask BlockOnNotCanceledAsync(this CancellationToken token)
        {
            IIoManualResetValueTaskSourceCore<bool> source = new IoManualResetValueTaskSourceCore<bool>();
            var reg = token.Register(static s =>
            {
                ((IoManualResetValueTaskSourceCore<bool>)s).Set(true);
            }, source);

            var waitForCancellation = new ValueTask<bool>(source, 0);
            await waitForCancellation.FastPath();
            await reg.DisposeAsync().FastPath();
        }        
    }    
}
