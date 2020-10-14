using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

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

//            var readyTask = await Task.WhenAny(asyncTask, cancellationTask).ConfigureAwait(false);
//            if (readyTask == cancellationTask)


//                //TODO, what happens here?
//#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
//                asyncTask.ContinueWith(_ => asyncTask.Exception,

//                    TaskContinuationOptions.OnlyOnFaulted |
//                    TaskContinuationOptions.ExecuteSynchronously).ConfigureAwait(false);
//#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

//            await registration.DisposeAsync();

//            return await readyTask.ConfigureAwait(false);
//        }

        public static async Task ForEachAsync<T>(this List<T> enumerable, Func<T, Task> action)
        {
            foreach (var item in enumerable)                
                await Task.Run(async () => { await action(item).ConfigureAwait(false); }).ConfigureAwait(false);
        }
    }    
}
