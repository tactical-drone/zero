using System;
using System.Collections.Generic;
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
        public static async Task<TResult> HandleCancellation<TResult>(
            this Task<TResult> asyncTask,
            CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<TResult>();
            var registration = cancellationToken.Register(() =>
                tcs.TrySetCanceled(), false);
            var cancellationTask = tcs.Task;

            var readyTask = await Task.WhenAny(asyncTask, cancellationTask).ConfigureAwait(false);
            if (readyTask == cancellationTask)


            //TODO, what happens here?
            await asyncTask.ContinueWith(_ => asyncTask.Exception,

                    TaskContinuationOptions.OnlyOnFaulted |
                    TaskContinuationOptions.ExecuteSynchronously);

            await registration.DisposeAsync();

            return await readyTask.ConfigureAwait(false);
        }

        public static async Task ForEachAsync<T>(this List<T> enumerable, Action<T> action)
        {
            foreach (var item in enumerable)                
                await Task.Run(() => { action(item); }).ConfigureAwait(false);
        }
    }    
}
