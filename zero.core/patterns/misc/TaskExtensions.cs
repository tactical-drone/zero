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
#pragma warning disable 4014

            //TODO, what happens here?
                asyncTask.ContinueWith(_ => asyncTask.Exception,
#pragma warning restore 4014
                    TaskContinuationOptions.OnlyOnFaulted |
                    TaskContinuationOptions.ExecuteSynchronously);

            registration.Dispose();

            return await readyTask;
        }

        public static async Task ForEachAsync<T>(this List<T> enumerable, Action<T> action)
        {
            foreach (var item in enumerable)                
                await Task.Run(() => { action(item); }).ConfigureAwait(false);
        }
    }    
}
