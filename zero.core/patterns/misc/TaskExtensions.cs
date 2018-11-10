using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    public static class TaskExtensions
    {
        public static async Task<TResult> HandleCancellation<TResult>(
            this Task<TResult> asyncTask,
            CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<TResult>();
            var registration = cancellationToken.Register(() =>
                tcs.TrySetCanceled(), false);
            var cancellationTask = tcs.Task;

            var readyTask = await Task.WhenAny(asyncTask, cancellationTask);
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
    }
}
