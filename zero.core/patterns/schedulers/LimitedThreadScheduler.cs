using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace zero.core.patterns.schedulers
{
    /// <summary>
    /// A limited thread scheduler
    /// </summary>
    /// <seealso cref="System.Threading.Tasks.TaskScheduler" />
    public sealed class LimitedThreadScheduler : TaskScheduler
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LimitedThreadScheduler"/> class.
        /// </summary>
        /// <param name="maxDegree">The maximum number of threads</param>
        /// <exception cref="ArgumentException"></exception>
        public LimitedThreadScheduler(int maxDegree)
        {
            if (maxDegree > MaximumConcurrencyLevel)
                throw new ArgumentException($"{maxDegree} > MaximumConcurrencyLevel = {MaximumConcurrencyLevel}");
            _maxDegree = maxDegree;
            _logger = LogManager.GetCurrentClassLogger();
            ThreadPool.GetAvailableThreads(out var maxThreads, out var maxIoThreads);
            _logger.Info($"Scheduler has {maxThreads} threads and {maxIoThreads} io threads!");
        }

        private readonly ConcurrentDictionary<Task, Task> _tasks = new ConcurrentDictionary<Task, Task>();

        private readonly int _maxDegree;
        private int _curDegree;
        private readonly Logger _logger;

        [ThreadStatic] private static bool _currentThreadIsProcessing;

        /// <summary>
        /// For debugger support only, generates an enumerable of <see cref="T:System.Threading.Tasks.Task"></see> instances currently queued to the scheduler waiting to be executed.
        /// </summary>
        /// <returns>
        /// An enumerable that allows a debugger to traverse the tasks currently queued to this scheduler.
        /// </returns>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return _tasks.Values.AsEnumerable();
        }

        /// <summary>
        /// Queues a <see cref="T:System.Threading.Tasks.Task"></see> to the scheduler.
        /// </summary>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task"></see> to be queued.</param>
        /// <exception cref="Exception">Unexpected race condition!</exception>
        protected override void QueueTask(Task task)
        {
            _currentThreadIsProcessing = true;

            try
            {
                if (!_tasks.TryAdd(task, task))
                    throw new Exception("Unexpected race condition!");
                else
                {
                    _logger.Trace($"Scheduler Q size = {_tasks.Count}, {_curDegree}/{_maxDegree}");
                }

                if (_curDegree < _maxDegree)
                {                    
                    _curDegree++;
                    ThreadPool.QueueUserWorkItem(_ =>
                    {
                        while (true)
                        {
                            if (_tasks.Count == 0 || !_tasks.TryRemove(_tasks.Last().Key, out var nextTask))
                                break;

                            if (!TryExecuteTask(nextTask))
                            {
                                _logger.Warn($"Unable to execute task! ManagedThreadId = `{Thread.CurrentThread.ManagedThreadId}'");
                                if (!_tasks.TryAdd(nextTask, nextTask))
                                    throw new Exception("Unexpected race condition!");
                            }
                        }

                        _curDegree--;
                    }, null);
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unexpected error in queuing task");
            }
            finally
            {
                _currentThreadIsProcessing = false;
            }
        }

        /// <summary>
        /// Determines whether the provided <see cref="T:System.Threading.Tasks.Task"></see> can be executed synchronously in this call, and if it can, executes it.
        /// </summary>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task"></see> to be executed.</param>
        /// <param name="taskWasPreviouslyQueued">A Boolean denoting whether or not task has previously been queued. If this parameter is True, then the task may have been previously queued (scheduled); if False, then the task is known not to have been queued, and this call is being made in order to execute the task inline without queuing it.</param>
        /// <returns>
        /// A Boolean value indicating whether the task was executed inline.
        /// </returns>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            if (_currentThreadIsProcessing) return false;

            if (taskWasPreviouslyQueued)
                if (!_tasks.Remove(task, out _))
                    _logger.Warn("Task was not queued or removed even though it should have been there!");

            if (!TryExecuteTask(task))
            {
                _tasks.TryAdd(task, task);
                return false;
            }

            return true;
        }
    }
}
