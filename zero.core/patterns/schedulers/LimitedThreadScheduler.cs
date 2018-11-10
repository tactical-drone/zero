using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace zero.core.patterns.schedulers
{
    public sealed class LimitedThreadScheduler : TaskScheduler
    {
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

        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return _tasks.Values.AsEnumerable();
        }

        protected override void QueueTask(Task task)
        {
            _currentThreadIsProcessing = true;

            try
            {
                if (!_tasks.TryAdd(task, task))
                    throw new Exception("Unexpected race condition!");
                else
                {
                    _logger.Trace($"Sheduler Q size = {_tasks.Count}, {_curDegree}/{_maxDegree}");
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
                                _logger.Warn(
                                    $"Unable to execute task! ManagedThreadId = `{Thread.CurrentThread.ManagedThreadId}'");
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
                _logger.Error(e, "Unexpected error in queing task");
            }
            finally
            {
                _currentThreadIsProcessing = false;
            }
        }

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
