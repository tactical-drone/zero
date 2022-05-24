using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using zero.core.patterns.semaphore.core;

namespace zero.gauge.core.misc
{
    public class ZeroCAS
    {
        private long _reg;
        [Benchmark]
        public async Task ZeroNext()
        {
            var _count = 100000;
            var threads = 100;
            

            var tasks = new List<Task>();
            for (var t = 0; t < threads; t++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < _count; i++)
                    {
                        _reg.ZeroNext(_count);
                    }
                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(15));
        }

        //[Benchmark]
        //public async Task ZeroNext2()
        //{
        //    var _count = 100000;
        //    var threads = 100;


        //    var tasks = new List<Task>();
        //    for (var t = 0; t < threads; t++)
        //    {
        //        tasks.Add(Task.Factory.StartNew(() =>
        //        {
        //            for (int i = 0; i < _count; i++)
        //            {
        //                _reg.ZeroNext2(_count);
        //            }
        //        }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
        //    }

        //    await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(15));
        //}
    }
}
