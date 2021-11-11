﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// ManualResetValueTaskSource
    /// </summary>
    /// <typeparam name="T">The result type, can be anything</typeparam>
    public sealed class IoManualResetValueTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
    {
        public IoManualResetValueTaskSource(bool inline = false)
        {
            _core.RunContinuationsAsynchronously = inline;            
        }
        private ManualResetValueTaskSourceCore<T> _core; // mutable struct; do not make this readonly
               
        public bool RunContinuationsAsynchronously { get => _core.RunContinuationsAsynchronously; set => _core.RunContinuationsAsynchronously = value; }
        public short Version => _core.Version;
        public void Reset() => _core.Reset();
        public void SetResult(T result) => _core.SetResult(result);        
        public void SetException(Exception error) => _core.SetException(error);

        public T GetResult(short token) => _core.GetResult(token);
        void IValueTaskSource.GetResult(short token) => _core.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);

        public ValueTask<T> WaitAsync()
        {            
            return new ValueTask<T>(this, _core.Version);
        }
    }
}
