using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.misc;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public struct IoAsyncMutex : IValueTaskSource<bool>, IIoMutex
    {
        /// <summary>
        /// Constructor 
        /// </summary>
        /// <param name="signalled">Initial states</param>
        /// <param name="asyncTasks"></param>
        /// <param name="allowInliningContinuations">Allow inline completions</param>
        public IoAsyncMutex(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true):this()
        {
            _manualReset = new ManualResetValueTaskSourceCore<bool>();
            _falseSentinel = ValueTask.FromResult(false);
            _trueSentinel = ValueTask.FromResult(true);
            
            _sentinelPool = new ValueTask<bool>[ushort.MaxValue + 1];
            _sentinelRefCount = new int[ushort.MaxValue + 1];
            _sentinelResult = new bool[ushort.MaxValue + 1];
            _sentinelBloom = new uint[(ushort.MaxValue + 1) / sizeof(uint)];
            _sentinelStatus = new ValueTaskSourceStatus[ushort.MaxValue];
            
            Configure(asyncTasks, signalled, allowInliningContinuations);

            var @this = this;
            var reg = asyncTasks.Token.Register(o =>
            {
                @this._manualReset.SetException(new TaskCanceledException($"{@this.Description}"));
            },false);
        }
        
        /// <summary>s
        /// Don't force async completions
        /// </summary>
        private bool _allowInliningAwaiters;
        
        /// <summary>
        /// mutex state
        /// </summary>
        private volatile int _signalled;
        
        /// <summary>
        /// backing
        /// </summary>
        private string _description;

        /// <summary>
        /// Description
        /// </summary>
        public string Description
        {
            get
            {
                // if (_description != null)
                //     return _description;
                return _description = $"{nameof(IoAsyncMutex)}({_manualReset.Version})";
            }
        }
        
        /// <summary>
        /// Pooled for fast path cancellation token exception
        /// </summary>
        // private TaskCanceledException _taskCanceledException;
        // private TaskCanceledException TaskCanceledExceptionSentinel => _taskCanceledException ??= new TaskCanceledException($"{Description} zeroed from {ZeroedFrom.Description}");

        /// <summary>
        /// pooled for fast path 
        /// </summary>
        private readonly ValueTask<bool> _trueSentinel;
        
        /// <summary>
        /// pooled for fast path 
        /// </summary>
        private readonly ValueTask<bool> _falseSentinel;

        /// <summary>
        /// Implements the mutex
        /// </summary>
        private ManualResetValueTaskSourceCore<bool> _manualReset;

        /// <summary>
        /// Configures the instance
        /// </summary>
        /// <param name="asyncTasks"></param>
        /// <param name="signalled">Initial state</param>
        /// <param name="allowInliningContinuations">Disable force async continuations</param>
        public void Configure(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true)
        {
            _allowInliningAwaiters = allowInliningContinuations;
            _asyncToken = asyncTasks.Token;
            _signalled = 0;//Don't remove this
            _zeroLock = 0;

            Array.Clear(_sentinelBloom,0,_sentinelBloom.Length);
            Array.Clear(_sentinelPool,0,_sentinelPool.Length);
            Array.Clear(_sentinelResult,0,_sentinelResult.Length);
            Array.Clear(_sentinelStatus,0,_sentinelStatus.Length);
            Array.Clear(_sentinelRefCount,0,_sentinelRefCount.Length);
            
            Reset(zero:false);
            //setup signal proxy

            //init
            if (signalled)
                _manualReset.SetResult(true);

            var sw = Stopwatch.StartNew();
            
            //init promise pool
            try
            {
                for (ushort i = 0; i < _sentinelPool.Length; i++)
                {
                    var clone = this.Clone();
                    _sentinelPool[i] = new ValueTask<bool>(clone, (short) (i - short.MaxValue));
                    
                    //_sentinelPool[i] = new ValueTask<bool>(this, (short) (i - short.MaxValue));
                    //_manualReset.Reset();
                    if (i - short.MaxValue == 0)
                    {
                        var val = _sentinelPool[i];
                        
                    }
                    
                     if(i % 1000 == 0)
                          Console.WriteLine($"{i}<<");
                    if(i == _sentinelPool.Length - 1)
                        break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            
            _signalled = signalled ? 1 : 0;
            
            Console.WriteLine($"INIT> took {sw.ElapsedTicks} ticks at {Stopwatch.Frequency/sw.ElapsedTicks} per hz: {TimeSpan.FromTicks(sw.ElapsedTicks).TotalMilliseconds}ms");
        }
        
        private readonly ValueTask<bool>[] _sentinelPool;
        private readonly int[] _sentinelRefCount;
        private readonly bool[] _sentinelResult;
        private readonly uint[] _sentinelBloom;
        private readonly ValueTaskSourceStatus[] _sentinelStatus; 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Bloom(short version, bool clear = false)
        {
            var index = version + short.MaxValue;
            try
            {
                if(!clear)
                    _sentinelBloom[index / sizeof(uint)] |= (uint) (0x1 << (version % sizeof(uint)));
                else
                    _sentinelBloom[index / sizeof(uint)] &= (uint)~(0x1 << (version % sizeof(uint)));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Bloomed(short token)
        {
            try
            {
                var index = token + short.MaxValue;
                try
                {
                    return (_sentinelBloom[index / sizeof(uint)] &= (uint) (0x1 << (token % sizeof(uint)))) > 0;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.WriteLine($"({token}) _sentinelBloom[{index / sizeof(uint)}] &= {(uint) (0x1 << (token % sizeof(uint))):x8})");
                }

                return false;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
        
        private volatile int _zeroLock;
        private CancellationToken _asyncToken;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set()
        {
            //signal
            try
            {
                if (_manualReset.GetStatus(_manualReset.Version) < ValueTaskSourceStatus.Succeeded)
                {
                    _manualReset.SetResult(!_asyncToken.IsCancellationRequested);
                    Console.WriteLine($"SET> s = {_manualReset.GetStatus(_manualReset.Version)}, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"SET ERROR: {_manualReset.GetStatus(_manualReset.Version)}");
                Console.WriteLine(e);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            try
            {
                //fail fast
                if (_asyncToken.IsCancellationRequested)
                {
                    return ValueTask.FromCanceled<bool>(_asyncToken);
                }

                var zeroStatus = _manualReset.GetStatus(_manualReset.Version);
                //fast path
                if (zeroStatus > ValueTaskSourceStatus.Pending)
                {
                    Console.WriteLine(
                        $"FAST PATH> s = {_manualReset.GetStatus(_manualReset.Version)}, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version) ? "1" : "0")}]{_sentinelResult[_manualReset.Version]}");
                    //Reset(_manualReset.Version);
                    return _manualReset.GetResult(_manualReset.Version) ? _trueSentinel : _falseSentinel;
                }


                if (Interlocked.Increment(ref _sentinelRefCount[_manualReset.Version + short.MaxValue]) > 1)
                {
                    Console.WriteLine(
                        $"AWAIT>>s = {_manualReset.GetStatus(_manualReset.Version)}, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version) ? "1" : "0")}]{_sentinelResult[_manualReset.Version]}");
                    //return new ValueTask<bool>(this, (short) (_manualReset.Version));
                    return _sentinelPool[_manualReset.Version + short.MaxValue + 10];
                }
                else
                {
                    Console.WriteLine(
                        $"AWAIT> s = {_manualReset.GetStatus(_manualReset.Version)}, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version) ? "1" : "0")}]{_sentinelResult[_manualReset.Version]}");
                    //return _sentinelPool[_manualReset.Version + short.MaxValue];
                }

                var val =  _sentinelPool[_manualReset.Version + short.MaxValue];
                return new ValueTask<bool>(this, _manualReset.Version);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        /// <summary>
        /// Resets for next use
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Reset(short token = 0, bool zero = true)
        {
            if(zero)
                Console.WriteLine($"RESET {token}> s = {_manualReset.GetStatus(_manualReset.Version)}, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
            
            //reset ref counts
            _sentinelRefCount[token] = 0;
            Bloom(token, true); 
            _sentinelResult[token] = default;
            
            //reset zero mutex
            try
            {
                if(zero)
                    _manualReset.Reset();
                else
                {
                    // _manualReset.Reset();
                    // _manualReset.Reset();
                    // _manualReset.Reset();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        /// <summary>s
        /// Get result
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
            var currentVersion = _manualReset.Version;
            var currentResult = _manualReset.GetResult(currentVersion);
            
            
            //cache get state
            if (token == currentVersion)
            {
                _sentinelResult[currentVersion] = currentResult;
                Bloom(currentVersion);
            
                Console.WriteLine($"get, s = {currentResult}, v = {currentVersion}, r = {_sentinelRefCount[currentVersion]}, C = [{(Bloomed(currentVersion)?"1":"0")}]{_sentinelResult[currentVersion]}");
                Reset(currentVersion);
                return currentResult;
            }//get cached state
            else if (Bloomed(token))
            {
                Console.WriteLine($"cache, s = {currentResult}, v = {_manualReset.Version}, r = {_sentinelRefCount[currentVersion]}, C = [{(Bloomed(currentVersion)?"1":"0")}]{_sentinelResult[currentVersion]}");
                return _sentinelResult[token];    
            }
            
            // //cache get state
            // if (token == currentVersion && Interlocked.CompareExchange(ref _zeroLock, 1, 0) == 0)
            // {
            //     _sentinelResult[currentVersion] = currentResult;
            //     Bloom(currentVersion);
            //
            //     Console.WriteLine($"get, s = {currentResult}, v = {currentVersion}, r = {_sentinelRefCount[currentVersion]}, C = [{(Bloomed(currentVersion)?"1":"0")}]{_sentinelResult[currentVersion]}");
            //     Reset(currentVersion);
            //     return currentResult;
            // }//get cached state
            // else if (Bloomed(token))
            // {
            //     Console.WriteLine($"cache, s = {currentResult}, v = {_manualReset.Version}, r = {_sentinelRefCount[currentVersion]}, C = [{(Bloomed(currentVersion)?"1":"0")}]{_sentinelResult[currentVersion]}");
            //     return _sentinelResult[token];    
            // }
            
            throw new InvalidAsynchronousStateException($"{nameof(GetResult)}: [INVALID], t = {token}, v = {currentVersion}");
        }

        /// <summary>
        /// Get the mutex status
        /// </summary>
        /// <param name="token">current version</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
            
            try
            {
                //cache get state
                if (token == _manualReset.Version && Interlocked.CompareExchange(ref _zeroLock, 1, 0) == 0)
                {
                    //_sentinelResult[currentVersion] = currentResult;
                     
                    Bloom(_manualReset.Version);

                    Console.WriteLine($"STAT, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
                    return _sentinelStatus[token] = _manualReset.GetStatus(_manualReset.Version);
                }//get cached state
                else if (Bloomed(token))
                {
                    Console.WriteLine($"STAT cache, s = {_manualReset.GetResult(_manualReset.Version)}, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
                    
                    return _sentinelStatus[token];    
                }
                
                
                // if (_manualReset.Version == token)
                // {
                //     Console.WriteLine($"getstatus({_manualReset.GetStatus(token)}), v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
                //     return _manualReset.GetStatus(token);
                // }
                // else if(Bloomed(token))
                // {
                //     Console.WriteLine($"getstatus({ValueTaskSourceStatus.Succeeded}), v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
                //     return ValueTaskSourceStatus.Succeeded;
                // } 
                return ValueTaskSourceStatus.Faulted;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return ValueTaskSourceStatus.Faulted;    
            }
        }

        /// <summary>
        /// Signal handler
        /// </summary>
        /// <param name="continuation">The callback</param>
        /// <param name="state">some state</param>
        /// <param name="token">current version</param>
        /// <param name="flags">flags</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            try
            {
                _manualReset.OnCompleted(continuation, state, token, flags);
                // _manualReset.OnCompleted(o =>
                // {
                //     Console.WriteLine("SIGNALLED!");
                // }, null, token, flags);
                Console.WriteLine($"oncomplete, v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
            }
            catch (Exception)
            {
                if (Bloomed(token))
                {
                    continuation?.Invoke(state);
                        Console.WriteLine($"oncomplete(cached), v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
                }
                else
                {
                    Console.WriteLine($"oncomplete(MISS), v = {_manualReset.Version}, r = {_sentinelRefCount[_manualReset.Version]}, C = [{(Bloomed(_manualReset.Version)?"1":"0")}]{_sentinelResult[_manualReset.Version]}");
                    //throw;    
                }
            }
        }
    }
}