using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore
{
    public struct IoAsyncMutex : IIoMutex
    {
        private const int BufferSize = 2;
        /// <summary>
        /// Constructor 
        /// </summary>
        /// <param name="signalled">Initial states</param>
        /// <param name="asyncTasks"></param>
        /// <param name="allowInliningContinuations">Allow inline completions</param>
        public IoAsyncMutex(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true):this()
        {
            _version = 0;
            _versionSet = 0;
            _completed = 0;
            _manualReset = new ManualResetValueTaskSourceCore<bool>();
            _falseSentinel = ValueTask.FromResult(false);
            _trueSentinel = ValueTask.FromResult(true);
            
            
           //SetRootRef(ref this);s
           _sentinelCore = new IIoMutex[BufferSize];
            _sentinelTask = new ValueTask<bool>[BufferSize];
            _sentinelRefCount = new int[BufferSize];
            _sentinelResult = new int[BufferSize];
            _sentinelStatus = new ValueTaskSourceStatus[BufferSize];
            
            
        }

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ByRef(ref IIoMutex root)
        {
            _sentinelRoot = root;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short Version()
        {
            return (short) _version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref IIoMutex GetRef(ref IIoMutex mutex)
        {
            return ref mutex;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short GetCurFrame()
        {
            return _frameId;
        }

        public bool SetWaiter(Action<object> continuation, object state)
        {
            if (Interlocked.CompareExchange(ref _continuation, continuation, null) == null)
            {
                _state = state;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Don't force async completions
        /// </summary>
        private bool _allowInliningAwaiters;
        
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
                return _description = $"{nameof(IoAsyncMutex)}({_version})";
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
            _frameId = 0;
            _sentinel = 0;

            Array.Clear(_sentinelCore,0,_sentinelCore.Length);
            //Array.Clear(_sentinelBloom,0,_sentinelBloom.Length);
            Array.Clear(_sentinelTask,0,_sentinelTask.Length);
            Array.Clear(_sentinelResult,0,_sentinelResult.Length);
            Array.Clear(_sentinelStatus,0,_sentinelStatus.Length);
            Array.Clear(_sentinelRefCount,0,_sentinelRefCount.Length);

            for (int i = 0; i < _sentinelResult.Length; i++)
            {
                _sentinelResult[i] = -1;
            }
            
            var sw = Stopwatch.StartNew();
            
            //init frame buffer
            try
            {
                
                //clone sentinel frame buffer
                
                for (int i = 0; i < BufferSize; i++)
                {
                    _sentinel++;
                    IIoMutex clone = this;
                    //clone.SetRoot(ref this);
                    _sentinelCore[i] = clone;
                    _sentinelTask[i] = new ValueTask<bool>(clone, (short) i);    
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            { 
                //reset the factory
                _version = 0;

                //init
                _sentinel = 0;
                if (signalled)
                    _manualReset.SetResult(true);

                Console.WriteLine($"INIT> took {sw.ElapsedTicks} ticks at {Stopwatch.Frequency/sw.ElapsedTicks} per hz: {TimeSpan.FromTicks(sw.ElapsedTicks).TotalMilliseconds}ms");    
            }
        }
        
        /// <summary>
        /// Get the root
        /// </summary>
        /// <returns>The root sentinel</returns>
        private IIoMutex GetSentinelRoot()
        {
            return _sentinelRoot;
        }
        
        private IIoMutex _sentinelRoot;
        private readonly IIoMutex[] _sentinelCore;
        private readonly ValueTask<bool>[] _sentinelTask;
        private readonly int[] _sentinelRefCount;
        private readonly int[] _sentinelResult;
        private readonly ValueTaskSourceStatus[] _sentinelStatus;

        private Action<object> _continuation;
        private object _state;

        // [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // private void Bloom(short version, bool clear = false)
        // {
        //     var index = Index(version);
        //     try
        //     {
        //         if(!clear)
        //             _sentinelBloom[index / sizeof(uint)] |= (uint) (0x1 << (version % sizeof(uint)));
        //         else
        //             _sentinelBloom[index / sizeof(uint)] &= (uint)~(0x1 << (version % sizeof(uint)));
        //     }
        //     catch (Exception e)
        //     {
        //         Console.WriteLine(e);
        //         throw;
        //     }
        // }

        // [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // private bool Bloomed(short token)
        // {
        //     try
        //     {
        //         var index = Index(token);
        //         try
        //         {
        //             return (_sentinelBloom[index / sizeof(uint)] &= (uint) (0x1 << (token % sizeof(uint)))) > 0;
        //         }
        //         catch (Exception e)
        //         {
        //             Console.WriteLine(e);
        //             Console.WriteLine($"({token}) _sentinelBloom[{(ushort)index}] &= {(uint) (0x1 << (token % sizeof(uint))):x8})");
        //         }
        //
        //         return false;
        //     }
        //     catch (Exception e)
        //     {
        //         Console.WriteLine(e);
        //         throw;
        //     }
        // }
        
        private CancellationToken _asyncToken;

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set()
        {
            var frameId = _frameId;
            //signal
            try
            {
                
                //leaf
                if (_sentinel > 0 && _manualReset.GetStatus((short) _version) < ValueTaskSourceStatus.Succeeded)
                {
                    _manualReset.SetResult(!_asyncToken.IsCancellationRequested);
                    return;
                }
                
                var frame = _sentinelCore[frameId];
                //root
                if (frame.GetStatus(frameId) < ValueTaskSourceStatus.Succeeded)
                {
                    frame.SetResult(!_asyncToken.IsCancellationRequested);
                }
                
                //Console.WriteLine($"SET(({_sentinel}))<{frame.GetWaited()},{frame.GetHooked()}>((({Thread.CurrentThread.ManagedThreadId}){GetHashCode()}){_manualReset.GetHashCode()})> v = {_version}, r = {_sentinelRefCount[_frameId]}, {_sentinelResult[_frameId]}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"SET> v = {_version}, f = {frameId}, m = {GetFrameToken(frameId)}");
                Console.WriteLine(e);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetWaited()
        {
            return _waited;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetWaited()
        {
            _waited = 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHooked()
        {
            return _hooked;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetHooked()
        {
            _hooked = 1;
        }
        

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            var frameId = _frameId;
            try
            {
                //fail fast
                if (_asyncToken.IsCancellationRequested)
                {
                    return ValueTask.FromCanceled<bool>(_asyncToken);
                }

                //current frame 
                var frame = _sentinelCore[frameId];
                
                //fast path
                if (frame.GetStatus(frameId) > ValueTaskSourceStatus.Pending)
                {
                    //Console.WriteLine($"FAST PATH> s = {frame.GetStatus(_frameId)}, v = {_version}, f = {_frameId}, r = {_sentinelRefCount[_frameId]}, {_sentinelResult[_frameId]}");
                    //Reset(_manualReset.Version);
                    return frame.GetResult(frameId) ? _trueSentinel : _falseSentinel;
                }
                
                //Console.WriteLine($"WAITASYNC(({_sentinel}))> s = {frame.GetStatus(_frameId)}, v = {_version}, r = {_sentinelRefCount[_frameId]}, {_sentinelResult[_frameId]}");

                //This is the concurrent case, //TODO
                if (Interlocked.Increment(ref _sentinelRefCount[frameId]) > 1)
                {
                    //throw new NotImplementedException();
                    return _sentinelTask[frameId];//TODO make bucket
                }

                _sentinelCore[frameId].SetWaited();

                var sentinel =  _sentinelTask[frameId];
                
                return sentinel;
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
        public void Zero()
        {
            var frameId = _frameId;
            //Console.WriteLine($"RESET(({_sentinel})) {_frameId}> v = {_frameId}, r = {_sentinelRefCount[_frameId]}]{_sentinelResult[_frameId]}");
            //sentinel
            if (_sentinel > 0)
            {
                if (Interlocked.CompareExchange(ref _versionSet, 1, 0) == 0)
                {
                    _manualReset.Reset();
                    Interlocked.Increment(ref _version);
                    _versionSet = 0;
                }
                else
                {
                    Console.WriteLine($"Sentinel RESET RACE!!!!! fid = {frameId}, v = {_version}");
                }
                return;
            }

            if (Interlocked.CompareExchange(ref _versionSet, 1, 0) ==  0)
            {
                //_frameId = (byte)(1 - _frameId);
                var nextFrameId =(byte) ((_frameId + 1) % BufferSize);
                
                //reset
                //Bloom(_frameId, true);
                _sentinelRefCount[nextFrameId] = 0;
                _sentinelResult[nextFrameId] = -1;
                _sentinelStatus[nextFrameId] = ValueTaskSourceStatus.Pending;
                _sentinelCore[nextFrameId].Zero();
                _hooked = 0;
                _frameId = nextFrameId;
                _continuation = null;
                _state = null;
                _versionSet = 0;
                _continuation = null;
                _state = null;
                _completed = 0;
                Interlocked.Increment(ref _version);
            }
            else
            {
                Console.WriteLine($"Root RESET RACE!!!!! fid = {frameId}, v = {_version}");
            }
        }

        /// <summary>s
        /// Get result
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
            var reqFrame = token;
            ValidateToken(token = GetFrameToken(reqFrame));

            //Console.WriteLine($"get(({_sentinel}))>, t = {token}, r = {_sentinelRefCount[_frameId]}, {_sentinelResult[_frameId]}");

            //roots
            if (_sentinel <= 0)
                throw new NotImplementedException($"We should not get here? s = {_sentinel}, token = {token}");

            //sentinel validation?
            //TODO does this make sense?
            if (token != (short) _version)
            {
                if (token < (short) _version && token > (short) _version - BufferSize)
                {
                    Console.WriteLine($"We are racing...diff = {(short) _version - token}");
                    var cachedVersion = (_frameId + (BufferSize - 1)) % BufferSize;
                    return _sentinelCore[cachedVersion].GetResult((short) (cachedVersion));
                }
                else
                {
                    throw new NotImplementedException(
                        $">2 frame deep race? token = {token}, version = {_version}, diff = {_version - token}");
                }
            }
            
            //teardown once
            // if ((_sentinelResult[reqFrame] = Interlocked.CompareExchange(ref _sentinelResult[reqFrame],
            //     _manualReset.GetResult(token) ? 1 : 0, -1)) == -1)
            if(Interlocked.CompareExchange(ref _completed, 1, 0) == 0)
            {
                //prep buffer
                //_sentinelResult[reqFrame] = _manualReset.GetResult(token) ? 1 : 0;
                
                //Signal one more
                
                // This contraption is not going to work. Stopping work here. Maybe if you new design fails we can try further here:
                // Things we can still try to add is making this _continuation a list of continuations. Then I think we have a manual reset event?
                // But with this it becomes hard to visualize liveness, so one would have to test using formal verification. Regardless, it is clear that
                // one waiter sometimes get left behind and eventually when it gets loose it will cause problems. 
                //
                // The fundamental issue is that ManualResetValueTaskSourceCore is too primitive. It can be made better with 
                // my ByRef trick allowing it to scale horizontally with the amount of waiters without having to 
                // specify (and implement) the max supported waiters in some kind of memory consuming pool (like we did here with BufferSize)
                //
                // ByRef effectively allows us to create a queue of waiters, which means you can (or should be able to) clone AsyncAutoResetEvent
                // almost trivially. Thus we have a struct asyncautoresetevent that gets those juicy zero alloc bonuses from
                // IValueTaskSource, as supposed to having to use any TaskCompletionSources. The only drawback with this new approach will be the mutex init
                // where you have to perform the ByRef call. A oneliner is simply not possible, for inception reasons.
                
                Action<object> c = null;
                var state = _state;
                _state = null;
                if ((c = Interlocked.CompareExchange(ref _continuation, null, _continuation)) != null)
                {
                    c.Invoke(state);
                    //_hooked = 0;
                }
                
                GetSentinelRoot().Zero();
            }
            
            return _sentinelResult[reqFrame] == 1;
            
            // //teardown once
            // if ((_sentinelResult[reqFrame] = Interlocked.CompareExchange(ref _sentinelResult[reqFrame],
            //     _manualReset.GetResult(token) ? 1 : 0, -1)) == -1)
            // {
            //     //prep buffer
            //     _sentinelResult[reqFrame] = _manualReset.GetResult(token) ? 1 : 0;
            //     
            //     //Signal one more
            //     
            //     // This contraption is not going to work. Stopping work here. Maybe if you new design fails we can try further here:
            //     // Things we can still try to add is making this _continuation a list of continuations. Then I think we have a manual reset event?
            //     // But with this it becomes hard to visualize liveness, so one would have to test using formal verification. Regardless, it is clear that
            //     // one waiter sometimes get left behind and eventually when it gets loose it will cause problems. 
            //     //
            //     // The fundamental issue is that ManualResetValueTaskSourceCore is too primitive. It can be made better with 
            //     // my ByRef trick allowing it to scale horizontally with the amount of waiters without having to 
            //     // specify (and implement) the max supported waiters in some kind of memory consuming pool (like we did here with BufferSize)
            //     //
            //     // ByRef effectively allows us to create a queue of waiters, which means you can (or should be able to) clone AsyncAutoResetEvent
            //     // almost trivially. Thus we have a struct asyncautoresetevent that gets those juicy zero alloc bonuses from
            //     // IValueTaskSource, as supposed to having to use any TaskCompletionSources. The only drawback with this new approach will be the mutex init
            //     // where you have to perform the ByRef call. A oneliner is simply not possible, for inception reasons.
            //     
            //     Action<object> c = null;
            //     var state = _state;
            //     _state = null;
            //     if ((c = Interlocked.CompareExchange(ref _continuation, null, _continuation)) != null)
            //     {
            //         c.Invoke(state);
            //         //_hooked = 0;
            //     }
            //     
            //     GetSentinelRoot().Reset();
            // }
            //
            // return _sentinelResult[reqFrame] == 1;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetResult(bool result)
        {
            if(_sentinel > 0)
                _manualReset.SetResult(result);
            else
                throw new NotImplementedException($"We should never set results on the root!");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private short GetFrameToken(short frame)
        {
            return _sentinelCore[frame].Version();
        }
        
        /// <summary>
        /// Get the mutex status
        /// </summary>
        /// <param name="token">current version</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
            var reqFrameId = token;
            ValidateToken(token = GetFrameToken(token));
            
            //sentinel
            try
            {
                if (_sentinel > 0)
                {
                    var status = _manualReset.GetStatus(token);
                    if(status == ValueTaskSourceStatus.Succeeded && Interlocked.CompareExchange(ref _sentinelResult[reqFrameId], _manualReset.GetResult(token) ? 1 : 0, -1 ) == -1 )
                        _sentinelStatus[reqFrameId] = status;
                    return status;
                }
                else
                {
                    throw new NotImplementedException("We should not get here");
                }
                    
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR: GetStatus> t = {token}, v =  {(short)_version}, V = {_manualReset.Version}");
                Console.WriteLine(e);
                //return _manualReset.GetStatus(token);
                return ValueTaskSourceStatus.Pending;
            }
            finally
            {
                //Console.WriteLine($"STAT(({_sentinel}))<{((IoAsyncMutex)_sentinelCore[_frameId]).GetWaited()},{((IoAsyncMutex)_sentinelCore[_frameId]).GetHooked()}>({_manualReset.GetStatus(token)}), v = {_version}, r = {_sentinelRefCount[_frameId]}, {_sentinelResult[_frameId]}");       
            }
        }

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateToken(short token)
        {
            if (token != (short)_version)
            {
                throw new InvalidOperationException($"Invalid state: challenge = {token}, expected {_version}");
            }
        }

        /// <summary>
        /// Signal handler
        /// </summary>
        /// <param name="continuation">The callback</param>
        /// <param name="state">some state</param>
        /// <param name="token">current version</param>
        /// <param name="flags">flags</param>
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]

        volatile int _waited;
        volatile int _hooked;
        private volatile int _version;
        private volatile int _versionSet;
        private volatile byte _frameId;
        private byte _sentinel;
        private int _completed;

        public IoAsyncMutex State => this;
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            var frameId = _frameId;
            ValidateToken(token = GetFrameToken(token));
            
            try
            {
                //Console.WriteLine($"HOOK(({_sentinel}))<{((IoAsyncMutex)_sentinelCore[_frameId]).GetWaited()},{((IoAsyncMutex)_sentinelCore[_frameId]).GetHooked()}>((({Thread.CurrentThread.ManagedThreadId}){GetHashCode()}){_manualReset.GetHashCode()})({state})>[{continuation.Target}] v = {_version}, r = {_sentinelRefCount[_frameId]}, {_sentinelResult[_frameId]}");
                //sentinel
                if (_sentinel > 0)
                {
                    if (_sentinelStatus[frameId] == ValueTaskSourceStatus.Succeeded)
                    {
                        //fast path
                        continuation(state);
                        Console.WriteLine($"OnComplete> FAST PATH! s = {_sentinel}, f ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S(c) = {_sentinelStatus[frameId]}, R(c) = {_sentinelResult[frameId]}");
                    }
                    else if (Interlocked.CompareExchange(ref _hooked, 1, 0) == 0)
                    {
                        try
                        {
                            _manualReset.OnCompleted(continuation, state,token, flags);
                        }
                        catch (InvalidOperationException)
                        {
                            ValueTaskSourceStatus status = ValueTaskSourceStatus.Faulted;

                            try
                            {
                                status = _manualReset.GetStatus(token);
                            }
                            catch 
                            {
                                Console.WriteLine($"ERROR: OnComplete> FAST PATH getting status, s = {_sentinel}, f ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S(c) = {_sentinelStatus[frameId]}, R(c) = {_sentinelResult[frameId]}");
                            }
                                
                            if (_sentinelStatus[frameId] == ValueTaskSourceStatus.Succeeded)
                            {
                                Console.WriteLine($"OnComplete1> FAST PATH! (RACED!!!) s = {_sentinel}, f ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S(c) = {_sentinelStatus[frameId]}, R(c) = {_sentinelResult[frameId]}");
                                continuation(state);
                                Console.WriteLine($"OnComplete2> FAST PATH! (RACED!!!) s = {_sentinel}, f ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S(c) = {_sentinelStatus[frameId]}, R(c) = {_sentinelResult[frameId]}");
                            }
                            else if(status == ValueTaskSourceStatus.Pending)
                            {
                                Console.WriteLine($"OnComplete1> FAST PATH! (Re-/>RACED!!!) s = {_sentinel}, f ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S(c) = {_sentinelStatus[frameId]}, R(c) = {_sentinelResult[frameId]}");
                                _manualReset.OnCompleted(continuation, state,token, flags);
                                Console.WriteLine($"OnComplete2> FAST PATH! (Re-/>RACED!!!) s = {_sentinel}, f ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S(c) = {_sentinelStatus[frameId]}, R(c) = {_sentinelResult[frameId]}");
                            }
                        }
                    }
                    // else if (_sentinelRoot.SetWaiter(continuation, state))
                    // {
                    //     Console.WriteLine($"OnComplete> We raced! s = {_sentinel}, f ={frameId}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S = {_manualReset.GetStatus(_manualReset.Version)}");
                    // }
                    else if(Interlocked.CompareExchange(ref _continuation, continuation, null) == null)
                    {
                        _state = state;
                        //Console.WriteLine($"OnComplete> We raced! s = {_sentinel}, f ={frameId}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S = {_manualReset.GetStatus(_manualReset.Version)}");
                    }
                    else
                    {
                        Console.WriteLine($"OnComplete> We DROPPED! s = {_sentinel}, rf ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, h = {_hooked}, v = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S = {_manualReset.GetStatus(_manualReset.Version)}");
                    }
                    return; 
                }
                //root
                
                _sentinelCore[frameId].OnCompleted(continuation, state, token, flags);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR: OnComplete> target = {continuation?.Target}");
                Console.WriteLine($"ERROR: OnComplete> s = {_sentinel}, f ={frameId}, cf = {_sentinelRoot.GetCurFrame()}, m = {GetFrameToken(frameId)}, V = {_manualReset.Version}, S = {_manualReset.GetStatus(_manualReset.Version)}");
                Console.WriteLine(e);
                throw;
            }
        }
    }
}