using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using zero.core.patterns.queue;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// zero base
    /// </summary>
    public interface IIoNanite : IEquatable<IIoNanite>
    {
        /// <summary>
        /// returns an identity
        /// </summary>
        long Serial { get; }
        
        /// <summary>
        /// The source of zero
        /// </summary>
        IIoNanite ZeroedFrom { get; }
        
        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        ValueTask Zero(IIoNanite @from, string reason, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default);
        
        /// <summary>
        /// A description of this object
        /// </summary>
        string Description { get; }

        /// <summary>
        /// Indicate zero status
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool Zeroed();

        /// <summary>
        /// Allow for hive like behaviour
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">If the zeroing is both ways</param>
        ValueTask<(TBase target, bool success, IoQueue<IIoNanite>.IoZNode sub)> ZeroHiveAsync<TBase>(TBase target,
            bool twoWay = false) where TBase : IIoNanite;

        /// <summary>
        /// Subscribe to zeroed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <param name="closureState">Closure state</param>
        /// <param name="filePath"></param>
        /// <param name="memberName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>The handler</returns>
        ValueTask<IoQueue<IoZeroSub>.IoZNode> ZeroSubAsync<T>(Func<IIoNanite, T, ValueTask<bool>> sub,
            T closureState = default,
            [CallerFilePath] string filePath = null, [CallerMemberName] string memberName = null,
            [CallerLineNumber] int lineNumber = 0);


        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        public ValueTask<bool> UnsubscribeAsync(IoQueue<IoZeroSub>.IoZNode sub);

        /// <summary>
        /// Ensures that this action is synchronized 
        /// </summary>
        /// <param name="ownershipAction">The action to synchronise</param>
        /// <param name="userData">Some user data passed to <see cref="ownershipAction"/></param>
        /// <param name="disposing">If disposing</param>
        /// <param name="force">Forces the action regardless of zero state</param>
        /// <returns>true on success, false otherwise</returns>
        //ValueTask<bool> ZeroAtomicAsync<T>(Func<IIoNanite, T, bool, ValueTask<bool>> ownershipAction,T userData = default,bool disposing = false, bool force = false);
        ValueTask<bool> ZeroAtomicAsync<T>(Func<IIoNanite, T, bool, ValueTask<bool>> ownershipAction, T userData = default, bool disposing = false, bool force = false);

        /// <summary>
        /// Collect unmanaged resources
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ZeroUnmanaged();

        /// <summary>
        /// Collect managed resources
        /// </summary>
        /// <returns></returns>
        ValueTask ZeroManagedAsync();

        /// <summary>
        /// Returns the objects concurrency level
        /// </summary>
        /// <returns></returns>
        int ZeroConcurrencyLevel();


        /// <summary>
        /// Return the hive mind
        /// </summary>
        /// <returns>The hive</returns>
        IoQueue<IIoNanite> ZeroHiveMind();

        /// <summary>
        /// Return the hive mind
        /// </summary>
        /// <returns>The hive</returns>
        IoQueue<IoZeroSub> ZeroHive();

        /// <summary>
        /// Cancels all processing
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ZeroPrime();
    }
}