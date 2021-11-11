using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// zero base
    /// </summary>
    public interface IIoNanite : IEquatable<IIoNanite>
    {
        /// <summary>
        /// Async construction
        /// </summary>
        /// <returns>True if success, false otherwise</returns>
        ValueTask<bool> ConstructAsync();

        /// <summary>
        /// returns an identity
        /// </summary>
        ulong SerialNr { get; }
        
        /// <summary>
        /// The source of zero
        /// </summary>
        IIoNanite ZeroedFrom { get; }
        
        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        ValueTask<bool> ZeroAsync(IIoNanite from);
        
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
        ValueTask<(TBase target, bool success)> ZeroHiveAsync<TBase>(TBase target, bool twoWay = false) where TBase : IIoNanite;

        /// <summary>
        /// Subscribe to zeroed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <param name="closureState">Closure state</param>
        /// <param name="filePath"></param>
        /// <param name="memberName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>The handler</returns>
        ValueTask<LinkedListNode<IoZeroSub>> ZeroSubAsync<T>(Func<IIoNanite, T, ValueTask<bool>> sub,
            T closureState = default,
            [CallerFilePath] string filePath = null, [CallerMemberName] string memberName = null,
            [CallerLineNumber] int lineNumber = 0);


        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        public ValueTask<bool> UnsubscribeAsync(LinkedListNode<IoZeroSub> sub);

        /// <summary>
        /// Ensures that this action is synchronized 
        /// </summary>
        /// <param name="ownershipAction">The action to synchronise</param>
        /// <param name="userData">Some user data passed to <see cref="ownershipAction"/></param>
        /// <param name="disposing">If disposing</param>
        /// <param name="force">Forces the action regardless of zero state</param>
        /// <returns>true on success, false otherwise</returns>
        //ValueTask<bool> ZeroAtomicAsync<T>(Func<IIoNanite, T, bool, ValueTask<bool>> ownershipAction,T userData = default,bool disposing = false, bool force = false);
        [MethodImpl(MethodImplOptions.Synchronized | MethodImplOptions.AggressiveInlining)]
        bool ZeroAtomic<T>(Func<IIoNanite, T, bool, ValueTask<bool>> ownershipAction, T userData = default, bool disposing = false, bool force = false);

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
        LinkedList<IIoNanite> ZeroHiveMind();

        /// <summary>
        /// Maintain zero context
        /// </summary>
        bool Zc { get; }
    }
}