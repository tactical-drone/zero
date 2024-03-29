﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// Some useful task extensions
    /// </summary>
    public static class TaskExtensions
    {
        /// <summary>
        /// Foreach async
        /// </summary>
        /// <param name="enumerable"></param>
        /// <param name="action"></param>
        /// <param name="nanite"></param>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TN"></typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask ForEachAsync<T,TN>(this List<T> enumerable, Func<T, TN, ValueTask> action, TN nanite = default)
        {
            foreach (var item in enumerable)                
                await Task.Factory.StartNew(static async state =>
                {
                    var (action, item, nanite) = (ValueTuple<Func<T,TN, ValueTask>, T, TN>)state;
                    await action(item,nanite).FastPath();
                }, ValueTuple.Create(action, item,nanite), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }

        /// <summary>
        /// Fast path for value tasks
        /// </summary>
        /// <typeparam name="T">The return type</typeparam>
        /// <param name="task">The value task to be fast pathed</param>
        /// <returns>The result of the async op</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueTask<T> FastPath<T>(this ValueTask<T> task) => task.IsCompletedSuccessfully ? new ValueTask<T>(task.Result) : task;

        /// <summary>
        /// Fast path for value tasks
        /// </summary>
        /// <typeparam name="T">The return type</typeparam>
        /// <param name="task">The value task to be fast pathed</param>
        /// <returns>The result of the async op</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueTask FastPath(this ValueTask task) => task.IsCompletedSuccessfully ? default : task;

        /// <summary>
        /// Block on a token until cancelled (wait one causes problems)
        /// </summary>
        /// <param name="tokenSource">The token to block on</param>
        /// <returns>A ValueTask</returns>
        public static async ValueTask BlockOnNotCanceledAsync(this CancellationTokenSource tokenSource)
        {
            IIoManualResetValueTaskSourceCore<bool> source = new IoManualResetValueTaskSourceCore<bool>();
            var waitForCancellation = new ValueTask<bool>(source, 0);
            var reg = tokenSource.Token.Register(static s =>
            {
                ((IIoManualResetValueTaskSourceCore<bool>)s).SetResult(true);
            }, source);

            await waitForCancellation.FastPath();
            await reg.DisposeAsync().FastPath();
        }        
    }    
}
