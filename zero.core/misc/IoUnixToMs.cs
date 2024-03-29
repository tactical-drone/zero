﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace zero.core.misc
{
    public static class IoUnixToMs
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTimeOffset DateTime(this long timestamp)
        {
            if (timestamp <= 253402300799 && timestamp >= -62135596800)
                return DateTimeOffset.FromUnixTimeSeconds(timestamp);
            else
            {                
                return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);                
            }
                
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long NormalizeDateTime(this long timestamp)
        {
            return timestamp.DateTime().ToUnixTimeMilliseconds();            
        }


        /// <summary>
        /// Delta time since in unix time
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ElapsedDelta(this long timestamp)
        {
            var delta = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - timestamp ;
            if (delta < 0)
                return -delta;
            return delta;
        }

        /// <summary>
        /// Delta time since in unix time
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ElapsedDeltaMs(this long timestamp)
        {
            var delta = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - timestamp;
            if (delta < 0)
                return -delta;
            return delta;
        }

        /// <summary>
        /// Delta time since in unix time
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long CurrentUtcMsDelta(this long timestamp)
        {
            var delta = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - timestamp ;
            if (delta < 0)
                return -delta;
            return delta;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ElapsedMs(this IoInt32 timestamp)
        {
            return Environment.TickCount - timestamp;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ElapsedMs(this int timestamp)
        {
            return unchecked(Environment.TickCount - timestamp);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ElapsedMsToSec(this int timestamp)
        {
            return unchecked(Environment.TickCount - timestamp) / 1000;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ElapsedUtc(this long timestamp)
        {
            return DateTimeOffset.UtcNow.ToUnixTimeSeconds() - timestamp;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ElapsedUtcMs(this long timestamp)
        {
            return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - timestamp;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ElapsedMs(this long timestamp)
        {
            return Environment.TickCount - timestamp;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ElapsedUtcMsToSec(this long timestamp)
        {
            return timestamp.ElapsedUtcMs() / 1000;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ElapsedMsToSec(this long timestamp)
        {
            return timestamp.ElapsedMs()/1000;
        }

        /// <summary>
        /// Delta ticks
        /// </summary>
        /// <param name="ticks"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long DeltaTicks(this long ticks)
        {
            var delta = System.DateTime.UtcNow.Ticks - ticks;
            if (delta < 0)
                return -delta;
            return delta;
        }

        /// <summary>
        /// Delta ticks
        /// </summary>
        /// <param name="ticks"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long DeltaTickSeconds(this long ticks)
        {
            var delta = System.DateTime.UtcNow.Ticks - ticks;
            if (delta < 0)
                return -delta;
            return delta / 10000000;
        }

        /// <summary>
        /// Delta ticks
        /// </summary>
        /// <param name="ticks"># of ticks</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TimeSpan TicksToSpan(this long ticks)
        {
            return TimeSpan.FromTicks(ticks);
        }
        
        /// <summary>
        /// Delta ticks
        /// </summary>
        /// <param name="ticks"># of ticks</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double TickSec(this long ticks)
        {
            return ticks.DeltaTicks().TicksToSpan().TotalSeconds;
        }
        
        /// <summary>
        /// Delta ticks
        /// </summary>
        /// <param name="ticks"># of ticks</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double TickMs(this long ticks)
        {
            return ticks.DeltaTicks().TicksToSpan().TotalMilliseconds;
        }
        
    }
}
