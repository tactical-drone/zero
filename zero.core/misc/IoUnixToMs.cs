using System;
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
        public static long UtDelta(this long timestamp)
        {
            var delta = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - timestamp ;
            if (delta < 0)
                return -delta;
            return delta;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long Elapsed(this long timestamp)
        {
            return DateTimeOffset.UtcNow.ToUnixTimeSeconds() - timestamp;
        }
    }
}
