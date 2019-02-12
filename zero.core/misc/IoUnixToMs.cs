using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;

namespace zero.core.misc
{
    public static class IoUnixToMs
    {        
        public static DateTimeOffset DateTime(this long timestamp)
        {
            if (timestamp <= 253402300799)
                return DateTimeOffset.FromUnixTimeSeconds(timestamp);
            else
                return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
        }

        public static long NormalizeDateTime(this long timestamp)
        {
            return timestamp.DateTime().ToUnixTimeMilliseconds();            
        }
    }
}
