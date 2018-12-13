using System;
using System.Runtime.InteropServices;

namespace zero.interop
{
    public static class IoLib
    {
        //Constants from /usr/include/bits/dlfcn.h
        public const int RtldLazy   = 0x00001; //Only resolve symbols as needed
        public const int RtldNow    = 0x00002; //Only resolve symbols as needed
        public const int DeepBind   = 0x00008; //Only resolve symbols as needed
        public const int RtldGlobal = 0x00100; //Make symbols available to libraries loaded later

        [DllImport("dl")]
        public static extern IntPtr dlopen(string file, int mode);
    }
}
