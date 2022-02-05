﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace zero.@unsafe.core.math
{
    public static class IoMath
    {
        public static unsafe int ByPtr754_64(ulong bits)
        {
            var fp = (double)bits;
            return ((int)(*(ulong*)&fp >> 52) & 2047) - 1023;
        }

        const int Fp64Prec = 53;

        static int[] CreateTableMix()
        {
            var ret = new int[1 << (64 - Fp64Prec)];
            for (var i = ret.Length; --i >= 0;)
            {
                ret[i] = ByPtr754_64((uint)i) + Fp64Prec;
            }
            return ret;
        }

        private static readonly int[] TableMix = CreateTableMix();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Log2(ulong bits)
        {
            int r;
            return (r = TableMix[bits >> Fp64Prec]) > 0 ? r : ByPtr754_64(bits);
        }
    }
}
