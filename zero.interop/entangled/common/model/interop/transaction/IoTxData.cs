﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using zero.interop.entangled.common.trinary;

namespace zero.interop.entangled.common.model.interop.transaction
{
    [StructLayout(LayoutKind.Sequential)]
    public struct IoTxData
    {
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_6561)]
        public byte[] signature_or_message;
    }
}
