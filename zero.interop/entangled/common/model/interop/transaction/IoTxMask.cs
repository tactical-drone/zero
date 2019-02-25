using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace zero.interop.entangled.common.model.interop.transaction
{
    [StructLayout(LayoutKind.Sequential)]
    public struct IoTxMask
    {
        short essence;
        short attachment;
        short consensus;
        short data;
        short metadata;
    }
}
