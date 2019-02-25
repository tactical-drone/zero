using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace zero.interop.entangled.common.model.interop.transaction
{
    [StructLayout(LayoutKind.Sequential)]
    public struct IoTxMetadata
    {
        public long snapshot_index;

        [MarshalAs(UnmanagedType.I1)]
        public bool solid;

        public long arrival_timestamp;
    }
}
