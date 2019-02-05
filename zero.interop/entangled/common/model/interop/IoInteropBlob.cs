using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled.common.model.interop
{
    public class IoInteropBlob : IIoBlob
    {
        public ReadOnlyMemory<byte> Blob { get; set; }
    }
}
