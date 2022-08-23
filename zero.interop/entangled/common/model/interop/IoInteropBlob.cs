using System;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled.common.model.interop
{
    public class IoInteropkey : IIoBlob
    {
        public ReadOnlyMemory<byte> key { get; set; }
    }
}
