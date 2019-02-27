using System;

namespace zero.interop.entangled.interfaces
{
    public interface IIoBlob
    {
        ReadOnlyMemory<byte> key { get; set; }
    }
}
