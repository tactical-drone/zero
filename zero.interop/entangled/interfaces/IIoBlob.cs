using System;
using System.Collections.Generic;
using System.Text;

namespace zero.interop.entangled.interfaces
{
    public interface IIoBlob
    {
        ReadOnlyMemory<byte> Blob { get; set; }
    }
}
