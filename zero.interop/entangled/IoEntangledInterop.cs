using System;
using System.Collections.Generic;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled
{
    public class IoEntangledInterop : IIoEntangledInterop
    {                
        public IIoTrinary Trinary { get; } = new IoInteropTrinary();
        public IIoInteropModel Model { get; } = new IoInteropModel();
    }
}
