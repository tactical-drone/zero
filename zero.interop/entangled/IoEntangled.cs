using System;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.native;
using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.common.trinary.native;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled
{
    public class IoEntangled : IIoEntangledInterop
    {
        private static IIoEntangledInterop _default;
        public static IIoEntangledInterop Default => _default ?? (_default = Environment.OSVersion.Platform == PlatformID.Unix
                                                         ? (IIoEntangledInterop) new IoEntangledInterop()
                                                         : new IoEntangled());

        public IIoTrinary Trinary { get; } = new IoNativeTrinary();
        public IIoInteropModel Model { get; } = new IoNativeModel();
    }
}
