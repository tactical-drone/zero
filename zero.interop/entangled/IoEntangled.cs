using System;
using System.IO;
using NLog;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.native;
using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.common.trinary.native;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled
{
    public class IoEntangled : IIoEntangledInterop
    {
        static IoEntangled()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private static Logger _logger;
        private static IIoEntangledInterop _default;

        public static IIoEntangledInterop Default
        {
            get
            {
                if (_default != null)
                    return _default;

                //Detect entangled interop
                if (Environment.OSVersion.Platform == PlatformID.Unix)
                {
                    var rootFolder = AppContext.BaseDirectory;
                    if (File.Exists(Path.Combine(rootFolder, "libinterop.so")))
                    {
                        _default = new IoEntangledInterop();
                        _logger.Info("Using entangled interop!");
                    }
                    else
                    {
                        _logger.Warn($"`{Path.Combine(rootFolder, "libinterop.so")}' not found, falling back to native decoders");
                        _default = new IoEntangled();
                    }
                }
                else //fallback to Tangle.Net
                {                    
                    _default = new IoEntangled();
                    _logger.Warn("Interop with entangled is not supported in windows, falling back to native decoders");
                }

                return _default;
            }            
        } 

        public IIoTrinary Trinary { get; } = new IoNativeTrinary();
        public IIoInteropModel Model { get; } = new IoNativeModel();
    }
}
