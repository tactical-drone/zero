using System;
using System.IO;
using NLog;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.common.trinary.native;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled
{
    /// <summary>
    /// Implements contract <see cref="IIoEntangled{TBlob}"/> depending on whether we are running in <see cref="Optimized"/> mode or not.
    /// If we are not running optimized Tangle.net is used to mock interop functionality.
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoEntangled : IIoEntangled 
    {
        static IoEntangled()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private static Logger _logger;
        private static IIoEntangled _default;

        public static bool Optimized => Environment.OSVersion.Platform == PlatformID.Unix;

        public static IIoEntangled Default
        {
            get
            {
                if (_default != null)
                    return _default;

                //Detect entangled interop
                if (Optimized)
                {
                    var rootFolder = AppContext.BaseDirectory;
                    if (File.Exists(Path.Combine(rootFolder, "libinterop.so")))
                    {
                        _default = (IIoEntangled) new IoEntangledInterop();
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

        public IIoTrinary Ternary { get; } = new IoNativeTrinary();
        public IIoModelDecoder ModelDecoder { get; } = (IIoModelDecoder) new IoNativeModelDecoder();
    }
}
