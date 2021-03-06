using System;
using System.IO;
using NLog;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.common.trinary.native;
using zero.interop.entangled.interfaces;
using zero.tangle.entangled.common.model.native;

namespace zero.tangle.entangled
{
    /// <summary>
    /// Implements contract <see cref="IIoEntangled{TKey}"/> depending on whether we are running in <see cref="Optimized"/> mode or not.
    /// If we are not running optimized Tangle.net is used to mock interop functionality.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class Entangled<TKey> : IIoEntangled<TKey>
    {
        static Entangled()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private static Logger _logger;
        private static IIoEntangled<TKey> _default;

        public static bool Optimized => Environment.OSVersion.Platform == PlatformID.Unix;

        public static IIoEntangled<TKey> Default
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
                        _default = (IIoEntangled<TKey>) new EntangledInterop();
                        _logger.Info("Using entangled interop!");
                    }
                    else
                    {
                        _logger.Warn($"`{Path.Combine(rootFolder, "libinterop.so")}' not found, falling back to native decoders");
                        _default = new Entangled<TKey>();
                    }
                }
                else //fallback to Tangle.Net
                {                    
                    _default = new Entangled<TKey>();
                    _logger.Warn("Interop with entangled is not supported in windows, falling back to native decoders");
                }

                return _default;
            }            
        } 

        public IIoTrinary Ternary { get; } = new IoNativeTrinary();
        public IIoModelDecoder<TKey> ModelDecoder { get; } = (IIoModelDecoder<TKey>) new TangleNetDecoder();
    }
}
