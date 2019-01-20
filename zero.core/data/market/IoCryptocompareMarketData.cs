using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.data.market
{
    /// <summary>
    /// The model for crypto compare market data
    /// </summary>
    public class IoCryptoCompareMarketData
    {
        public IoTaMarketSymbolData Raw { get; set; } = new IoTaMarketSymbolData();
    }
}
