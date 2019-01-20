using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.data.market
{
    /// <summary>
    /// The model for crypto compare market data
    /// </summary>
    public class IoTaMarketSymbolData
    {
        public IoTMarketTickers Iot { get; set; } = new IoTMarketTickers();
    }
}
