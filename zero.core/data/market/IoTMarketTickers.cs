using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.data.market
{
    /// <summary>
    /// The model for crypto compare market data
    /// </summary>
    public class IoTMarketTickers
    {
        public IoMarketDataModel Btc { get; set; } = new IoMarketDataModel();
        public IoMarketDataModel Eth { get; set; } = new IoMarketDataModel();
        public IoMarketDataModel Eur { get; set; } = new IoMarketDataModel();
        public IoMarketDataModel Usd { get; set; } = new IoMarketDataModel();
    }
}
