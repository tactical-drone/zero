namespace zero.core.feat.data.market;

/// <summary>
///     The model for crypto compare market data
/// </summary>
public class IoTMarketTickers
{
    public IoMarketDataModel Btc { get; set; } = new();
    public IoMarketDataModel Eth { get; set; } = new();
    public IoMarketDataModel Eur { get; set; } = new();
    public IoMarketDataModel Usd { get; set; } = new();
}