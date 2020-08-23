using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NLog;

namespace zero.core.data.market
{
    /// <summary>
    /// Fetches market data from crypto compare
    /// </summary>
    public static class IoMarketDataClient
    {
        static IoMarketDataClient()
        {
            Logger = LogManager.GetCurrentClassLogger();
            HttpClient.DefaultRequestHeaders.AcceptEncoding.Clear();
            HttpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            HttpClient.DefaultRequestHeaders.Add("User-Agent", "zero");

            Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(55)).Subscribe(async _ =>
                {
                    var newData = await FetchData().ConfigureAwait(false);
                    if (newData != null)
                        CurrentData = newData;
                });
        }

        private static readonly Logger Logger;
        private static readonly HttpClient HttpClient = new HttpClient();
        private static DateTime _lastFetchTime = DateTime.Now;
        public static double BundleSize = 1000000.0;

        public static IoCryptoCompareMarketData CurrentData = new IoCryptoCompareMarketData();
        public static volatile short Quality = short.MaxValue;
        
        static async Task<IoCryptoCompareMarketData> FetchData()
        {
            var fetch = await HttpClient.GetStringAsync("https://min-api.cryptocompare.com/data/pricemultifull?fsyms=IOT&tsyms=USD,EUR,BTC,ETH").ContinueWith(
                response =>
                {
                    switch (response.Status)
                    {
                        case TaskStatus.Canceled:                            
                        case TaskStatus.Faulted:
                            Logger.Trace(response.Exception, "Unable to fetch iota market data:");
                            Quality = (short) (DateTime.Now - _lastFetchTime).TotalMinutes;
                            break;
                        case TaskStatus.RanToCompletion:
                            Quality = 0;
                            _lastFetchTime = DateTime.Now;
                            return JsonConvert.DeserializeObject<IoCryptoCompareMarketData>(response.Result);                            
                    }

                    return null;
                }).ConfigureAwait(false);
            return fetch;
        }
    }
}
