using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NLog;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "ObjectCreationAsStatement")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class ApiCommand
    {
        private static readonly ConcurrentDictionary<string, ApiCommand> ApiLookup =
            new ConcurrentDictionary<string, ApiCommand>();

        private static readonly Logger Logger;
        public string command;

        static ApiCommand()
        {
            new getNodeInfo();
            new getNeighbors();
            new addNeighbors();
            new removeNeighbors();
            new getTips();
            new findTransactions();
            new getTrytes();
            new getInclusionStates();
            new getBalances();
            new getTransactionsToApprove();
            new attachToTangle();
            new interruptAttachingToTangle();
            new broadcastTransactions();
            new storeTransactions();
            Logger = LogManager.GetLogger(nameof(ApiCommand));
        }

        public ApiCommand(string key)
        {
            ApiLookup.TryAdd(key, this);
        }

        public static async Task<ResponseBase> Process(JObject jsonCommandObject)
        {
            Logger.Trace($"API message received\n{jsonCommandObject}");

            var key = jsonCommandObject.GetValue("command").ToString();
            return ApiLookup.ContainsKey(key)
                ? await ApiLookup[key].ProcessCommand(jsonCommandObject)
                : new ErrorResponse {error = $"'{key}' parameter has not been specified"};
        }

#pragma warning disable 1998
        public virtual async Task<ResponseBase> ProcessCommand(JObject jsonCommandobJObject)
#pragma warning restore 1998
        {
            return new ErrorResponse {error = $"An undefined error has occured"};
        }

        public class ResponseBase
        {
        }

        public class ErrorResponse : ResponseBase
        {
            public string error;
        }
    }
}