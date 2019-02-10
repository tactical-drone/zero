using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NLog;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "ObjectCreationAsStatement")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class IoItApiCommand
    {
        private static readonly ConcurrentDictionary<string, IoItApiCommand> ApiLookup =
            new ConcurrentDictionary<string, IoItApiCommand>();

        private static readonly Logger Logger;
        public string command;

        static IoItApiCommand()
        {            
            Logger = LogManager.GetLogger(nameof(IoItApiCommand));
        }

        public IoItApiCommand(string key)
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
            return new ErrorResponse {error = $"An undefined error has occurred"};
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