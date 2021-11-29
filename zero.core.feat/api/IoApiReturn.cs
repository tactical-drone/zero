using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace zero.core.feat.api
{
    public class IoApiReturn //: IActionResult
    {
        public static IoApiReturn Result(bool success, string message = null, object rows = null, long time = 0)
        {
            return new IoApiReturn{Success = success, Time = time, Message = message, Rows = rows};
        }

        public bool Success;
        public string Message;
        public long Time;
        public object Rows;

        public Task ExecuteResultAsync(ActionContext context)
        {
            return Task.FromResult(Task.CompletedTask);
        }
    }
}
