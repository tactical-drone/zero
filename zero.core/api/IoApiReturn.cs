using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal;

namespace zero.core.api
{
    public class IoApiReturn //: IActionResult
    {
        public static IoApiReturn Result(bool success, string message = null, object rows = null)
        {
            return new IoApiReturn{Success = success, Message = message, Rows = rows};
        }

        public bool Success;
        public string Message;
        public object Rows;

        public Task ExecuteResultAsync(ActionContext context)
        {
            return Task.FromResult(Task.CompletedTask);
        }
    }
}
