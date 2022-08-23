using System.Threading.Tasks;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Text.Json;
using zero.core.feat.api.commands;

namespace zero.api.Controllers
{
    [EnableCors("ApiCorsPolicy")]
    //[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    [Route("/wallet")]
    [ApiController]
    public class ApiController : Controller
    {
        private readonly ILogger<ApiController> _logger;

        public ApiController(ILogger<ApiController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public JsonResult Get()
        {
            return new JsonResult("You will be assimilated!");            
        }                

        [HttpPost]
        public async Task<JsonResult> PostAsync([FromBody]JsonDocument command)
        {
            var commandTask = IoItApiCommand.ProcessAsync(command);

            try
            {
                await commandTask;
            }
            catch
            {
                // ignored
            }

            switch (commandTask.Status)
            {
                case TaskStatus.RanToCompletion:
                    return Json(commandTask.Result);
                case TaskStatus.Canceled:
                    return Json(new IoItApiCommand.ErrorResponse { error = "Canceled" });
                case TaskStatus.Faulted:
                    return Json(new IoItApiCommand.ErrorResponse { error = $"Failed: {commandTask.Exception.Message}" });
            }

            return Json(new IoItApiCommand.ErrorResponse { error = $"An undefined error did occur ({commandTask.Status})" });
        }
    }
}
