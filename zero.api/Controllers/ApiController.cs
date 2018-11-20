using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using zero.core.api.commands;
using zero.core.patterns;
using zero.core.patterns.misc;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Razor.Language.Intermediate;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using NLog;

namespace zero.api.Controllers
{
    [ApiController]
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    [Route("/api")]
    public class ApiController : Controller
    {
        readonly Logger _logger;

        public ApiController()
        {
            //TODO fix this
            _logger = LogManager.GetCurrentClassLogger();
        }

        [HttpGet]
        public JsonResult Get()
        {
            return new JsonResult("You will be assimilated!");            
        }                

        //[HttpGet("{id}")]
        //public string Get(int id)
        //{
        //    return "value";
        //}

        //[HttpPost]
        //public string Post([FromBody]JObject command)
        //{
        //    Logger.Debug("Received command: {0}", command.GetValue("command").ToString());
        //    return command.ToString();
        //}

        [HttpPost]
        public async Task<JsonResult> Post([FromBody]JObject command)
        {
            var commandTask = ApiCommand.Process(command);

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
                    return Json(new ApiCommand.ErrorResponse { error = "Canceled" });
                case TaskStatus.Faulted:
                    return Json(new ApiCommand.ErrorResponse { error = $"Failed: {commandTask.Exception.Message}" });
            }

            return Json(new ApiCommand.ErrorResponse { error = $"An undefined error did occur ({commandTask.Status})" });
        }

        //[HttpPost]
        //public IActionResult Post([FromBody]getNodeInfo value)
        //{
        //    Logger.Debug("getNodeInfo");
        //    return Json(value);
        //}

        //// PUT api/values/5
        //[HttpPut("{id}")]
        //public void Put(int id, [FromBody]string value)
        //{
        //}

        //// DELETE api/values/5
        //[HttpDelete("{id}")]
        //public void Delete(int id)
        //{
        //}
    }
}
