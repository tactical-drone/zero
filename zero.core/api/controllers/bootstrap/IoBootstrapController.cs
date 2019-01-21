﻿using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using zero.interop.entangled;

namespace zero.core.api.controllers.bootstrap
{
    [EnableCors("ApiCorsPolicy")]    
    [ApiController]
    [Route("bootstrap")]
    public class IoBootstrapController:Controller
    {
        public IoBootstrapController()
        {
            
        }

        [HttpGet]
        [Route("kind")]
        public IoApiReturn Kind()
        {
            if (IoEntangled<object>.Optimized)
                return IoApiReturn.Result(true,"Using interop decoders", "");
            else
                return IoApiReturn.Result(true, "Using native decoders", "/native");
        }
    }
}