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
            if (IoEntangled.Optimized)
                return IoApiReturn.Result(true,"Using interop decoders", "");
            else
                return IoApiReturn.Result(true, "Using native decoders", "/native");
        }
    }
}
