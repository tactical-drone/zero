using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using zero.core.feat.api;
using zero.tangle.entangled;

namespace zero.tangle.api.controllers.bootstrap
{
    [EnableCors("ApiCorsPolicy")]
    [Route("bootstrap")]
    public class IoBootstrapController: Controller
    {
        public IoBootstrapController()
        {
            
        }

        [HttpGet]
        [Route("kind")]
        public IoApiReturn Kind()
        {
            if (Entangled<string>.Optimized)
                return IoApiReturn.Result(true,"Using interop decoders", "");
            else
                return IoApiReturn.Result(true, "Using native decoders", "/native");
        }
    }
}
