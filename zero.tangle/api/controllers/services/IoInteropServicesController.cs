using Microsoft.AspNetCore.Mvc;
using zero.tangle.api.controllers.generic;

namespace zero.tangle.api.controllers.services
{
    [Route("services/node")]
    public class IoInteropServicesController: IoNodeServices<byte[]>
    {
    }
}
