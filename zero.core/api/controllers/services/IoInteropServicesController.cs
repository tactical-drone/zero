using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using zero.core.api.controllers.generic;

namespace zero.core.api.controllers.services
{
    [Route("services/node")]
    public class IoInteropServicesController: IoNodeServices<byte[]>
    {
    }
}
