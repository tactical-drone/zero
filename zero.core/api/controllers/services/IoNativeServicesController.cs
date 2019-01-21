using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using zero.core.api.controllers.generic;

namespace zero.core.api.controllers.services
{
    [Route("native/services/node")]
    public class IoNativeServicesController :IoNodeServices<string>
    {
    }
}
