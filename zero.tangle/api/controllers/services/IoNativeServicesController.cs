using Microsoft.AspNetCore.Mvc;
using zero.tangle.api.controllers.generic;

namespace zero.tangle.api.controllers.services
{
    [Route("native/services/node")]
    public class IoNativeServicesController :IoNodeServices<string>
    {
    }
}
