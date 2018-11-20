using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace zero.web.routers
{
    public struct AureliaRoute
    {
        public string Route { get; set; }

        public string Name { get; set; }

        public string ModuleId { get; set; }

        public object Nav { get; set; }

        public string Title { get; set; }
    }
}
