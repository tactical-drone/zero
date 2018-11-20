using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace zero.web.routers
{
    public interface IAureliaRouteProvider
    {
        string Area { get; }

        IEnumerable<AureliaRoute> Routes { get; }
    }

    public class AureliaRouteProvider : IAureliaRouteProvider
    {
        public string Area => "Admin";

        public IEnumerable<AureliaRoute> Routes
        {
            get
            {
                var routes = new List<AureliaRoute>();

                routes.Add(new AureliaRoute
                {
                    Route = "",
                    Name = "index",
                    ModuleId = "/aurelia-app/index",
                    Nav = true,
                    Title = "Home"
                });
                routes.Add(new AureliaRoute
                {
                    Route = "flickr",
                    Name = "flickr",
                    ModuleId = "/aurelia-app/flickr",
                    Nav = true,
                    Title = "Flickr"
                });

                return routes;
            }
        }
    }
}
