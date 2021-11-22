using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using zero.cocoon.events.services;


namespace zero.sync
{
    class StartServices
    {
        public StartServices(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            //services.AddAuthentication()
            //    .AddCookie(cfg => cfg.SlidingExpiration = true)
            //    .AddJwtBearer(cfg =>
            //    {
            //        cfg.SaveToken = true;
            //        cfg.TokenValidationParameters = new TokenValidationParameters()
            //        {
            //            ValidIssuer = Configuration["Tokens:Issuer"],
            //            ValidAudience = Configuration["Tokens:Issuer"],
            //            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(Configuration["Tokens:Key"]))
            //        };
            //    });

            Configuration["Kestrel"] =
                $"{{\r\n  \"Kestrel\": {{\r\n    \"Limits\": {{\r\n      \"MaxConcurrentConnections\": 100,\r\n      \"MaxConcurrentUpgradedConnections\": 100\r\n    }},\r\n    \"DisableStringReuse\": true\r\n  }}\r\n}}";

            services.AddGrpc();

            //services.AddCors(options =>
            //{
            //    options.AddPolicy("ApiCorsPolicy",
            //        //builder => builder.SetIsOriginAllowed(s => s.Contains("https://localhost"))
            //        builder => builder.AllowAnyOrigin()
            //            .AllowAnyMethod()
            //            .AllowAnyHeader());
            //});

            //services.AddMvc()
            //    .SetCompatibilityVersion(CompatibilityVersion.Version_3_0)
            //    .AddMvcOptions(options => options.EnableEndpointRouting = false);

            //.AddApplicationPart(typeof(IIoNodeController).GetTypeInfo().Assembly);

            //.AddJsonOptions(opts =>
            //{
            //    opts.SerializerSettings.ContractResolver = new DefaultContractResolver()
            //    {
            //        NamingStrategy = new DefaultNamingStrategy()
            //    };
            //});

            //Add node services            

            //services.AddSingleton(new IoBootstrapController());
            //services.AddSingleton(new IoInteropServicesController());
            //services.AddSingleton(new IoNativeServicesController());

        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();
            app.UseEndpoints(builder =>
            {
                builder.MapGrpcService<AutoPeeringEventService>();
            });
        }
    }
}
