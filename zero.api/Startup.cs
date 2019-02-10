using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using zero.tangle.api.controllers.bootstrap;
using zero.tangle.api.controllers.services;


namespace zero.api
{
    /// <summary>
    /// Starts asp.net core
    /// </summary>
    public class Startup
    {
        public Startup(IConfiguration configuration)
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

            services.AddCors(options =>
            {
                options.AddPolicy("ApiCorsPolicy",
                    //builder => builder.SetIsOriginAllowed(s => s.Contains("https://localhost"))
                    builder => builder.AllowAnyOrigin()
                        .AllowAnyMethod()
                        .AllowAnyHeader());
            });

            services.AddMvc()
                .SetCompatibilityVersion(CompatibilityVersion.Version_3_0);
            //.AddApplicationPart(typeof(IIoNodeController).GetTypeInfo().Assembly);

            //.AddJsonOptions(opts =>
            //{
            //    opts.SerializerSettings.ContractResolver = new DefaultContractResolver()
            //    {
            //        NamingStrategy = new DefaultNamingStrategy()
            //    };
            //});

            //Add node services            
            services.AddSingleton(new IoBootstrapController());
            services.AddSingleton(new IoInteropServicesController());
            services.AddSingleton(new IoNativeServicesController());
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseCors("ApiCorsPolicy");            
            app.UseMvc();
        }
    }
}
