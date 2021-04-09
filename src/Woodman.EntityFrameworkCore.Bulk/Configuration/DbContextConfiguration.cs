using Microsoft.Extensions.Logging;
using System;

namespace Woodman.EntityFrameworkCore.Bulk
{
    public class DbContextConfiguration
    {
        public LoggerFactory Logger => DbLoggingConfiguration.LoggerFactor;

        public static DbContextConfiguration Build(Action<DbContextConfiguration> configure = null)
        {
            var config = new DbContextConfiguration();

            configure?.Invoke(config);

            return config;
        }
    }
}
