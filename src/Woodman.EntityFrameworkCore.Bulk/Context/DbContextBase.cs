using Microsoft.EntityFrameworkCore;

namespace Woodman.EntityFrameworkCore.Bulk
{
    public partial class DbContextBase : DbContext
    {
        private readonly string _connectionString;

        protected DbContextBase() { }

        protected DbContextBase(DbContextOptions options) : base(options) { }

        protected DbContextBase(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (optionsBuilder.IsConfigured)
                return;

            optionsBuilder.UseLoggerFactory(DbLoggingConfiguration.LoggerFactor);

            if (this is INpgSqlDbContextBase)
            {
                optionsBuilder.UseNpgsql(_connectionString, o =>
                {
                    o.EnableRetryOnFailure();
                });
            }
            else
            {
                optionsBuilder.UseSqlServer(_connectionString, o =>
                {
                    o.EnableRetryOnFailure();
                });
            }
        }
    }
}
 