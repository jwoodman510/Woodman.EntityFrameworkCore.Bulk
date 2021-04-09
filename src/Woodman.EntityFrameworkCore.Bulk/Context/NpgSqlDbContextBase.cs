using Microsoft.EntityFrameworkCore;

namespace Woodman.EntityFrameworkCore.Bulk
{
    public interface INpgSqlDbContextBase { }

    public class NpgSqlDbContextBase : DbContextBase, INpgSqlDbContextBase
    {
        protected NpgSqlDbContextBase() { }

        protected NpgSqlDbContextBase(DbContextOptions options) : base(options) { }

        protected NpgSqlDbContextBase(string connectionString) : base(connectionString) { }
    }
}
