using Woodman.EntityFrameworkCore.Bulk;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class SqlContext : DbContextBase
    {
        public SqlContext() { }

        public SqlContext(string connectionString):base(connectionString) { }

        public void TestConfig(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
        }
    }

    public class PostgresContext : NpgSqlDbContextBase
    {
        public PostgresContext(string connectionString) : base(connectionString) { }

        public void TestConfig(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
        }
    }

    public class DbContextTests
    {
        [Fact(DisplayName = "Uses SQL Server by Default")]
        public void DefaultsToSql()
        {
            using var db = new SqlContext("abc");

            var optionsBuilder = new DbContextOptionsBuilder();

            db.TestConfig(optionsBuilder);

            Assert.True(optionsBuilder.IsConfigured);
            Assert.True(db.Database.IsSqlServer());
        }

        [Fact(DisplayName = "NpgSqlDbContextBase Uses NpgSQL")]
        public void UsesNpgSql()
        {
            var db = new PostgresContext("abc");

            var optionsBuilder = new DbContextOptionsBuilder();

            db.TestConfig(optionsBuilder);

            Assert.True(optionsBuilder.IsConfigured);
            Assert.True(db.Database.IsNpgsql());
        }
    }
}
