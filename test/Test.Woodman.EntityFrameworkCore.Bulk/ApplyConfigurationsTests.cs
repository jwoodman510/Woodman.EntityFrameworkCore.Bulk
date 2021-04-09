using Woodman.EntityFrameworkCore.Bulk;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System.Linq;
using Xunit;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class ApplyConfigurationsTests
    {
        private const int ExpectedDefaultValue = 112354;

        [Fact]
        public void AppliesConfiguration()
        {
            using(var db = new TestContext(new DbContextOptionsBuilder().UseInMemoryDatabase("test").Options))
            {
                var entityType = db.Model.FindEntityType(typeof(TestEntity));

                var annotation = entityType.FindAnnotation(ExpectedDefaultValue.ToString());

                Assert.NotNull(annotation);
            }
        }

        private class TestContext : DbContext
        {
            public TestContext(DbContextOptions options) : base(options) { }

            public virtual DbSet<TestEntity> Entities { get; set; }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                this.ApplyConfigurations(modelBuilder);
            }
        }

        public class TestEntityConfiguration : IEntityTypeConfiguration<TestEntity>
        {
            public void Configure(EntityTypeBuilder<TestEntity> builder)
            {
                builder.HasKey(e => e.Id);

                builder.HasAnnotation(ExpectedDefaultValue.ToString(), ExpectedDefaultValue);
            }
        }
    }
}
