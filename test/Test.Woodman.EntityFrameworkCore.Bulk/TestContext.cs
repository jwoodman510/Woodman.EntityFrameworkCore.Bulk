using Woodman.EntityFrameworkCore.Bulk;
using Microsoft.EntityFrameworkCore;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class TestEntity
    {
        public int Id { get; set; }
    }

    public class MockableContext : DbContextBase
    {
        public virtual DbSet<TestEntity> TestEntity { get; set; }

        public MockableContext() : base() { }

        public MockableContext(DbContextOptions options) : base(options) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestEntity>(entity =>
            {
                entity.HasKey(e => e.Id);

                entity.Property(e => e.Id).HasColumnName("ID");
            });
        }
    }
}
