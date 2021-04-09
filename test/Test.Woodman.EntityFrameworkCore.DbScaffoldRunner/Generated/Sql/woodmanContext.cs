using Microsoft.EntityFrameworkCore;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql
{
    public partial class woodmanContext : DbContext
    {
        public virtual DbSet<EfCoreTest> EfCoreTest { get; set; }
        public virtual DbSet<EfCoreTestChild> EfCoreTestChild { get; set; }

        public virtual DbSet<EfCoreTestNonValueTypeKeys> EfCoreTestNonValueTypeKeys { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseSqlServer(@"Server=localhost\SQLEXPRESS;User Id={un}; Password={pw};Database=woodman;");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EfCoreTest>(entity =>
            {
                entity.Property(e => e.Id).HasColumnName("ID");

                entity.Property(e => e.CreatedDate).HasColumnType("datetime");

                entity.Property(e => e.ModifiedDate).HasColumnType("datetime");

                entity.Property(e => e.Name)
                    .HasMaxLength(50)
                    .IsUnicode(false);
            });

            modelBuilder.Entity<EfCoreTestNonValueTypeKeys>(entity =>
            {
                entity.Property(e => e.Id)
                  .HasColumnName("Id")
                  .ValueGeneratedOnAdd();
                entity.HasKey(e => new { e.Id, e.Tier1Id, e.Tier2Id });

                entity.Property(e => e.CreatedDate).HasColumnType("datetime");

                entity.Property(e => e.ModifiedDate).HasColumnType("datetime");

                entity.Property(e => e.Name)
                    .HasMaxLength(50)
                    .IsUnicode(false);
            });

            modelBuilder.Entity<EfCoreTestChild>(entity =>
            {
                entity.HasKey(e => new { e.Id, e.EfCoreTestId });

                entity.Property(e => e.Id)
                    .HasColumnName("ID")
                    .ValueGeneratedOnAdd();

                entity.Property(e => e.CreatedDate).HasColumnType("datetime");

                entity.Property(e => e.ModifiedDate).HasColumnType("datetime");

                entity.Property(e => e.Name)
                    .HasMaxLength(50)
                    .IsUnicode(false);

                entity.HasOne(d => d.EfCoreTest)
                    .WithMany(p => p.EfCoreTestChild)
                    .HasForeignKey(d => d.EfCoreTestId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_EfCoreTestChild_EfCoreTest1");
            });
        }
    }
}