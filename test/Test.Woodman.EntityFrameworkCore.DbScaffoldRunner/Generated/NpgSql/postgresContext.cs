using Microsoft.EntityFrameworkCore;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql
{
    public partial class postgresContext : DbContext
    {
        public virtual DbSet<Efcoretest> Efcoretest { get; set; }
        public virtual DbSet<Efcoretestchild> Efcoretestchild { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseNpgsql(@"Host=localhost;Database=postgres;Username={un};Password={pw}");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasPostgresExtension("adminpack");

            modelBuilder.Entity<Efcoretest>(entity =>
            {
                entity.ToTable("efcoretest");

                entity.Property(e => e.Id)
                    .HasColumnName("id")
                    .HasDefaultValueSql("nextval('id_seq'::regclass)");

                entity.Property(e => e.Createddate)
                    .HasColumnName("createddate")
                    .HasColumnType("date");

                entity.Property(e => e.Modifieddate)
                    .HasColumnName("modifieddate")
                    .HasColumnType("date");

                entity.Property(e => e.Name).HasColumnName("name");
            });

            modelBuilder.Entity<Efcoretestchild>(entity =>
            {
                entity.HasKey(e => new { e.Id, e.Efcoretestid });

                entity.ToTable("efcoretestchild");

                entity.Property(e => e.Id)
                    .HasColumnName("id")
                    .HasDefaultValueSql("nextval('id_seq_child'::regclass)");

                entity.Property(e => e.Efcoretestid).HasColumnName("efcoretestid");

                entity.Property(e => e.Createddate)
                    .HasColumnName("createddate")
                    .HasColumnType("date");

                entity.Property(e => e.Modifieddate)
                    .HasColumnName("modifieddate")
                    .HasColumnType("date");

                entity.Property(e => e.Name).HasColumnName("name");

                entity.HasOne(d => d.Efcoretest)
                    .WithMany(p => p.Efcoretestchild)
                    .HasForeignKey(d => d.Efcoretestid)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("efcoretestchild_efcoretestid_fkey");
            });

            modelBuilder.HasSequence("id_seq");

            modelBuilder.HasSequence("id_seq_child");
        }
    }
}
