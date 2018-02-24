using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql
{
    public partial class postgresContext : DbContext
    {
        public virtual DbSet<Efcoretest> Efcoretest { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseNpgsql(@"Host=localhost;Database=postgres;Username=test;Password=Tatert0t");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
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
        }
    }
}
