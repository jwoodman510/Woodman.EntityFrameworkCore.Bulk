using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public abstract class BulkTestBase
    {
        public abstract string Name { get; }

        public abstract int InMemId { get; }

        protected readonly List<int> SqlIds = new List<int>();
        protected readonly List<int> NpgSqlIds = new List<int>();
        protected readonly List<int> InMemIds = new List<int>();

        protected readonly DbContextOptions InMemDbOpts;

        public BulkTestBase()
        {
            InMemDbOpts = new DbContextOptionsBuilder<woodmanContext>().UseInMemoryDatabase(Name).Options;

            InitSqlDb();
            InitNpgSqlDb();
            InitInMemDb();
        }

        protected virtual void InitSqlDb()
        {
            using (var db = new woodmanContext())
            {
                var save = false;

                for (var i = 1; i <= 10; i++)
                {
                    var name = $"{Name}_{i}";

                    var existing = db.EfCoreTest
                        .Where(e => e.Name == name)
                        .FirstOrDefault();

                    if (existing == null)
                    {
                        save = true;

                        var inserted = db.Add(new EfCoreTest
                        {
                            Name = name,
                            CreatedDate = DateTime.Now,
                            ModifiedDate = DateTime.Now
                        });

                        SqlIds.Add(inserted.Entity.Id);
                    }
                    else
                    {
                        SqlIds.Add(existing.Id);
                    }
                }

                if (save)
                {
                    db.SaveChanges();
                }
            }
        }

        protected virtual void InitNpgSqlDb()
        {
            using (var db = new postgresContext())
            {
                var save = false;

                for (var i = 1; i <= 10; i++)
                {
                    var name = $"{Name}_{i}";

                    var existing = db.Efcoretest
                        .Where(e => e.Name == name)
                        .FirstOrDefault();

                    if (existing == null)
                    {
                        save = true;

                        var inserted = db.Efcoretest.Add(new Efcoretest
                        {
                            Name = name,
                            Createddate = DateTime.Now,
                            Modifieddate = DateTime.Now
                        });

                        NpgSqlIds.Add(inserted.Entity.Id);
                    }
                    else
                    {
                        NpgSqlIds.Add(existing.Id);
                    }
                }

                if (save)
                {
                    db.SaveChanges();
                }
            }
        }

        protected virtual void InitInMemDb()
        {
            using (var db = new woodmanContext(InMemDbOpts))
            {
                var save = false;

                for (var i = 1; i <= 10; i++)
                {
                    var name = $"{Name}_{i}";

                    var existing = db.EfCoreTest
                        .Where(e => e.Name == name)
                        .FirstOrDefault();

                    if (existing == null)
                    {
                        save = true;

                        var inserted = db.Add(new EfCoreTest
                        {
                            Id = InMemId + i - 1,
                            Name = name,
                            CreatedDate = DateTime.Now,
                            ModifiedDate = DateTime.Now
                        });

                        InMemIds.Add(inserted.Entity.Id);
                    }
                    else
                    {
                        InMemIds.Add(existing.Id);
                    }

                    if (save)
                    {
                        db.SaveChanges();
                    }
                }
            }
        }
    }
}
