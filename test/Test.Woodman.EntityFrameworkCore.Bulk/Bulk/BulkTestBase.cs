using System;
using System.Collections.Generic;
using System.Linq;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Microsoft.EntityFrameworkCore;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public abstract class BulkTestBase
    {
        public abstract string Name { get; }

        public abstract int InMemId { get; }

        protected readonly List<int> SqlIds = new List<int>();
        protected readonly List<int> NpgSqlIds = new List<int>();
        protected readonly List<int> InMemIds = new List<int>();

        protected readonly List<object[]> SqlCompositeIds = new List<object[]>();
        protected readonly List<object[]> NpgSqlCompositeIds = new List<object[]>();
        protected readonly List<object[]> InMemCompositeIds = new List<object[]>();

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
                for (var i = 1; i <= 10; i++)
                {
                    var name = $"{Name}_{i}";
                    int efCoreTestId;
                    InitiEfCoreTestNonValueTypeKeys(db, i);
                    var existing = db.EfCoreTest
                        .Where(e => e.Name == name)
                        .FirstOrDefault();

                    if (existing == null)
                    {
                        var inserted = db.Add(new EfCoreTest
                        {
                            Name = name,
                            CreatedDate = DateTime.Now,
                            ModifiedDate = DateTime.Now
                        });

                        db.SaveChanges();

                        efCoreTestId = inserted.Entity.Id;
                        SqlIds.Add(inserted.Entity.Id);
                    }
                    else
                    {
                        efCoreTestId = existing.Id;
                        SqlIds.Add(existing.Id);
                    }

                    if (i > 5)
                        continue;

                    for (var j = 1; j <= 4; j++)
                    {
                        var childName = $"{Name}_{i}_{j}";

                        var existingChild = db.EfCoreTestChild
                            .Where(e => e.Name == childName)
                            .FirstOrDefault();

                        if (existingChild == null)
                        {
                            var inserted = db.Add(new EfCoreTestChild
                            {
                                EfCoreTestId = efCoreTestId,
                                Name = name,
                                CreatedDate = DateTime.Now,
                                ModifiedDate = DateTime.Now
                            });

                            db.SaveChanges();

                            SqlCompositeIds.Add(new object[] { inserted.Entity.Id, efCoreTestId });
                        }
                        else
                        {
                            SqlCompositeIds.Add(new object[] { existing.Id, efCoreTestId });
                        }
                    }
                }
            }
        }

        protected virtual void InitiEfCoreTestNonValueTypeKeys(woodmanContext db, int iteration)
        {
            var name = $"{Name}_{iteration}";
            var existing = db.EfCoreTestNonValueTypeKeys
                .Where(e => e.Name == name)
                .FirstOrDefault();

            if (existing == null)
            {
                var inserted = db.Add(new EfCoreTestNonValueTypeKeys
                {
                    Tier1Id = Guid.NewGuid().ToString(),
                    Tier2Id = Guid.NewGuid().ToString(),
                    Name = name,
                    CreatedDate = DateTime.Now,
                    ModifiedDate = DateTime.Now,
                    IsArchived = false
                });
                db.SaveChanges();
            }
        }

        protected virtual void InitNpgSqlDb()
        {
            using (var db = new postgresContext())
            {
                for (var i = 1; i <= 10; i++)
                {
                    var name = $"{Name}_{i}";

                    var existing = db.Efcoretest
                        .Where(e => e.Name == name)
                        .FirstOrDefault();

                    var efCoreTestId = 0;

                    if (existing == null)
                    {
                        var inserted = db.Efcoretest.Add(new Efcoretest
                        {
                            Name = name,
                            Createddate = DateTime.Now,
                            Modifieddate = DateTime.Now
                        });

                        db.SaveChanges();

                        efCoreTestId = inserted.Entity.Id;
                        NpgSqlIds.Add(inserted.Entity.Id);
                    }
                    else
                    {
                        efCoreTestId = existing.Id;
                        NpgSqlIds.Add(existing.Id);
                    }

                    if (i > 5)
                        continue;

                    for (var j = 1; j <= 4; j++)
                    {
                        var childName = $"{Name}_{i}_{j}";

                        var existingChild = db.Efcoretestchild
                            .Where(e => e.Name == childName)
                            .FirstOrDefault();

                        if (existingChild == null)
                        {
                            var inserted = db.Add(new Efcoretestchild
                            {
                                Efcoretestid = efCoreTestId,
                                Name = name,
                                Createddate = DateTime.Now,
                                Modifieddate = DateTime.Now
                            });

                            db.SaveChanges();

                            NpgSqlCompositeIds.Add(new object[] { inserted.Entity.Id, efCoreTestId });
                        }
                        else
                        {
                            NpgSqlCompositeIds.Add(new object[] { existing.Id, efCoreTestId });
                        }
                    }
                }
            }
        }

        protected virtual void InitInMemDb()
        {
            using (var db = new woodmanContext(InMemDbOpts))
            {
                for (var i = 1; i <= 10; i++)
                {
                    var name = $"{Name}_{i}";

                    var existing = db.EfCoreTest
                        .Where(e => e.Name == name)
                        .FirstOrDefault();

                    var efCoreTestId = 0;

                    if (existing == null)
                    {
                        var inserted = db.Add(new EfCoreTest
                        {
                            Id = InMemId + i - 1,
                            Name = name,
                            CreatedDate = DateTime.Now,
                            ModifiedDate = DateTime.Now
                        });

                        db.SaveChanges();

                        efCoreTestId = inserted.Entity.Id;
                        InMemIds.Add(inserted.Entity.Id);
                    }
                    else
                    {
                        efCoreTestId = existing.Id;
                        InMemIds.Add(existing.Id);
                    }

                    if (i > 5)
                        continue;

                    for (var j = 1; j <= 4; j++)
                    {
                        var childName = $"{Name}_{i}_{j}";

                        var existingChild = db.EfCoreTestChild
                            .Where(e => e.Name == childName)
                            .FirstOrDefault();

                        if (existingChild == null)
                        {
                            var inserted = db.Add(new EfCoreTestChild
                            {
                                EfCoreTestId = efCoreTestId,
                                Name = name,
                                CreatedDate = DateTime.Now,
                                ModifiedDate = DateTime.Now
                            });

                            db.SaveChanges();

                            InMemCompositeIds.Add(new object[] { inserted.Entity.Id, efCoreTestId });
                        }
                        else
                        {
                            InMemCompositeIds.Add(new object[] { existing.Id, efCoreTestId });
                        }
                    }
                }
            }
        }
    }
}