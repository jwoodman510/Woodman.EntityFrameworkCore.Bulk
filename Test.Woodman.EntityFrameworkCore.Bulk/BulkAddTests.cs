using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Xunit;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkAddTests
    {
        private readonly DbContextOptions InMemDbOpts = new DbContextOptionsBuilder<woodmanContext>().UseInMemoryDatabase(nameof(BulkAddTests)).Options;

        public BulkAddTests()
        {
            Init();
        }

        [Fact(DisplayName = "Sql Primary Key")]
        public async Task CreatesSql()
        {
            var toCreate = new List<EfCoreTest>();

            for (var i = 0; i < 10; i++)
            {
                toCreate.Add(new EfCoreTest
                {
                    Name = i == 0 ? null : $"{nameof(BulkAddTests)}_{i}",
                    CreatedDate = DateTime.UtcNow,
                    ModifiedDate = DateTime.UtcNow
                });
            }

            using (var db = new woodmanContext())
            {
                await db.EfCoreTest.BulkAddAsync(toCreate);
                
                foreach (var e in toCreate)
                {
                    var added = await db.EfCoreTest.FindAsync(e.Id);

                    Assert.NotNull(added);
                    Assert.Equal(e.Name, added.Name);
                }
            }
        }

        [Fact(DisplayName = "NpgSql Primary Key")]
        public async Task CreatesNpgSql()
        {
            var toCreate = new List<Efcoretest>();

            for (var i = 0; i < 10; i++)
            {
                toCreate.Add(new Efcoretest
                {
                    Name = i == 0 ? null : $"{nameof(BulkAddTests)}_{i}",
                    Createddate = DateTime.UtcNow,
                    Modifieddate = DateTime.UtcNow
                });
            }

            using (var db = new postgresContext())
            {
                await db.Efcoretest.BulkAddAsync(toCreate);
                
                foreach (var e in toCreate)
                {
                    var added = await db.Efcoretest.FindAsync(e.Id);

                    Assert.NotNull(added);
                    Assert.Equal(e.Name, added.Name);
                }
            }
        }

        [Fact(DisplayName = "InMem Primary Key")]
        public async Task CreatesInMem()
        {
            var toCreate = new List<EfCoreTest>();

            for (var i = 0; i < 10; i++)
            {
                toCreate.Add(new EfCoreTest
                {
                    Id = i + 1,
                    Name = i == 0 ? null : $"{nameof(BulkAddTests)}_{i}",
                    CreatedDate = DateTime.UtcNow,
                    ModifiedDate = DateTime.UtcNow
                });
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                await db.EfCoreTest.BulkAddAsync(toCreate);
                
                foreach (var e in toCreate)
                {
                    var added = await db.EfCoreTest.FindAsync(e.Id);

                    Assert.NotNull(added);
                    Assert.Equal(e.Name, added.Name);
                }
            }
        }

        [Fact(DisplayName = "Sql Composite Key")]
        public async Task CreatesSqlComposite()
        {
            var parent = new EfCoreTest
            {
                Name = $"{nameof(BulkAddTests)}_Composite",
                CreatedDate = DateTime.UtcNow,
                ModifiedDate = DateTime.UtcNow
            };

            using(var db = new woodmanContext())
            {
                await db.AddAsync(parent);
                await db.SaveChangesAsync();
            }

            var toCreate = new List<EfCoreTestChild>();

            for (var i = 0; i < 10; i++)
            {
                toCreate.Add(new EfCoreTestChild
                {
                    EfCoreTestId = parent.Id,
                    Name = i == 0 ? null : $"{nameof(BulkAddTests)}_{i}",
                    CreatedDate = DateTime.UtcNow,
                    ModifiedDate = DateTime.UtcNow
                });
            }

            using (var db = new woodmanContext())
            {
                await db.EfCoreTestChild.BulkAddAsync(toCreate);

                foreach (var e in toCreate)
                {
                    var added = await db.EfCoreTestChild.FindAsync(e.Id, e.EfCoreTestId);

                    Assert.NotNull(added);
                    Assert.Equal(e.Name, added.Name);
                }
            }
        }

        [Fact(DisplayName = "NpgSql Composite Key")]
        public async Task CreatesNpgSqlComposite()
        {
            var parent = new Efcoretest
            {
                Name = $"{nameof(BulkAddTests)}_Composite",
                Createddate = DateTime.UtcNow,
                Modifieddate = DateTime.UtcNow
            };

            using (var db = new postgresContext())
            {
                await db.AddAsync(parent);
                await db.SaveChangesAsync();
            }

            var toCreate = new List<Efcoretestchild>();

            for (var i = 0; i < 10; i++)
            {
                toCreate.Add(new Efcoretestchild
                {
                    Efcoretestid = parent.Id,
                    Name = i == 0 ? null : $"{nameof(BulkAddTests)}_{i}",
                    Createddate = DateTime.UtcNow,
                    Modifieddate = DateTime.UtcNow
                });
            }

            using (var db = new postgresContext())
            {
                await db.Efcoretestchild.BulkAddAsync(toCreate);

                foreach (var e in toCreate)
                {
                    var added = await db.Efcoretestchild.FindAsync(e.Id, e.Efcoretestid);

                    Assert.NotNull(added);
                    Assert.Equal(e.Name, added.Name);
                }
            }
        }

        [Fact(DisplayName = "InMem Composite Key")]
        public async Task CreatesInMemComposite()
        {
            var parent = new EfCoreTest
            {
                Id = 99999,
                Name = $"{nameof(BulkAddTests)}_Composite",
                CreatedDate = DateTime.UtcNow,
                ModifiedDate = DateTime.UtcNow
            };

            using (var db = new woodmanContext(InMemDbOpts))
            {
                await db.AddAsync(parent);
                await db.SaveChangesAsync();
            }

            var toCreate = new List<EfCoreTestChild>();

            for (var i = 0; i < 10; i++)
            {
                toCreate.Add(new EfCoreTestChild
                {
                    Id = i + 1,
                    EfCoreTestId = parent.Id,
                    Name = i == 0 ? null : $"{nameof(BulkAddTests)}_{i}",
                    CreatedDate = DateTime.UtcNow,
                    ModifiedDate = DateTime.UtcNow
                });
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                await db.EfCoreTestChild.BulkAddAsync(toCreate);

                foreach (var e in toCreate)
                {
                    var added = await db.EfCoreTestChild.FindAsync(e.Id, e.EfCoreTestId);

                    Assert.NotNull(added);
                    Assert.Equal(e.Name, added.Name);
                }
            }
        }

        private static void Init()
        {
            using (var db = new woodmanContext())
            {
                var childEntities = db.EfCoreTestChild
                    .Where(e => e.Name == null || e.Name.Contains(nameof(BulkAddTests)))
                    .ToList();

                if (childEntities.Count > 0)
                {
                    db.RemoveRange(childEntities);
                    db.SaveChanges();
                }

                var entities = db.EfCoreTest
                    .Where(e => e.Name == null || e.Name.Contains(nameof(BulkAddTests)))
                    .ToList();

                if (entities.Count > 0)
                {
                    db.RemoveRange(entities);
                    db.SaveChanges();
                }
            }

            using (var db = new postgresContext())
            {
                var childEntities = db.Efcoretestchild
                    .Where(e => e.Name == null || e.Name.Contains(nameof(BulkAddTests)))
                    .ToList();

                if (childEntities.Count > 0)
                {
                    db.RemoveRange(childEntities);
                    db.SaveChanges();
                }

                var entities = db.Efcoretest
                    .Where(e => e.Name == null || e.Name.Contains(nameof(BulkAddTests)))
                    .ToList();

                if (entities.Count > 0)
                {
                    db.RemoveRange(entities);
                    db.SaveChanges();
                }
            }
        }
    }
}
