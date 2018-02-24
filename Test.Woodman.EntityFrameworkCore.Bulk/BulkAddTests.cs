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
            using (var db = new woodmanContext())
            {
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

        [Fact]
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
                var ids = await db.EfCoreTest.BulkAddAsync(toCreate);

                Assert.Equal(toCreate.Count, ids.Length);

                var index = 0;
                foreach (var id in ids)
                {
                    var added = await db.EfCoreTest.FindAsync((int)id);

                    Assert.Equal(toCreate[index].Name, added.Name);

                    index++;
                }
            }
        }

        [Fact]
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
                var ids = await db.Efcoretest.BulkAddAsync(toCreate);

                Assert.Equal(toCreate.Count, ids.Length);

                var index = 0;
                foreach (var id in ids)
                {
                    var added = await db.Efcoretest.FindAsync((int)id);

                    Assert.Equal(toCreate[index].Name, added.Name);

                    index++;
                }
            }
        }

        [Fact]
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
                var ids = await db.EfCoreTest.BulkAddAsync(toCreate);

                Assert.Equal(toCreate.Count, ids.Length);

                var index = 0;
                foreach (var id in ids)
                {
                    var added = await db.EfCoreTest.FindAsync((int)id);

                    Assert.Equal(toCreate[index].Name, added.Name);

                    index++;
                }
            }
        }
    }
}
