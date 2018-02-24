using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Microsoft.EntityFrameworkCore;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkUpdateTests : BulkTestBase
    {
        public override string Name => nameof(BulkUpdateTests);

        public override int InMemId => 11;

        [Fact]
        public async Task UpdatesSql()
        {
            var toUpdate = SqlIds.Take(2)
                .ToDictionary(key => key, value => DateTime.UtcNow.AddDays(value));

            using (var db = new woodmanContext())
            {
                var result = await db.EfCoreTest.BulkUpdateAsync(toUpdate.Select(x => x.Key), id => new EfCoreTest
                {
                    ModifiedDate = toUpdate[id]
                });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new woodmanContext())
            {
                var updated = await db.EfCoreTest
                    .Where(x => toUpdate.Select(y => y.Key).Contains(x.Id))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    Assert.Contains(u.Id, toUpdate.Select(x => x.Key));

                    var expected = toUpdate[u.Id];
                    var actual = u.ModifiedDate;

                    Assert.Equal(expected.ToString("G"), actual.ToString("G"));
                }
            }
        }

        [Fact]
        public async Task UpdatesNpgSql()
        {
            var toUpdate = NpgSqlIds.Take(2)
                .ToDictionary(key => key, value => DateTime.UtcNow.AddDays(value + 1));

            using (var db = new postgresContext())
            {
                var result = await db.Efcoretest.BulkUpdateAsync(toUpdate.Select(x => x.Key), id => new Efcoretest
                {
                    Modifieddate = toUpdate[id]
                });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new postgresContext())
            {
                var updated = await db.Efcoretest
                    .Where(x => toUpdate.Select(y => y.Key).Contains(x.Id))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    Assert.Contains(u.Id, toUpdate.Select(x => x.Key));

                    var expected = toUpdate[u.Id];
                    var actual = u.Modifieddate;

                    Assert.Equal(expected.ToString("d"), actual.ToString("d"));
                }
            }
        }

        [Fact]
        public async Task UpdatesInMem()
        {
            var toUpdate = InMemIds.Take(2)
                .ToDictionary(key => key, value => DateTime.UtcNow.AddDays(value));

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var result = await db.EfCoreTest.BulkUpdateAsync(toUpdate.Select(x => x.Key), id => new EfCoreTest
                {
                    ModifiedDate = toUpdate[id]
                });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var updated = await db.EfCoreTest
                    .Where(x => toUpdate.Select(y => y.Key).Contains(x.Id))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    Assert.Contains(u.Id, toUpdate.Select(x => x.Key));

                    var expected = toUpdate[u.Id];
                    var actual = u.ModifiedDate;

                    Assert.Equal(expected.ToString("G"), actual.ToString("G"));
                }
            }
        }

        [Fact]
        public async Task UpdatesWithoutKeysSql()
        {
            var createdDate = DateTime.UtcNow;

            var toUpdate = SqlIds.Take(2).ToList();

            using (var db = new woodmanContext())
            {
                var result = await db.EfCoreTest
                    .Where(x => toUpdate.Contains(x.Id))
                    .BulkUpdateAsync(() => new EfCoreTest
                    {
                        CreatedDate = createdDate
                    });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new woodmanContext())
            {
                var updated = await db.EfCoreTest
                    .Where(x => toUpdate.Contains(x.Id))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    Assert.Contains(u.Id, toUpdate);

                    var expected = createdDate;
                    var actual = u.CreatedDate;

                    Assert.Equal(expected.ToString("G"), actual.ToString("G"));
                }
            }
        }

        [Fact]
        public async Task UpdatesWithoutKeysNpgSql()
        {
            var createdDate = DateTime.Parse(DateTime.UtcNow.AddDays(1).Date.ToString("d"));

            var toUpdate = NpgSqlIds.Skip(2).Take(2).ToList();

            using (var db = new postgresContext())
            {
                var result = await db.Efcoretest
                    .Where(x => toUpdate.Contains(x.Id))
                    .BulkUpdateAsync(() => new Efcoretest
                    {
                        Createddate = createdDate
                    });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new postgresContext())
            {
                var updated = await db.Efcoretest
                    .Where(x => toUpdate.Contains(x.Id))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    Assert.Contains(u.Id, toUpdate);

                    var expected = createdDate;
                    var actual = u.Createddate.ToUniversalTime();

                    Assert.Equal(expected.ToString("d"), actual.ToString("d"));
                }
            }
        }

        [Fact]
        public async Task UpdatesWithoutKeysInMem()
        {
            var createdDate = DateTime.UtcNow;

            var toUpdate = InMemIds.Take(2).ToList();

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var result = await db.EfCoreTest
                    .Where(x => toUpdate.Contains(x.Id))
                    .BulkUpdateAsync(() => new EfCoreTest
                    {
                        CreatedDate = createdDate
                    });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var updated = await db.EfCoreTest
                    .Where(x => toUpdate.Contains(x.Id))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    Assert.Contains(u.Id, toUpdate);

                    var expected = createdDate;
                    var actual = u.CreatedDate;

                    Assert.Equal(expected.ToString("G"), actual.ToString("G"));
                }
            }
        }
    }
}
