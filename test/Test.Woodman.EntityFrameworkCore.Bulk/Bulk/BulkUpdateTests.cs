using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Microsoft.EntityFrameworkCore;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Woodman.EntityFrameworkCore.Bulk.Bulk;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkUpdateTests : BulkTestBase
    {
        public override string Name => nameof(BulkUpdateTests);

        public override int InMemId => 11;

        [Fact(DisplayName = "Sql Primary Key")]
        public async Task UpdatesSql()
        {
            var toUpdate = SqlIds.Take(2)
                .ToDictionary(key => key, value => DateTime.UtcNow.AddDays(value));

            using (var db = new woodmanContext())
            {
                var result = await db.EfCoreTest
                    .Where(x => x.Name != null)
                    .BulkUpdateAsync(toUpdate.Select(x => x.Key), id => new EfCoreTest
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

        [Fact(DisplayName = "NpgSql Primary Key")]
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

        [Fact(DisplayName = "InMem Primary Key")]
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

        [Fact(DisplayName = "Sql")]
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

        [Fact(DisplayName = "NpgSql")]
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

        [Fact(DisplayName = "InMem")]
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

        [Fact(DisplayName = "Sql Composite Key")]
        public async Task UpdatesSqlComposite()
        {
            var toUpdate = SqlCompositeIds.Take(2)
                .ToDictionary(key => key, value => DateTime.UtcNow.AddDays(3));

            using (var db = new woodmanContext())
            {
                var result = await db.EfCoreTestChild.BulkUpdateAsync(toUpdate.Select(x => x.Key), id => new EfCoreTestChild
                {
                    ModifiedDate = toUpdate[id]
                });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new woodmanContext())
            {
                var updated = await db.EfCoreTestChild
                    .Join(toUpdate.Select(y => y.Key))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    var keyExists = toUpdate.Keys.Any(k => Enumerable.SequenceEqual(new object[] { u.Id, u.EfCoreTestId }, k));

                    Assert.True(keyExists);

                    var expected = toUpdate.First(x => Enumerable.SequenceEqual(new object[] { u.Id, u.EfCoreTestId }, x.Key)).Value;
                    var actual = u.ModifiedDate;

                    Assert.Equal(expected.ToString("G"), actual.ToString("G"));
                }
            }
        }

        [Fact(DisplayName = "Sql Composite Key - Non Value Type")]
        public async Task UpdatesSqlCompositeNonValueType()
        {
            using (var db = new woodmanContext())
            {
                var toUpdate = await db.EfCoreTestNonValueTypeKeys.Take(2).ToListAsync();
                var keys = toUpdate.Select(x => new object[] { x.Id, x.Tier1Id, x.Tier2Id });

                var result = await db.EfCoreTestNonValueTypeKeys.BulkUpdateAsync(keys, id => new EfCoreTestNonValueTypeKeys
                {
                    ModifiedDate = DateTime.UtcNow
                });

                Assert.Equal(toUpdate.Count, result);
            }
        }

        [Fact(DisplayName = "NpgSql Composite Key")]
        public async Task UpdatesNpgSqlComposite()
        {
            var toUpdate = NpgSqlCompositeIds.Take(2)
                .ToDictionary(key => key, value => DateTime.UtcNow.AddDays(3));

            using (var db = new postgresContext())
            {
                var result = await db.Efcoretestchild.BulkUpdateAsync(toUpdate.Select(x => x.Key), id => new Efcoretestchild
                {
                    Modifieddate = toUpdate[id]
                });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new postgresContext())
            {
                var updated = await db.Efcoretestchild
                    .Join(toUpdate.Select(y => y.Key))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    var keyExists = toUpdate.Keys.Any(k => Enumerable.SequenceEqual(new object[] { u.Id, u.Efcoretestid }, k));

                    Assert.True(keyExists);

                    var expected = toUpdate.First(x => Enumerable.SequenceEqual(new object[] { u.Id, u.Efcoretestid }, x.Key)).Value;
                    var actual = u.Modifieddate;

                    Assert.Equal(expected.ToString("d"), actual.ToString("d"));
                }
            }
        }

        [Fact(DisplayName = "InMem Composite Key")]
        public async Task UpdatesInMemComposite()
        {
            var toUpdate = InMemCompositeIds.Take(2)
                .ToDictionary(key => key, value => DateTime.UtcNow.AddDays(3));

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var result = await db.EfCoreTestChild.BulkUpdateAsync(toUpdate.Select(x => x.Key), id => new EfCoreTestChild
                {
                    ModifiedDate = toUpdate.First(u => Enumerable.SequenceEqual(id, u.Key)).Value
                });

                Assert.Equal(toUpdate.Count, result);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var updated = await db.EfCoreTestChild
                    .Join(toUpdate.Select(y => y.Key))
                    .ToListAsync();

                Assert.Equal(toUpdate.Count, updated.Count);

                foreach (var u in updated)
                {
                    var keyExists = toUpdate.Keys.Any(k => Enumerable.SequenceEqual(new object[] { u.Id, u.EfCoreTestId }, k));

                    Assert.True(keyExists);

                    var expected = toUpdate.First(x => Enumerable.SequenceEqual(new object[] { u.Id, u.EfCoreTestId }, x.Key)).Value;
                    var actual = u.ModifiedDate;

                    Assert.Equal(expected.ToString("G"), actual.ToString("G"));
                }
            }
        }
    }
}
