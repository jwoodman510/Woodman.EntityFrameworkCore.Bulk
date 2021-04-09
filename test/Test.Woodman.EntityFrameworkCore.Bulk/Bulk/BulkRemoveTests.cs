using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Microsoft.EntityFrameworkCore;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Woodman.EntityFrameworkCore.Bulk.Bulk;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkRemoveTests : BulkTestBase
    {
        public override string Name => nameof(BulkRemoveTests);

        public override int InMemId => 21;

        [Fact(DisplayName = "Sql Primary Key")]
        public async Task RemovesSql()
        {
            var toDelete = SqlIds.Skip(5).Take(5).ToList();

            using (var db = new woodmanContext())
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new woodmanContext())
            {
                var result = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkRemoveTests)))
                    .BulkRemoveAsync(toDelete);

                Assert.Equal(5, result);
            }

            using (var db = new woodmanContext())
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "NpgSql Primary Key")]
        public async Task RemovesNpgSql()
        {
            var toDelete = NpgSqlIds.Skip(5).Take(5).ToList();

            using (var db = new postgresContext())
            {
                var entities = await db.Efcoretest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new postgresContext())
            {
                var result = await db.Efcoretest
                    .Where(e => e.Name.Contains(nameof(BulkRemoveTests)))
                    .BulkRemoveAsync(toDelete);

                Assert.Equal(5, result);
            }

            using (var db = new postgresContext())
            {
                var entities = await db.Efcoretest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "InMem Primary Key")]
        public async Task RemovesInMem()
        {
            var toDelete = InMemIds.Take(5).ToList();

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var result = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkRemoveTests)))
                    .BulkRemoveAsync(toDelete);

                Assert.Equal(5, result);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "Sql")]
        public async Task RemovesWithoutKeysSql()
        {
            var toDelete = SqlIds.Skip(5).Take(5).ToList();

            using (var db = new woodmanContext())
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new woodmanContext())
            {
                var result = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .BulkRemoveAsync();

                Assert.Equal(5, result);
            }

            using (var db = new woodmanContext())
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "NpgSql")]
        public async Task RemovesWithoutKeysNpgSql()
        {
            var toDelete = NpgSqlIds.Skip(5).Take(5).ToList();

            using (var db = new postgresContext())
            {
                var entities = await db.Efcoretest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new postgresContext())
            {
                var result = await db.Efcoretest
                    .Where(e => toDelete.Contains(e.Id))
                    .BulkRemoveAsync();

                Assert.Equal(5, result);
            }

            using (var db = new postgresContext())
            {
                var entities = await db.Efcoretest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "InMem")]
        public async Task RemovesWithoutKeysInMem()
        {
            var toDelete = InMemIds.Skip(5).Take(5).ToList();

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var result = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .BulkRemoveAsync();

                Assert.Equal(5, result);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var entities = await db.EfCoreTest
                    .Where(e => toDelete.Contains(e.Id))
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "Sql Composite Key")]
        public async Task RemovesSqlComposite()
        {
            var toDelete = SqlCompositeIds.Take(5).ToList();

            using (var db = new woodmanContext())
            {
                var entities = await db.EfCoreTestChild
                    .Join(toDelete)
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new woodmanContext())
            {
                var result = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkRemoveTests)))
                    .BulkRemoveAsync(toDelete);

                Assert.Equal(5, result);
            }

            using (var db = new woodmanContext())
            {
                var entities = await db.EfCoreTestChild
                    .Join(toDelete)
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "NpgSql Composite Key")]
        public async Task RemovesNpgSqlComposite()
        {
            var toDelete = NpgSqlCompositeIds.Take(5).ToList();

            using (var db = new postgresContext())
            {
                var entities = await db.Efcoretestchild
                    .Join(toDelete)
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new postgresContext())
            {
                var result = await db.Efcoretestchild
                    .Where(e => e.Name.Contains(nameof(BulkRemoveTests)))
                    .BulkRemoveAsync(toDelete);

                Assert.Equal(5, result);
            }

            using (var db = new postgresContext())
            {
                var entities = await db.Efcoretestchild
                    .Join(toDelete)
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }

        [Fact(DisplayName = "InMem Composite Key")]
        public async Task RemovesInMemComposite()
        {
            var toDelete = InMemCompositeIds.Take(5).ToList();

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var entities = await db.EfCoreTestChild
                    .Join(toDelete)
                    .ToListAsync();

                Assert.NotEmpty(entities);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var result = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkRemoveTests)))
                    .BulkRemoveAsync(toDelete);

                Assert.Equal(5, result);
            }

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var entities = await db.EfCoreTestChild
                    .Join(toDelete)
                    .ToListAsync();

                Assert.Empty(entities);
            }
        }
    }
}
