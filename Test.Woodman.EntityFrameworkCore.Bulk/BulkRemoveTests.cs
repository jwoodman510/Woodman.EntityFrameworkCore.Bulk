using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Microsoft.EntityFrameworkCore;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkRemoveTests : BulkTestBase
    {
        public override string Name => nameof(BulkRemoveTests);

        public override int InMemId => 21;

        [Fact]
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

        [Fact]
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

        [Fact]
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

        [Fact]
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

        [Fact]
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

        [Fact]
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
    }
}
