using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Woodman.EntityFrameworkCore.Bulk.Bulk;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkJoinTests : BulkTestBase
    {
        public override string Name => nameof(BulkJoinTests);

        public override int InMemId => 1;

        [Fact(DisplayName = "Sql Primary Key")]
        public async Task JoinsByPrimaryKeySql()
        {
            var expectedIds = SqlIds.Take(5).ToList();

            List<int> actualIds;

            using (var db = new woodmanContext())
            {
                actualIds = await db.EfCoreTest
                    .Join(expectedIds)
                    .Select(e => e.Id)
                    .ToListAsync();
            }

            Assert.Equal(expectedIds.Count, actualIds.Count);

            foreach (var expected in expectedIds)
            {
                Assert.Contains(expected, actualIds);
            }
        }

        [Fact(DisplayName = "NpgSql Primary Key")]
        public async Task JoinsByPrimaryKeyNpgSql()
        {
            var expectedIds = NpgSqlIds.Take(5).ToList();

            List<int> actualIds;

            using (var db = new postgresContext())
            {
                actualIds = await db.Efcoretest
                    .Join(expectedIds)
                    .Select(e => e.Id)
                    .ToListAsync();
            }

            Assert.Equal(expectedIds.Count, actualIds.Count);

            foreach (var expected in expectedIds)
            {
                Assert.Contains(expected, actualIds);
            }
        }

        [Fact(DisplayName = "InMem Primary Key")]
        public async Task JoinsByPrimaryKeyInMem()
        {
            var expectedIds = InMemIds.Take(5).ToList();

            List<int> actualIds;

            using (var db = new woodmanContext(InMemDbOpts))
            {
                actualIds = await db.EfCoreTest
                    .Join(expectedIds)
                    .Select(e => e.Id)
                    .ToListAsync();
            }

            Assert.Equal(expectedIds.Count, actualIds.Count);

            foreach (var expected in expectedIds)
            {
                Assert.Contains(expected, actualIds);
            }
        }

        [Fact(DisplayName = "Sql Composite Key")]
        public async Task JoinsByCompositeKeySql()
        {
            var expectedIds = SqlCompositeIds.Take(5).ToList();

            List<object[]> actualIds;

            using (var db = new woodmanContext())
            {
                actualIds = await db.EfCoreTestChild
                    .Join(expectedIds)
                    .Select(e => new object[] { e.Id, e.EfCoreTestId })
                    .ToListAsync();
            }

            Assert.Equal(expectedIds.Count, actualIds.Count);

            foreach (var expected in expectedIds)
            {
                Assert.Contains(expected, actualIds);
            }
        }

        [Fact(DisplayName = "NpgSql Composite Key")]
        public async Task JoinsByCompositeNpgSql()
        {
            var expectedIds = NpgSqlCompositeIds.Take(5).ToList();

            List<object[]> actualIds;

            using (var db = new postgresContext())
            {
                actualIds = await db.Efcoretestchild
                    .Join(expectedIds)
                    .Select(e => new object[] { e.Id, e.Efcoretestid })
                    .ToListAsync();
            }

            Assert.Equal(expectedIds.Count, actualIds.Count);

            foreach (var expected in expectedIds)
            {
                Assert.Contains(expected, actualIds);
            }
        }

        [Fact(DisplayName = "InMem Composite Key")]
        public async Task JoinsByCompositeInMem()
        {
            var expectedIds = InMemCompositeIds.Take(5).ToList();

            List<object[]> actualIds;

            using (var db = new woodmanContext(InMemDbOpts))
            {
                actualIds = await db.EfCoreTestChild
                    .Join(expectedIds)
                    .Select(e => new object[] { e.Id, e.EfCoreTestId })
                    .ToListAsync();
            }

            Assert.Equal(expectedIds.Count, actualIds.Count);

            foreach (var expected in expectedIds)
            {
                Assert.Contains(expected, actualIds);
            }
        }
    }
}
