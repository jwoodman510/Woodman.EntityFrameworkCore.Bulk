using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Xunit;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkJoinTests : BulkTestBase
    {
        public override string Name => nameof(BulkJoinTests);

        public override int InMemId => 1;

        [Fact]
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

        [Fact]
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

        [Fact]
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
    }
}
