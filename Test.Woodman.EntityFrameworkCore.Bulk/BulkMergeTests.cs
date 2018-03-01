using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using System;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Test.Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;

namespace Test.Woodman.EntityFrameworkCore.Bulk
{
    public class BulkMergeTests : BulkTestBase
    {
        public override string Name => nameof(BulkMergeTests);

        public override int InMemId => 31;

        [Fact(DisplayName = "Sql Primary Key")]
        public async Task MergesSql()
        {
            List<EfCoreTest> entities;

            var dontDelete = new EfCoreTest
            {
                Name = "dont delete",
                CreatedDate = DateTime.UtcNow,
                ModifiedDate = DateTime.UtcNow
            };

            using (var db = new woodmanContext())
            {
                entities = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();

                var added = await db.AddAsync(dontDelete);

                await db.SaveChangesAsync();

                dontDelete.Id = added.Entity.Id;
            }

            const int numUpdate = 5;

            var toMerge = entities
                .Take(numUpdate)
                .Select(x =>
                {
                    x.ModifiedDate = DateTime.UtcNow.AddDays(x.Id);
                    return x;
                })
                .ToList();

            var numDelete = entities.Count - toMerge.Count;

            var numAdd = 2;

            for (var i = 0; i < numAdd; i++)
            {
                toMerge.Add(new EfCoreTest
                {
                    Name = $"{nameof(BulkMergeTests)}_insert_{i}",
                    ModifiedDate = DateTime.Now,
                    CreatedDate = DateTime.Now
                });
            }

            var expectedRecordsAffected = numUpdate + numDelete + numAdd;

            using (var db = new woodmanContext())
            {
                var numRowsAffected = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .BulkMergeAsync(toMerge);

                Assert.Equal(expectedRecordsAffected, numRowsAffected);

                var dbCount = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .CountAsync();

                Assert.Equal(toMerge.Count, dbCount);

                foreach (var m in toMerge)
                {
                    var dbEntity = await db.EfCoreTest.FindAsync(m.Id);

                    Assert.NotNull(dbEntity);
                    Assert.Equal(m.Name, dbEntity.Name);
                    Assert.Equal(m.ModifiedDate.ToString("G"), dbEntity.ModifiedDate.ToString("G"));
                    Assert.Equal(m.CreatedDate.ToString("G"), dbEntity.CreatedDate.ToString("G"));
                }

                var dontDeleteEntity = await db.EfCoreTest.FindAsync(dontDelete.Id);
                Assert.NotNull(dontDeleteEntity);
            }
        }

        [Fact(DisplayName = "NpgSql Primary Key")]
        public async Task MergesNpgSql()
        {
            List<Efcoretest> entities;

            var dontDelete = new Efcoretest
            {
                Name = "dont delete",
                Createddate = DateTime.UtcNow,
                Modifieddate = DateTime.UtcNow
            };

            using (var db = new postgresContext())
            {
                entities = await db.Efcoretest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();

                var added = await db.AddAsync(dontDelete);

                await db.SaveChangesAsync();

                dontDelete.Id = added.Entity.Id;
            }

            const int numUpdate = 5;

            var toMerge = entities
                .OrderBy(e => e.Id)
                .Take(numUpdate)
                .Select(x =>
                {
                    x.Modifieddate = DateTime.UtcNow.AddDays(x.Id);
                    return x;
                })
                .ToList();

            var numDelete = entities.Count - toMerge.Count;

            var numAdd = 2;

            for (var i = 0; i < numAdd; i++)
            {
                toMerge.Add(new Efcoretest
                {
                    Name = $"{nameof(BulkMergeTests)}_insert_{i}",
                    Modifieddate = DateTime.Now,
                    Createddate = DateTime.Now
                });
            }

            var expectedRecordsAffected = numUpdate + numDelete + numAdd;

            using (var db = new postgresContext())
            {
                var numRowsAffected = await db.Efcoretest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .BulkMergeAsync(toMerge);

                Assert.Equal(expectedRecordsAffected, numRowsAffected);

                var dbCount = await db.Efcoretest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .CountAsync();

                Assert.Equal(toMerge.Count, dbCount);

                foreach (var m in toMerge)
                {
                    var dbEntity = await db.Efcoretest.FindAsync(m.Id);

                    Assert.NotNull(dbEntity);
                    Assert.Equal(m.Name, dbEntity.Name);
                    Assert.Equal(m.Modifieddate.ToString("d"), dbEntity.Modifieddate.ToString("d"));
                    Assert.Equal(m.Createddate.ToString("d"), dbEntity.Createddate.ToString("d"));
                }

                var dontDeleteEntity = await db.Efcoretest.FindAsync(dontDelete.Id);
                Assert.NotNull(dontDeleteEntity);
            }
        }

        [Fact(DisplayName = "InMem Primary Key")]
        public async Task MergesInMem()
        {
            List<EfCoreTest> entities;

            using (var db = new woodmanContext(InMemDbOpts))
            {
                entities = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();
            }

            const int numUpdate = 5;

            var toMerge = entities
                .Take(numUpdate)
                .Select(x =>
                {
                    x.ModifiedDate = DateTime.UtcNow.AddDays(x.Id);
                    return x;
                })
                .ToList();

            var numDelete = entities.Count - toMerge.Count;

            var numAdd = 2;

            for (var i = 0; i < numAdd; i++)
            {
                toMerge.Add(new EfCoreTest
                {
                    Id = InMemId - i - 1,
                    Name = $"{nameof(BulkMergeTests)}_insert_{i}",
                    ModifiedDate = DateTime.Now,
                    CreatedDate = DateTime.Now
                });
            }

            var expectedRecordsAffected = numUpdate + numDelete + numAdd;

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var numRowsAffected = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .BulkMergeAsync(toMerge);

                Assert.Equal(expectedRecordsAffected, numRowsAffected);

                var dbCount = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .CountAsync();

                Assert.Equal(toMerge.Count, dbCount);

                foreach (var m in toMerge)
                {
                    var dbEntity = await db.EfCoreTest.FindAsync(m.Id);

                    Assert.NotNull(dbEntity);
                    Assert.Equal(m.Name, dbEntity.Name);
                    Assert.Equal(m.ModifiedDate.ToString("G"), dbEntity.ModifiedDate.ToString("G"));
                    Assert.Equal(m.CreatedDate.ToString("G"), dbEntity.CreatedDate.ToString("G"));
                }
            }
        }

        [Fact(DisplayName = "Sql Composite Key")]
        public async Task MergesSqlComposite()
        {
            List<EfCoreTestChild> entities;

            using (var db = new woodmanContext())
            {
                entities = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();
            }

            const int numUpdate = 5;

            var toMerge = entities
                .Take(numUpdate)
                .Select(x =>
                {
                    x.ModifiedDate = DateTime.UtcNow.AddDays(x.Id);
                    return x;
                })
                .ToList();

            var numDelete = entities.Count - toMerge.Count;

            var numAdd = 2;

            for (var i = 0; i < numAdd; i++)
            {
                toMerge.Add(new EfCoreTestChild
                {
                    EfCoreTestId = toMerge[0].EfCoreTestId,
                    Name = $"{nameof(BulkMergeTests)}_insert_{i}",
                    ModifiedDate = DateTime.Now,
                    CreatedDate = DateTime.Now
                });
            }

            var expectedRecordsAffected = numUpdate + numDelete + numAdd;

            using (var db = new woodmanContext())
            {
                var numRowsAffected = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .BulkMergeAsync(toMerge);

                Assert.Equal(expectedRecordsAffected, numRowsAffected);

                var dbCount = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .CountAsync();

                Assert.Equal(toMerge.Count, dbCount);

                foreach (var m in toMerge)
                {
                    var dbEntity = await db.EfCoreTestChild.FindAsync(m.Id, m.EfCoreTestId);

                    Assert.NotNull(dbEntity);
                    Assert.Equal(m.Name, dbEntity.Name);
                    Assert.Equal(m.ModifiedDate.ToString("G"), dbEntity.ModifiedDate.ToString("G"));
                    Assert.Equal(m.CreatedDate.ToString("G"), dbEntity.CreatedDate.ToString("G"));
                }
            }
        }

        [Fact(DisplayName = "NpgSql Composite Key")]
        public async Task MergesNpgSqlComposite()
        {
            List<Efcoretestchild> entities;

            using (var db = new postgresContext())
            {
                entities = await db.Efcoretestchild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();
            }

            const int numUpdate = 5;

            var toMerge = entities
                .Take(numUpdate)
                .Select(x =>
                {
                    x.Modifieddate = DateTime.UtcNow.AddDays(x.Id);
                    return x;
                })
                .ToList();

            var numDelete = entities.Count - toMerge.Count;

            var numAdd = 2;

            for (var i = 0; i < numAdd; i++)
            {
                toMerge.Add(new Efcoretestchild
                {
                    Efcoretestid = toMerge[0].Efcoretestid,
                    Name = $"{nameof(BulkMergeTests)}_insert_{i}",
                    Modifieddate = DateTime.Now,
                    Createddate = DateTime.Now
                });
            }

            var expectedRecordsAffected = numUpdate + numDelete + numAdd;

            using (var db = new postgresContext())
            {
                var numRowsAffected = await db.Efcoretestchild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .BulkMergeAsync(toMerge);

                Assert.Equal(expectedRecordsAffected, numRowsAffected);

                var dbCount = await db.Efcoretestchild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .CountAsync();

                Assert.Equal(toMerge.Count, dbCount);

                foreach (var m in toMerge)
                {
                    var dbEntity = await db.Efcoretestchild.FindAsync(m.Id, m.Efcoretestid);

                    Assert.NotNull(dbEntity);
                    Assert.Equal(m.Name, dbEntity.Name);
                    Assert.Equal(m.Modifieddate.ToString("d"), dbEntity.Modifieddate.ToString("d"));
                    Assert.Equal(m.Createddate.ToString("d"), dbEntity.Createddate.ToString("d"));
                }
            }
        }

        [Fact(DisplayName = "InMem Composite Key")]
        public async Task MergesInMemComposite()
        {
            List<EfCoreTestChild> entities;

            using (var db = new woodmanContext(InMemDbOpts))
            {
                entities = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();
            }

            const int numUpdate = 5;

            var toMerge = entities
                .Take(numUpdate)
                .Select(x =>
                {
                    x.ModifiedDate = DateTime.UtcNow.AddDays(x.Id);
                    return x;
                })
                .ToList();

            var numDelete = entities.Count - toMerge.Count;

            var numAdd = 2;

            for (var i = 0; i < numAdd; i++)
            {
                toMerge.Add(new EfCoreTestChild
                {
                    Id = 12341235 - i,
                    EfCoreTestId = toMerge[0].EfCoreTestId,
                    Name = $"{nameof(BulkMergeTests)}_insert_{i}",
                    ModifiedDate = DateTime.Now,
                    CreatedDate = DateTime.Now
                });
            }

            var expectedRecordsAffected = numUpdate + numDelete + numAdd;

            using (var db = new woodmanContext(InMemDbOpts))
            {
                var numRowsAffected = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .BulkMergeAsync(toMerge);

                Assert.Equal(expectedRecordsAffected, numRowsAffected);

                var dbCount = await db.EfCoreTestChild
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .CountAsync();

                Assert.Equal(toMerge.Count, dbCount);

                foreach (var m in toMerge)
                {
                    var dbEntity = await db.EfCoreTestChild.FindAsync(m.Id, m.EfCoreTestId);

                    Assert.NotNull(dbEntity);
                    Assert.Equal(m.Name, dbEntity.Name);
                    Assert.Equal(m.ModifiedDate.ToString("G"), dbEntity.ModifiedDate.ToString("G"));
                    Assert.Equal(m.CreatedDate.ToString("G"), dbEntity.CreatedDate.ToString("G"));
                }
            }
        }
    }
}
