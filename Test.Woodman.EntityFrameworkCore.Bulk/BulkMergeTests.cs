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

        [Fact]
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

            BulkMergeResult result;

            using (var db = new woodmanContext())
            {
                var minDate = DateTime.UtcNow.AddYears(-10);

                result = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)) && e.CreatedDate > minDate)
                    .BulkMergeAsync(toMerge);

                Assert.Equal(numAdd, result.InsertedIds.Length);
                Assert.Equal(expectedRecordsAffected, result.NumRowsAffected);

                var mergedEntities = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();

                Assert.Equal(toMerge.Count, mergedEntities.Count);

                foreach (var m in mergedEntities)
                {
                    if (result.InsertedIds.Contains(m.Id))
                    {
                        var index = result.InsertedIds.ToList().IndexOf(m.Id);

                        var expected = $"{nameof(BulkMergeTests)}_insert_{index}";

                        Assert.Equal(expected, m.Name);
                    }
                    else
                    {
                        var updated = toMerge.FirstOrDefault(x => x.Id == m.Id);

                        Assert.NotNull(updated);
                        Assert.Equal(updated.ModifiedDate.ToString("G"), m.ModifiedDate.ToString("G"));
                    }
                }

                var dontDeleteEntity = await db.EfCoreTest.FindAsync(dontDelete.Id);
                Assert.NotNull(dontDeleteEntity);
            }
        }

        [Fact]
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

            BulkMergeResult result;

            using (var db = new postgresContext())
            {
                var minDate = DateTime.UtcNow.AddYears(-10);

                result = await db.Efcoretest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)) && e.Createddate > minDate)
                    .BulkMergeAsync(toMerge);

                Assert.Equal(numAdd, result.InsertedIds.Length);
                Assert.Equal(expectedRecordsAffected, result.NumRowsAffected);

                var mergedEntities = await db.Efcoretest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();

                Assert.Equal(toMerge.Count, mergedEntities.Count);

                foreach (var m in mergedEntities)
                {
                    if (result.InsertedIds.Contains(m.Id))
                    {
                        var index = result.InsertedIds.ToList().IndexOf(m.Id);

                        var expected = $"{nameof(BulkMergeTests)}_insert_{index}";

                        Assert.Equal(expected, m.Name);
                    }
                    else
                    {
                        var updated = toMerge.FirstOrDefault(x => x.Id == m.Id);

                        Assert.NotNull(updated);
                        Assert.Equal(updated.Modifieddate.ToString("d"), m.Modifieddate.ToString("d"));
                    }
                }

                var dontDeleteEntity = await db.Efcoretest.FindAsync(dontDelete.Id);
                Assert.NotNull(dontDeleteEntity);
            }
        }

        [Fact]
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

            BulkMergeResult result;

            using (var db = new woodmanContext(InMemDbOpts))
            {
                result = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .BulkMergeAsync(toMerge);

                Assert.Equal(numAdd, result.InsertedIds.Length);
                Assert.Equal(expectedRecordsAffected, result.NumRowsAffected);

                var mergedEntities = await db.EfCoreTest
                    .Where(e => e.Name.Contains(nameof(BulkMergeTests)))
                    .ToListAsync();

                Assert.Equal(toMerge.Count, mergedEntities.Count);

                foreach (var m in mergedEntities)
                {
                    if (result.InsertedIds.Contains(m.Id))
                    {
                        var index = result.InsertedIds.ToList().IndexOf(m.Id);

                        var expected = $"{nameof(BulkMergeTests)}_insert_{index}";

                        Assert.Equal(expected, m.Name);
                    }
                    else
                    {
                        var updated = toMerge.FirstOrDefault(x => x.Id == m.Id);

                        Assert.NotNull(updated);
                        Assert.Equal(updated.ModifiedDate.ToString("G"), m.ModifiedDate.ToString("G"));
                    }
                }
            }
        }
    }
}
