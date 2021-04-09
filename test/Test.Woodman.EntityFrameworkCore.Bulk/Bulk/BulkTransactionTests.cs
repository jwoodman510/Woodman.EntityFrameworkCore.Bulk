using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql;
using Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Woodman.EntityFrameworkCore.Bulk.Bulk;

namespace Test.Woodman.EntityFrameworkCore.Bulk.Bulk
{
    public class BulkTransactionTests
    {
        private static int InitialCount => 2000;

        public BulkTransactionTests()
        {
            Init();
        }

        [Fact(DisplayName = "Sql Transaction - Commit", Skip = "Manual")]
        public async Task SqlCommit()
        {
            using (var db = new woodmanContext())
            using(var scope = db.Database.CurrentTransaction ?? db.Database.BeginTransaction())
            {
                var toAdd = new EfCoreTest
                {
                    Name = $"{nameof(BulkTransactionTests)}_Added",
                    CreatedDate = DateTime.Now,
                    ModifiedDate = DateTime.Now
                };

                await db.EfCoreTest.AddAsync(toAdd);
                await db.SaveChangesAsync(false);

                var ids = await db.EfCoreTest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .Select(x => x.Id)
                    .ToListAsync();

                await db.EfCoreTest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .BulkRemoveAsync(ids);

                scope.Commit();
            }

            using (var db = new woodmanContext())
            {
                var count = await db.EfCoreTest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .CountAsync();

                Assert.Equal(0, count);
            }
        }

        [Fact(DisplayName = "Sql Transaction - Rollback", Skip = "Manual")]
        public async Task SqlRollback()
        {
            using (var db = new woodmanContext())
            using (var scope = db.Database.CurrentTransaction ?? db.Database.BeginTransaction())
            {
                var toAdd = new EfCoreTest
                {
                    Name = $"{nameof(BulkTransactionTests)}_Added",
                    CreatedDate = DateTime.Now,
                    ModifiedDate = DateTime.Now
                };

                await db.EfCoreTest.AddAsync(toAdd);
                await db.SaveChangesAsync(false);

                var ids = await db.EfCoreTest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .Select(x => x.Id)
                    .ToListAsync();

                await db.EfCoreTest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .BulkRemoveAsync(ids);

                scope.Rollback();
            }

            using (var db = new woodmanContext())
            {
                var count = await db.EfCoreTest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .CountAsync();

                Assert.Equal(InitialCount, count);
            }
        }

        [Fact(DisplayName = "NpgSql Transaction - Commit", Skip = "Manual")]
        public async Task NpgSqlCommit()
        {
            using (var db = new postgresContext())
            using (var scope = db.Database.CurrentTransaction ?? db.Database.BeginTransaction())
            {
                var toAdd = new Efcoretest
                {
                    Name = $"{nameof(BulkTransactionTests)}_Added",
                    Createddate = DateTime.Now,
                    Modifieddate = DateTime.Now
                };

                await db.Efcoretest.AddAsync(toAdd);
                await db.SaveChangesAsync(false);

                var ids = await db.Efcoretest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .Select(x => x.Id)
                    .ToListAsync();

                await db.Efcoretest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .BulkRemoveAsync(ids);

                scope.Commit();
            }

            using (var db = new postgresContext())
            {
                var count = await db.Efcoretest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .CountAsync();

                Assert.Equal(0, count);
            }
        }

        [Fact(DisplayName = "NpgSql Transaction - Rollback", Skip = "Manual")]
        public async Task NpgSqlRollback()
        {
            using (var db = new postgresContext())
            using (var scope = db.Database.CurrentTransaction ?? db.Database.BeginTransaction())
            {
                var toAdd = new Efcoretest
                {
                    Name = $"{nameof(BulkTransactionTests)}_Added",
                    Createddate = DateTime.Now,
                    Modifieddate = DateTime.Now
                };

                await db.Efcoretest.AddAsync(toAdd);
                await db.SaveChangesAsync(false);

                var ids = await db.Efcoretest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .Select(x => x.Id)
                    .ToListAsync();

                await db.Efcoretest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .BulkRemoveAsync(ids);

                scope.Rollback();
            }

            using (var db = new postgresContext())
            {
                var count = await db.Efcoretest
                    .Where(x => x.Name.Contains(nameof(BulkTransactionTests)))
                    .CountAsync();

                Assert.Equal(InitialCount, count);
            }
        }

        private static void Init()
        {
            using (var db = new woodmanContext())
            {
                var entities = db.EfCoreTest.Where(e => e.Name.Contains(nameof(BulkTransactionTests))).ToList();

                if (entities.Count > 0)
                {
                    db.RemoveRange(entities);
                }

                for(var i = 0; i < InitialCount; i++)
                {
                    db.Add(new EfCoreTest
                    {
                        Name = nameof(BulkTransactionTests),
                        CreatedDate = DateTime.Now,
                        ModifiedDate = DateTime.Now
                    });
                }

                db.SaveChanges();
            }

            using (var db = new postgresContext())
            {
                var entities = db.Efcoretest.Where(e => e.Name.Contains(nameof(BulkTransactionTests))).ToList();

                if (entities.Count > 0)
                {
                    db.RemoveRange(entities);
                }

                for (var i = 0; i < InitialCount; i++)
                {
                    db.Add(new Efcoretest
                    {
                        Name = nameof(BulkTransactionTests),
                        Createddate = DateTime.Now,
                        Modifieddate = DateTime.Now
                    });
                }

                db.SaveChanges();
            }
        }
    }
}
