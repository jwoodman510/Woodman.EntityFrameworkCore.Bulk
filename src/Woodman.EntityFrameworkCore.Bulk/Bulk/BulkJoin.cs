using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;

namespace Woodman.EntityFrameworkCore.Bulk.Bulk
{
    public static class BulkJoin
    {
        public static IQueryable<TEntity> Join<TEntity>(this DbSet<TEntity> queryable, IEnumerable<int> keys)
            where TEntity : class
        {
            return queryable.Join(keys.Select(k => new object[] { k }));
        }

        public static IQueryable<TEntity> Join<TEntity>(this DbSet<TEntity> queryable, IEnumerable<long> keys)
            where TEntity : class
        {
            return queryable.Join(keys.Select(k => new object[] { k }));
        }

        public static IQueryable<TEntity> Join<TEntity>(this DbSet<TEntity> queryable, IEnumerable<string> keys, char delimiter = ',')
            where TEntity : class
        {
            return queryable.Join(keys.Select(k => new object[] { k }), delimiter);
        }

        public static IQueryable<TEntity> Join<TEntity>(this DbSet<TEntity> queryable, IEnumerable<object[]> keys, char delimiter = ',')
            where TEntity : class
        {
            var toFind = keys?.ToList() ?? new List<object[]>();

            if (toFind == null || toFind.Count == 0)
            {
                return queryable.Where(e => false);
            }

            return queryable
                .BuildBulkExecutor()
                .Join(queryable, toFind, delimiter);
        }
    }
}
