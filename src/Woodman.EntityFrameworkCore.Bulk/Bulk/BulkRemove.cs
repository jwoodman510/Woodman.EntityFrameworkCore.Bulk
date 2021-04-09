using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Woodman.EntityFrameworkCore.Bulk.Bulk
{
    public static class BulkRemove
    {
        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable)
            where TEntity : class
        {
            return await queryable.BulkRemoveAsync(new string[0], false);
        }

        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<int> keys)
            where TEntity : class
        {
            return await queryable.BulkRemoveAsync(keys, true);
        }

        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<long> keys)
            where TEntity : class
        {
            return await queryable.BulkRemoveAsync(keys, true);
        }

        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys)
            where TEntity : class
        {
            return await queryable.BulkRemoveAsync(keys, true);
        }

        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<object[]> keys)
            where TEntity : class
        {
            return await queryable.BulkRemoveAsync(keys, true);
        }

        private static async Task<int> BulkRemoveAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, bool filterKeys)
            where TEntity : class
        {
            var toRemove = typeof(TKey) == typeof(object[])
                ? keys?.Select(k => k as object[])?.ToList() ?? new List<object[]>()
                : keys?.Select(k => new object[] { k })?.ToList() ?? new List<object[]>();

            if (toRemove == null || toRemove.Count == 0 && filterKeys)
            {
                return 0;
            }

            return await queryable
                .BuildBulkExecutor()
                .BulkRemoveAsync(queryable, filterKeys, toRemove);
        }
    }
}
