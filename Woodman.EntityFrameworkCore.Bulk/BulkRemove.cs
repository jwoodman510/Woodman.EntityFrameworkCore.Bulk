using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
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

        private static async Task<int> BulkRemoveAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, bool filterKeys)
            where TEntity : class
        {
            var toRemove = keys?.ToList() ?? new List<TKey>();

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
