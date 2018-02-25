using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkMerge
    {
        public static async Task<int> BulkMergeAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TEntity> entities) where TEntity : class
        {
            var current = entities?.ToList() ?? new List<TEntity>();

            if (current.Count == 0)
            {
                return 0;
            }

            return await queryable
                .BuildBulkExecutor()
                .BulkMergeAsync(queryable, current);            
        }
    }
}
