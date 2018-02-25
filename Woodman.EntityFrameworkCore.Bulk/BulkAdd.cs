using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkAdd
    {
        public static async Task BulkAddAsync<TEntity>(this DbSet<TEntity> queryable, IEnumerable<TEntity> entities)
            where TEntity : class
        {
            var toAdd = entities?.ToList() ?? new List<TEntity>();

            if (toAdd.Count == 0)
            {
                return;
            }

            await queryable
                .BuildBulkExecutor()
                .BulkAddAsync(toAdd);
        }
    }
}
