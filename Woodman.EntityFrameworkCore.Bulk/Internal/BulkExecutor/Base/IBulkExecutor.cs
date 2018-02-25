using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal interface IBulkExecutor<TEntity> where TEntity : class
    {
        IQueryable<TEntity> Join(IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter);

        Task<int> BulkRemoveAsync<TKey>(IQueryable<TEntity> queryable, bool hasKeys, List<TKey> keys);

        Task BulkAddAsync(List<TEntity> entities);

        Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties);

        Task<int> BulkUpdateAsync<TKey>(IQueryable<TEntity> queryable, List<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc);

        Task<int> BulkMergeAsync(IQueryable<TEntity> queryable, List<TEntity> current);
    }
}
