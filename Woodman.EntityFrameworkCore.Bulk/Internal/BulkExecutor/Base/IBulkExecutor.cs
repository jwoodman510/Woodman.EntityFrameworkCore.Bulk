using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal interface IBulkExecutor<TEntity> where TEntity : class
    {
        IQueryable<TEntity> Join(IQueryable<TEntity> queryable, IEnumerable<object[]> keys, char delimiter);

        Task<int> BulkRemoveAsync(IQueryable<TEntity> queryable, bool hasKeys, List<object[]> keys);

        Task BulkAddAsync(List<TEntity> entities);

        Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties);

        Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, List<object[]> keys, List<string> updateProperties, Func<object[], TEntity> updateFunc);

        Task<int> BulkMergeAsync(IQueryable<TEntity> queryable, List<TEntity> current);
    }
}
