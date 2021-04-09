using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal interface IBulkExecutor<TEntity> where TEntity : class
    {
        IQueryable<TEntity> Join(DbSet<TEntity> rootQuery, List<object[]> keys, char delimiter);

        Task<int> BulkRemoveAsync(IQueryable<TEntity> queryable, bool hasKeys, List<object[]> keys);

        Task BulkAddAsync(List<TEntity> entities);

        Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties);

        Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, List<object[]> keys, List<string> updateProperties, Func<object[], TEntity> updateFunc);

        Task<int> BulkMergeAsync(
            IQueryable<TEntity> queryable,
            List<TEntity> current,
            Func<TEntity, object> updateColumnsToExlcude = null,
            BulkMergeNotMatchedBehavior notMatchedBehavior = BulkMergeNotMatchedBehavior.DoNothing,
            Expression<Func<TEntity>> whenNotMatched = null);
    }
}
