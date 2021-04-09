using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Woodman.EntityFrameworkCore.Bulk.Bulk
{
    public static class BulkMerge
    {
        public static async Task<int> BulkMergeAsync<TEntity>(
            this IQueryable<TEntity> queryable,
            IEnumerable<TEntity> entities,
            Func<TEntity, object> updateColumnsToExlcude = null,
            BulkMergeNotMatchedBehavior notMatchedBehavior = BulkMergeNotMatchedBehavior.DoNothing,
            Expression<Func<TEntity>> whenNotMatched = null)
            where TEntity : class
        {
            var current = entities?.ToList() ?? new List<TEntity>();

            if (current.Count == 0)
            {
                return 0;
            }

            whenNotMatched?.EnsureMemberInitExpression();

            return await queryable
                .BuildBulkExecutor()
                .BulkMergeAsync(queryable, current, updateColumnsToExlcude, notMatchedBehavior, whenNotMatched);
        }
    }
}