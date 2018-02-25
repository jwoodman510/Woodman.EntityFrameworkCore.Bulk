using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkUpdate
    {
        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, Expression<Func<TEntity>> updateFactory)
            where TEntity : class
        {
            var memberInitExpression = updateFactory.EnsureMemberInitExpression();
            var updateFunc = updateFactory.Compile();
            var updatedEntity = updateFunc();

            var updateProperties = memberInitExpression.Bindings.Select(b => b.Member.Name).ToList();

            if (updateProperties.Count == 0)
            {
                return 0;
            }

            return await queryable
                .BuildBulkExecutor()
                .BulkUpdateAsync(queryable, updatedEntity, updateProperties);
        }

        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<int> keys, Expression<Func<int, TEntity>> updateFactory)
            where TEntity : class
        {
            return await queryable.BulkUpdateAsync<int, TEntity>(keys, updateFactory);
        }

        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<long> keys, Expression<Func<long, TEntity>> updateFactory)
            where TEntity : class
        {
            return await queryable.BulkUpdateAsync<long, TEntity>(keys, updateFactory);
        }

        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys, Expression<Func<string, TEntity>> updateFactory)
            where TEntity : class
        {
            return await queryable.BulkUpdateAsync<string, TEntity>(keys, updateFactory);
        }

        private static async Task<int> BulkUpdateAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, Expression<Func<TKey, TEntity>> updateFactory)
            where TEntity : class
        {
            var toUpdate = keys?.ToList() ?? new List<TKey>();

            if (toUpdate == null || toUpdate.Count == 0)
            {
                return 0;
            }

            var memberInitExpression = updateFactory.EnsureMemberInitExpression();

            var updateProperties = memberInitExpression.Bindings
                .Select(b => b.Member.Name)
                .ToList();

            if (updateProperties.Count == 0)
            {
                return 0;
            }

            var updateFunc = updateFactory.Compile();

            return await queryable
                .BuildBulkExecutor()
                .BulkUpdateAsync(queryable, toUpdate, updateProperties, updateFunc);
        }   
    }
}
