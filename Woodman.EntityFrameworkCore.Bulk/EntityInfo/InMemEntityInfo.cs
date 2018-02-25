using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal class InMemEntityInfo : EntityInfoBase
    {
        public override IQueryable<TEntity> Join<TEntity>(IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter)
        {
            return queryable.Join(keys, delimiter, this);
        }

        public override async Task<int> BulkMergeAsync<TEntity>(IQueryable<TEntity> queryable, List<TEntity> current, DbContext dbContext)
        {
            return await queryable.BulkMergeAsync(current, dbContext, this);
        }

        public override async Task<int> BulkRemoveAsync<TKey, TEntity>(IQueryable<TEntity> queryable, bool hasKeys, List<TKey> toRemove, DbContext dbContext)
        {
            return await queryable.BulkRemoveAsync(toRemove, hasKeys, dbContext, this);
        }

        public override async Task BulkAddAsync<TEntity>(List<TEntity> toAdd, DbContext dbContext)
        {
            await BulkAdd.BulkAddAsync(toAdd, dbContext, this);
        }

        public override async Task<int> BulkUpdateAsync<TEntity>(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties, DbContext dbContext)
        {
            return await queryable.BulkUpdateAsync(updatedEntity, updateProperties, dbContext, this);
        }

        public override async Task<int> BulkUpdateAsync<TKey, TEntity>(IQueryable<TEntity> queryable, List<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc, DbContext dbContext)
        {
            return await queryable.BulkUpdateAsync(keys, updateProperties, updateFunc, dbContext, this);
        }

        public List<IProperty> Properties => EntityType?.GetProperties()?.ToList();

        public InMemEntityInfo(IEntityType entityType)
            : base(entityType)
        {

        }
    }
}
