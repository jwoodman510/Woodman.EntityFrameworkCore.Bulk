using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal class InMemBulkExecutor<TEntity> : BulkExecutor<TEntity>, IBulkExecutor<TEntity> where TEntity : class
    {
        public InMemBulkExecutor(DbContext dbContext) : base(dbContext){ }

        public IQueryable<TEntity> Join(IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter)
        {
            var primKeys = new HashSet<string>(keys);

            return queryable.Where(entity => primKeys.Contains(GetPrimaryKey(entity).ToString()));
        }

        public async Task<int> BulkRemoveAsync<TKey>(IQueryable<TEntity> queryable, bool filterKeys, List<TKey> keys)
        {
            var primKeys = new HashSet<string>(keys.Select(k => k.ToString()));

            var entities = filterKeys
                ? queryable
                    .Where(entity => primKeys.Contains(GetPrimaryKey(entity).ToString()))
                    .ToList()
                : queryable.ToList();

            foreach (var entity in entities)
            {
                DbContext.Remove(entity);
            }

            if (entities.Count > 0)
            {
                DbContext.SaveChanges();
            }

            return await Task.FromResult(entities.Count);
        }

        public async Task BulkAddAsync(List<TEntity> entities)
        {
            await DbContext.AddRangeAsync(entities);
            await DbContext.SaveChangesAsync();
        }

        public async Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties)
        {
            var updatePropDict = updateProperties.ToDictionary(key => key, value => typeof(TEntity).GetProperty(value));

            var entities = queryable.ToList();

            foreach (var entity in entities)
            {
                foreach (var updateProp in updatePropDict)
                {
                    var propInfo = updatePropDict[updateProp.Key];

                    propInfo.SetValue(entity, propInfo.GetValue(updatedEntity));
                }
            }

            if (entities.Count > 0)
            {
                DbContext.SaveChanges();
            }

            return await Task.FromResult(entities.Count);
        }

        public async Task<int> BulkUpdateAsync<TKey>(IQueryable<TEntity> queryable, List<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc)
        {
            var primKeys = keys.ToDictionary(k => k.ToString());

            var entities = queryable.Where(entity => primKeys.ContainsKey(GetPrimaryKey(entity).ToString()))
                .ToList();

            var updatePropDict = updateProperties.ToDictionary(key => key, value => typeof(TEntity).GetProperty(value));

            foreach (var entity in entities)
            {
                var key = primKeys[GetPrimaryKey(entity).ToString()];
                var updatedEntity = updateFunc(key);

                foreach (var updateProp in updatePropDict)
                {
                    var propInfo = updatePropDict[updateProp.Key];

                    propInfo.SetValue(entity, propInfo.GetValue(updatedEntity));
                }
            }

            if (entities.Count > 0)
            {
                DbContext.SaveChanges();
            }

            return await Task.FromResult(entities.Count);
        }

        public async Task<int> BulkMergeAsync(IQueryable<TEntity> queryable, List<TEntity> current)
        {
            var previous = await queryable.ToListAsync();

            var toDelete = previous
                .Where(p => current.All(c => !PrimaryKeyEquals(c, p)))
                .ToList();

            var toAdd = current
                .Where(c => previous.All(p => !PrimaryKeyEquals(c, p)))
                .ToList();

            var toUpdate = previous
                .Where(p => current.Any(c => PrimaryKeyEquals(c, p)))
                .ToList();

            DbContext.RemoveRange(toDelete);

            await DbContext.AddRangeAsync(toAdd);

            foreach (var entity in toUpdate)
            {
                var updatedEntity = current.First(c => PrimaryKeyEquals(c, entity));

                foreach (var property in Properties)
                {
                    property.PropertyInfo.SetValue(entity, property.PropertyInfo.GetValue(updatedEntity));
                }
            }

            return await DbContext.SaveChangesAsync();
        }
    }
}
