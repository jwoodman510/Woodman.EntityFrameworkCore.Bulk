using Woodman.EntityFrameworkCore.Bulk.Bulk.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal class InMemBulkExecutor<TEntity> : BulkExecutor<TEntity>, IBulkExecutor<TEntity> where TEntity : class
    {
        public InMemBulkExecutor(DbContext dbContext) : base(dbContext){ }

        public IQueryable<TEntity> Join(DbSet<TEntity> rootQuery, List<object[]> keys, char delimiter)
        {
            if (PrimaryKey.IsCompositeKey)
            {
                ValidateCompositeKeys(keys);

                var keyNames = PrimaryKey.Keys.Select(x => x.Name.ToString()).ToList();
                var keyPredicate = Predicates.ContainsCompositeKeys<TEntity>(keyNames, keys);

                return rootQuery.Where(keyPredicate);
            }
            else
            {
                var primKeys = new HashSet<object>(keys.Select(k => k[0]));
                var keyPredicate = Predicates.ContainsPrimaryKeys<TEntity>(PrimaryKey.Primary.Name, primKeys);

                return rootQuery.Where(keyPredicate);
            }
        }

        public async Task<int> BulkRemoveAsync(IQueryable<TEntity> queryable, bool filterKeys, List<object[]> keys)
        {
            ValidateCompositeKeys(keys);

            if (filterKeys)
            {
                if (PrimaryKey.IsCompositeKey)
                {
                    ValidateCompositeKeys(keys);

                    var keyNames = PrimaryKey.Keys.Select(x => x.Name.ToString()).ToList();
                    var keyPredicate = Predicates.ContainsCompositeKeys<TEntity>(keyNames, keys);

                    DbContext.RemoveRange(queryable.Where(keyPredicate));
                }
                else
                {
                    var primKeys = new HashSet<object>(keys.Select(k => k[0]));
                    var keyPredicate = Predicates.ContainsPrimaryKeys<TEntity>(PrimaryKey.Primary.Name, primKeys);

                    DbContext.RemoveRange(queryable.Where(keyPredicate));
                }
            }
            else
            {
                DbContext.RemoveRange(queryable);
            }

            var numDeleted = DbContext.SaveChanges();

            return await Task.FromResult(numDeleted);
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

        public async Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, List<object[]> keys, List<string> updateProperties, Func<object[], TEntity> updateFunc)
        {
            ValidateCompositeKeys(keys);

            List<TEntity> entities;

            if (PrimaryKey.IsCompositeKey)
            {
                ValidateCompositeKeys(keys);

                var keyNames = PrimaryKey.Keys.Select(x => x.Name.ToString()).ToList();
                var keyPredicate = Predicates.ContainsCompositeKeys<TEntity>(keyNames, keys);

                entities = queryable.Where(keyPredicate).ToList();
            }
            else
            {
                var primKeys = new HashSet<object>(keys.Select(k => k[0]));
                var keyPredicate = Predicates.ContainsPrimaryKeys<TEntity>(PrimaryKey.Primary.Name, primKeys);

                entities = queryable.Where(keyPredicate).ToList();
            }

            var updatePropDict = updateProperties.ToDictionary(key => key, value => typeof(TEntity).GetProperty(value));

            foreach (var entity in entities)
            {
                var key = PrimaryKey.IsCompositeKey
                    ? GetCompositeKey(entity)
                    : new object[] { GetPrimaryKey(entity) };

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

        public async Task<int> BulkMergeAsync(
            IQueryable<TEntity> queryable,
            List<TEntity> current,
            Func<TEntity, object> updateColumnsToExlcude = null,
            BulkMergeNotMatchedBehavior notMatchedBehavior = BulkMergeNotMatchedBehavior.DoNothing,
            Expression<Func<TEntity>> whenNotMatched = null)
        {
            var previous = await queryable.ToListAsync();

            var notMatched = previous
                .Where(p => current.All(c => !PrimaryKeyEquals(c, p)))
                .ToList();

            var toAdd = current
                .Where(c => previous.All(p => !PrimaryKeyEquals(c, p)))
                .ToList();

            var toUpdate = previous
                .Where(p => current.Any(c => PrimaryKeyEquals(c, p)))
                .ToList();

            switch (notMatchedBehavior)
            {
                case BulkMergeNotMatchedBehavior.Delete:
                    DbContext.RemoveRange(notMatched);
                    break;
                case BulkMergeNotMatchedBehavior.Update when whenNotMatched != null:
                    foreach (var entity in notMatched)
                    {
                        var notMatchedProps = whenNotMatched.GetSetPropertyNames();
                        var notMatchedEntity = whenNotMatched.Compile().Invoke();

                        foreach (var property in Properties.Where(x => notMatchedProps.Contains(x.Name)))
                        {
                            property.PropertyInfo.SetValue(entity, property.PropertyInfo.GetValue(notMatchedEntity));
                        }
                    }
                    break;
            }

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
