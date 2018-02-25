using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal abstract class EntityInfoBase
    {
        public abstract IQueryable<TEntity> Join<TEntity>(IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter)
            where TEntity : class;

        public abstract Task<int> BulkMergeAsync<TEntity>(IQueryable<TEntity> queryable, List<TEntity> current, DbContext dbContext)
            where TEntity : class;

        public abstract Task<int> BulkRemoveAsync<TKey, TEntity>(IQueryable<TEntity> queryable, bool hasKeys, List<TKey> toRemove, DbContext dbContext)
            where TEntity : class;

        public abstract Task BulkAddAsync<TEntity>(List<TEntity> toAdd, DbContext dbContext)
            where TEntity : class;

        public abstract Task<int> BulkUpdateAsync<TEntity>(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties, DbContext dbContext)
            where TEntity : class;

        public abstract Task<int> BulkUpdateAsync<TKey, TEntity>(IQueryable<TEntity> queryable, List<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc, DbContext dbContext)
            where TEntity : class;

        public bool HasPrimaryKey { get; }

        public IEntityType EntityType { get; }

        public string PrimaryKeyName { get; }

        public bool IsPrimaryKeyGenerated { get; }

        private PropertyInfo PrimaryKeyProperty { get; }

        private object PrimaryKeyDefaultValue { get; }

        public EntityInfoBase(IEntityType entityType)
        {
            EntityType = entityType;

            var pk = EntityType.FindPrimaryKey();

            HasPrimaryKey = pk != null;

            if (HasPrimaryKey)
            {
                PrimaryKeyName = pk.Properties[0].Name;
                PrimaryKeyProperty = pk.Properties[0].PropertyInfo;
                PrimaryKeyDefaultValue = Activator.CreateInstance(PrimaryKeyProperty.PropertyType);
                IsPrimaryKeyGenerated = pk.Properties[0].ValueGenerated == ValueGenerated.OnAdd;
            }
        }

        public object GetPrimaryKey(object entity)
        {
            return PrimaryKeyProperty.GetValue(entity);
        }

        public void SetPrimaryKey(object entity, object keyVal)
        {
            PrimaryKeyProperty.SetValue(entity, keyVal);
        }

        public bool PrimaryKeyEquals(object entity1, object entity2)
        {
            return GetPrimaryKey(entity1)?.Equals(GetPrimaryKey(entity2)) ?? false;
        }

        public bool IsPrimaryKeyUnset(object entity)
        {
            var val = GetPrimaryKey(entity);

            return val == null
                ? PrimaryKeyDefaultValue == null
                : val.Equals(PrimaryKeyDefaultValue);
        }
    }
}
