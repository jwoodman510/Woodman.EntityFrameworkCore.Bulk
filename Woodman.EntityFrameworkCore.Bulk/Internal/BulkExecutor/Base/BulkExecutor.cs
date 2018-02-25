using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore
{
    internal abstract class BulkExecutor<TEntity>
    {
        protected DbContext DbContext { get; }

        protected IEntityType EntityType { get; }

        protected List<IProperty> Properties { get; }

        protected bool HasPrimaryKey { get; }

        protected string PrimaryKeyName { get; }

        protected bool IsPrimaryKeyGenerated { get; }

        protected PropertyInfo PrimaryKeyProperty { get; }

        protected object PrimaryKeyDefaultValue { get; }

        protected BulkExecutor(DbContext dbContext)
        {
            DbContext = dbContext;

            EntityType = dbContext.Model.FindEntityType(typeof(TEntity));

            Properties = EntityType.GetProperties()?.ToList();

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

        protected object GetPrimaryKey(TEntity entity)
        {
            return PrimaryKeyProperty.GetValue(entity);
        }

        protected void SetPrimaryKey(TEntity entity, object keyVal)
        {
            PrimaryKeyProperty.SetValue(entity, keyVal);
        }

        protected bool PrimaryKeyEquals(TEntity entity1, TEntity entity2)
        {
            return GetPrimaryKey(entity1)?.Equals(GetPrimaryKey(entity2)) ?? false;
        }

        protected bool IsPrimaryKeyUnset(TEntity entity)
        {
            var val = GetPrimaryKey(entity);

            return val == null
                ? PrimaryKeyDefaultValue == null
                : val.Equals(PrimaryKeyDefaultValue);
        }
    }
}
