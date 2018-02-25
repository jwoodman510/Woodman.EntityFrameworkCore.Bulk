using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Reflection;

namespace Woodman.EntityFrameworkCore.Bulk.EntityInfo
{
    internal class EntityInfoBase
    {
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
