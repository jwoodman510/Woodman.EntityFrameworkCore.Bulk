using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Woodman.EntityFrameworkCore.Bulk.EntityInfo
{
    internal class InMemEntityInfo : EntityInfoBase
    {
        public string PrimaryKeyName => EntityType?.FindPrimaryKey()?.Properties[0].Name;

        public PropertyInfo PrimaryKeyProperty => EntityType?.FindPrimaryKey()?.Properties[0].PropertyInfo;

        public List<IProperty> Properties => EntityType?.GetProperties()?.ToList();

        public InMemEntityInfo(IEntityType entityType)
            : base(entityType)
        {

        }

        public object GetPrimaryKey(object entity)
        {
            return PrimaryKeyProperty.GetValue(entity);
        }

        public bool PrimaryKeyEquals(object entity1, object entity2)
        {
            return GetPrimaryKey(entity1)?.Equals(GetPrimaryKey(entity2)) ?? false;
        }
    }
}
