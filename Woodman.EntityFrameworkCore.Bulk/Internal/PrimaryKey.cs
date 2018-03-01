using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore
{
    internal class PrimaryKey
    {
        public bool IsCompositeKey { get; }

        public IReadOnlyList<Key> Keys { get; }

        public Key Primary => IsCompositeKey ? null : Keys[0];

        public PrimaryKey(IEntityType entityType)
        {
            var key = entityType.FindPrimaryKey();

            IsCompositeKey = key.Properties.Count > 1;

            Keys = key.Properties.Select(prop => new Key(prop)).ToList();
        }

        public class Key
        {
            public string Name { get; }

            public PropertyInfo Property { get; }

            public object DefaultValue { get; }

            public bool IsGenerated { get; }

            public string ColumnName { get; }

            public string ColumnType { get; }

            public Key(IProperty property)
            {
                Name = property.Name;
                Property = property.PropertyInfo;
                DefaultValue = Activator.CreateInstance(Property.PropertyType);
                IsGenerated = property.ValueGenerated == ValueGenerated.OnAdd;

                var relationalInfo = property.Relational();

                if(relationalInfo != null)
                {
                    ColumnName = relationalInfo.ColumnName;
                    ColumnType = property.IsNullable
                        ? $"{relationalInfo.ColumnType} NULL"
                        : relationalInfo.ColumnType;
                }                
            }
        }
    }
}
