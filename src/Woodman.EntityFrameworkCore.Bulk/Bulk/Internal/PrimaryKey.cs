using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Metadata;

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
            public string Name => _property.Name;

            public PropertyInfo Property => _property.PropertyInfo;

            public bool IsGenerated => _property.ValueGenerated == ValueGenerated.OnAdd;

            public string ColumnName => _property.GetColumnName();

            public string ColumnType => _property.IsNullable
                ? $"{_property.GetColumnType()} NULL"
                : _property.GetColumnType();

            public object DefaultValue
            {
                get
                {
                    if (Property.PropertyType.IsValueType)
                    {
                        return Activator.CreateInstance(Property.PropertyType);
                    }
                    else
                    {
                        var constructor = Property.PropertyType.GetConstructor(Type.EmptyTypes);

                        if (constructor != null)
                        {
                            return Activator.CreateInstance(Property.PropertyType);
                        }

                        return null;
                    }
                }
            }

            private readonly IProperty _property;

            public Key(IProperty property)
            {
                _property = property;
            }
        }
    }
}