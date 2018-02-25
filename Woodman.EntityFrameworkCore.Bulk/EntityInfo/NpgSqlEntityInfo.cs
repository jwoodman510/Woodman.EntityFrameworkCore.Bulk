using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;
using System.Linq;


namespace Woodman.EntityFrameworkCore.Bulk.EntityInfo
{
    internal class NpgSqlEntityInfo : EntityInfoBase
    {
        public string TableName { get; }

        public string PrimaryKeyColumnName { get; }

        public string PrimaryKeyColumnType { get; }

        public List<NpgSqlPropertyMapping> PropertyMappings { get; }

        public NpgSqlEntityInfo(IEntityType entityType)
            : base(entityType)
        {
            TableName = EntityType.Npgsql()?.TableName;

            var primaryKeyProperty = EntityType.FindPrimaryKey()?.Properties?.FirstOrDefault();
            var primaryKeyPropertyAnnotations = primaryKeyProperty?.Npgsql();

            PrimaryKeyColumnName = primaryKeyPropertyAnnotations?.ColumnName;
            PrimaryKeyColumnType = primaryKeyPropertyAnnotations?.ColumnType;

            PropertyMappings = EntityType.GetProperties()?.ToList()?.Select(p => new NpgSqlPropertyMapping(p))?.ToList();
        }

        public class NpgSqlPropertyMapping
        {
            private readonly IProperty _property;

            public string PropertyName => _property.Name;

            public string ColumnName { get; }

            public string ColumnType { get; }

            public bool IsNullable { get; }

            public bool IsPrimaryKey { get; }

            public NpgSqlPropertyMapping(IProperty property)
            {
                _property = property;
                IsNullable = _property?.IsNullable ?? false;
                IsPrimaryKey = _property.IsPrimaryKey();

                var propertyAnnotations = _property.Npgsql();

                ColumnName = propertyAnnotations?.ColumnName;
                ColumnType = (propertyAnnotations?.ColumnType ?? string.Empty) + (IsNullable ? " NULL" : string.Empty);
            }

            public object GetPropertyValue(object entity)
            {
                return _property.PropertyInfo.GetValue(entity);
            }

            public string GetDbValue(object entity)
            {
                var val = _property.PropertyInfo.GetValue(entity);

                return val == null
                    ? "NULL"
                    : $"'{val.ToString().Replace("'", "''")}'";
            }
        }
    }
}
