using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal class SqlEntityInfo : EntityInfoBase
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

        public string TableName { get; }

        public string PrimaryKeyColumnName { get; }

        public string PrimaryKeyColumnType { get; }

        public List<SqlPropertyMapping> PropertyMappings { get; }

        public SqlEntityInfo(IEntityType entityType)
            : base(entityType)
        {
            TableName = EntityType.SqlServer()?.TableName;

            var primaryKeyProperty = EntityType.FindPrimaryKey()?.Properties?.FirstOrDefault();
            var primaryKeyPropertyAnnotations = primaryKeyProperty?.SqlServer();

            PrimaryKeyColumnName = primaryKeyPropertyAnnotations?.ColumnName;
            PrimaryKeyColumnType = primaryKeyPropertyAnnotations?.ColumnType;

            PropertyMappings = EntityType.GetProperties()?.ToList()?.Select(p => new SqlPropertyMapping(p))?.ToList();
        }

        public class SqlPropertyMapping
        {
            private readonly IProperty _property;

            public string PropertyName => _property.Name;

            public string ColumnName { get; }

            public string ColumnType { get; }

            public bool IsNullable { get; }

            public bool IsPrimaryKey { get; }

            public SqlPropertyMapping(IProperty property)
            {
                _property = property;
                IsNullable = _property?.IsNullable ?? false;
                IsPrimaryKey = _property.IsPrimaryKey();

                var propertyAnnotations = _property.SqlServer();

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
