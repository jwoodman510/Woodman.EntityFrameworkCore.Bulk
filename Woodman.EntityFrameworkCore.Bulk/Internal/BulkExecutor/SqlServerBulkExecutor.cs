using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal class SqlServerBulkExecutor<TEntity> : RelationalBulkExecutor<TEntity>, IBulkExecutor<TEntity> where TEntity : class
    {
        public SqlServerBulkExecutor(DbContext dbContext) : base(dbContext) { }

        public IQueryable<TEntity> Join(IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter)
        {
            var sql = $@"
                SELECT a.* FROM dbo.{TableName} a
                JOIN dbo.Split({"{0}"}, '{delimiter}') as b ON a.{PrimaryKeyColumnName} = b.[Data]";

            var escapedKeys = keys.Select(k => k.Replace("'", "''"));

            return queryable.FromSql(sql, string.Join($"{delimiter}", escapedKeys));
        }

        public async Task<int> BulkRemoveAsync<TKey>(IQueryable<TEntity> queryable, bool filterKeys, List<TKey> keys)
        {
            var alias = $"d_{TableName}";
            var qryAlias = $"q_{TableName}";
            var kAlias = $"k_{TableName}";
            var keysParam = $"@keys_{TableName}";

            const string delimiter = ",";

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var keySql = filterKeys
                ? $"JOIN dbo.Split({keysParam}, '{delimiter}') as {kAlias} ON {kAlias}.[Data] = {alias}.{PrimaryKeyColumnName}"
                : string.Empty;

            var sqlCmd = $@"
                DELETE {alias} FROM {TableName} {alias}
                JOIN (
                    {qrySql}
                ) AS {qryAlias} ON {qryAlias}.{PrimaryKeyColumnName} = {alias}.{PrimaryKeyColumnName}
                {keySql}";

            var sqlParams = new List<SqlParameter>();

            if (filterKeys)
            {
                var escapedKeys = string.Join(delimiter, keys.Select(k => k.ToString().Replace("'", "''")));

                sqlParams.Add(new SqlParameter(keysParam, escapedKeys));
            }

            sqlParams.AddRange(parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        public async Task BulkAddAsync(List<TEntity> entities)
        {
            var tableVar = "@ToAdd";

            var props = PropertyMappings
                .Where(p => IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var columnsSql = $@"{string.Join(",", props.Select(p => $@"
                    {p.ColumnName}"))}";

            var deltaSql = string.Join(",", entities.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", props.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                DECLARE {tableVar} TABLE ({string.Join(", ", props.Select(m => $"{m.ColumnName} {m.ColumnType}"))})

                SET NOCOUNT ON

                INSERT INTO {tableVar} VALUES
                    {deltaSql}

                SET NOCOUNT OFF

                INSERT INTO dbo.{TableName}
                ({columnsSql})
                OUTPUT Inserted.{PrimaryKeyColumnName}
                SELECT
                {columnsSql}
                FROM {tableVar}";

            await ExecuteSqlCommandAsync(sqlCmd, entities);
        }

        public async Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties)
        {
            var alias = $"u_{TableName}";
            var qryAlias = $"q_{TableName}";

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var propertyMappings = PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .Select(p => new
                {
                    PropertyMapping = p,
                    SqlParam = new SqlParameter($"@{TableName}_{p.PropertyName}", p.GetPropertyValue(updatedEntity) ?? DBNull.Value)
                }).ToList();

            var sqlParams = new List<SqlParameter>();

            sqlParams.AddRange(parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
            sqlParams.AddRange(propertyMappings.Select(p => p.SqlParam));

            var columnSql = string.Join(",", propertyMappings.Select(prop => $@"
                    {alias}.{prop.PropertyMapping.ColumnName} = {prop.SqlParam.ParameterName}"));

            var sqlCmd = $@"
                UPDATE {alias}
                SET {columnSql}
                FROM dbo.{TableName} {alias}
                JOIN (
                   {qrySql}
                ) AS {qryAlias} ON {qryAlias}.{PrimaryKeyColumnName} = {alias}.{PrimaryKeyColumnName}";

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        public async Task<int> BulkUpdateAsync<TKey>(IQueryable<TEntity> queryable, List<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc)
        {
            var alias = $"u_{TableName}";
            var qryAlias = $"q_{TableName}";
            var deltaAlias = $"d_{TableName}";
            var tableVar = "@Delta";

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var propertyMappings = PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .ToDictionary(x => x.PropertyName);

            var deltaSql = string.Join(",", keys.Select(key =>
            {
                var entity = updateFunc(key);
                var keyVal = key.ToString().Replace("'", "''");

                return $@"
                    ('{keyVal}', {string.Join(", ", propertyMappings.Select(m => $"{m.Value.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                DECLARE {tableVar} TABLE ({PrimaryKeyColumnName} {PrimaryKeyColumnType}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.ColumnName} {m.Value.ColumnType}"))})

                SET NOCOUNT ON

                INSERT INTO {tableVar} VALUES
                    {deltaSql}

                SET NOCOUNT OFF

                UPDATE {alias}
                SET
                    {string.Join(",", updateProperties.Select(prop => $@"
                    {alias}.{propertyMappings[prop].ColumnName} = {deltaAlias}.{propertyMappings[prop].ColumnName}"))}
                FROM dbo.{TableName} {alias}
                JOIN (
                   {qrySql}
                ) AS {qryAlias} ON {qryAlias}.{PrimaryKeyColumnName} = {alias}.{PrimaryKeyColumnName}
                JOIN {tableVar} {deltaAlias} ON {deltaAlias}.{PrimaryKeyColumnName} = {alias}.{PrimaryKeyColumnName}";

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }

        public async Task<int> BulkMergeAsync(IQueryable<TEntity> queryable, List<TEntity> current)
        {
            var deltaSql = string.Join(",", current.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", PropertyMappings.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var updateColumnSql = string.Join(",", PropertyMappings.Where(p => IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true).Select(p => $@"
                TARGET.{p.ColumnName} = SOURCE.{p.ColumnName}"));

            var insertColumnSql = string.Join(",", PropertyMappings.Where(p => IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true).Select(p => $@"
                {p.ColumnName}"));

            var insertSourceColumnSql = string.Join(",", PropertyMappings.Where(p => IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true).Select(p => $@"
                SOURCE.{p.ColumnName}"));

            var sqlCmd = $@"
                DECLARE @merge TABLE ({string.Join(", ", PropertyMappings.Select(m => $"{m.ColumnName} {m.ColumnType}"))})

                SET NOCOUNT ON

                INSERT INTO @merge VALUES
                    {deltaSql}

                SET NOCOUNT OFF

                ;
                WITH tgt_cte AS (
                    {qrySql}
                )
                MERGE tgt_cte AS TARGET
                USING @merge AS SOURCE ON (TARGET.{PrimaryKeyColumnName} = SOURCE.{PrimaryKeyColumnName})
                WHEN MATCHED THEN
                UPDATE SET {updateColumnSql}
                WHEN NOT MATCHED BY TARGET THEN
                INSERT ({insertColumnSql})
                VALUES ({insertSourceColumnSql})
                WHEN NOT MATCHED BY SOURCE THEN
                DELETE
                OUTPUT inserted.{PrimaryKeyColumnName}, $action AS action;";

            return await ExecuteSqlCommandAsync(sqlCmd, current, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }
    }
}
