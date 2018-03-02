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

        public IQueryable<TEntity> Join(IQueryable<TEntity> queryable, IEnumerable<object[]> keys, char delimiter)
        {
            var sql = $@"
                DECLARE @Keys TABLE ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))})

                INSERT INTO @Keys VALUES {string.Join(",", keys.Select(k => $@"
                    ({string.Join(", ", k.Select(val => StringifyKeyVal(val)))})"))}

                SELECT a.*
                FROM dbo.{TableName} a
                JOIN @Keys k ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" a.{k.ColumnName} = k.{k.ColumnName}"))}";

            return queryable.FromSql(sql);
        }

        public async Task<int> BulkRemoveAsync(IQueryable<TEntity> queryable, bool filterKeys, List<object[]> keys)
        {
            var alias = $"d_{TableName}";
            var qryAlias = $"q_{TableName}";
            var kAlias = $"k_{TableName}";
            var keysParam = $"@keys_{TableName}";

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var keyTblSql = filterKeys
                ? $@"
                SET NOCOUNT ON

                DECLARE @Keys TABLE ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))})

                INSERT INTO @Keys VALUES {string.Join(",", keys.Select(k => $@"
                    ({string.Join(", ", k.Select(val => StringifyKeyVal(val)))})"))}
                
                SET NOCOUNT OFF"
                : string.Empty;

            var keyJoinSql = filterKeys
                ? $@"JOIN @Keys k ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = k.{k.ColumnName}"))}"
                : string.Empty;

            var sqlCmd = $@"
                {keyTblSql}

                DELETE {alias} FROM {TableName} {alias}
                JOIN (
                    {qrySql}
                ) AS {qryAlias} ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {qryAlias}.{k.ColumnName}"))}
                {keyJoinSql}";

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }

        public async Task BulkAddAsync(List<TEntity> entities)
        {
            var tableVar = "@ToAdd";

            var props = PropertyMappings.Where(p => !p.IsGenerated).ToList();

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
                OUTPUT {string.Join(",", PrimaryKey.Keys.Select(k => $@"
                    inserted.{k.ColumnName}"))}
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
                ) AS {qryAlias} ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {qryAlias}.{k.ColumnName}"))}";

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        public async Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, List<object[]> keys, List<string> updateProperties, Func<object[], TEntity> updateFunc)
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

                return $@"
                    ({string.Join(", ", key.Select(keyVal => $"{StringifyKeyVal(keyVal)}"))}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                DECLARE {tableVar} TABLE ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.ColumnName} {m.Value.ColumnType}"))})

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
                ) AS {qryAlias} ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {qryAlias}.{k.ColumnName}"))}
                JOIN {tableVar} {deltaAlias} ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {deltaAlias}.{k.ColumnName}"))}";

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

            var updateColumnSql = string.Join(",", PropertyMappings.Where(p => !p.IsGenerated).Select(p => $@"
                TARGET.{p.ColumnName} = SOURCE.{p.ColumnName}"));

            var insertColumnSql = string.Join(",", PropertyMappings.Where(p => !p.IsGenerated).Select(p => $@"
                {p.ColumnName}"));

            var insertSourceColumnSql = string.Join(",", PropertyMappings.Where(p => !p.IsGenerated).Select(p => $@"
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
                USING @merge AS SOURCE ON (
                    {string.Join(" AND ", PrimaryKey.Keys.Select(k => $@"TARGET.{k.ColumnName} = SOURCE.{k.ColumnName}"))}
                )
                WHEN MATCHED THEN
                UPDATE SET {updateColumnSql}
                WHEN NOT MATCHED BY TARGET THEN
                INSERT ({insertColumnSql})
                VALUES ({insertSourceColumnSql})
                WHEN NOT MATCHED BY SOURCE THEN
                DELETE
                OUTPUT
                    $action AS action, {string.Join(",", PrimaryKey.Keys.Select(k => $@"
                    inserted.{k.ColumnName}"))};";

            return await ExecuteSqlCommandAsync(sqlCmd, current, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }
    }
}
