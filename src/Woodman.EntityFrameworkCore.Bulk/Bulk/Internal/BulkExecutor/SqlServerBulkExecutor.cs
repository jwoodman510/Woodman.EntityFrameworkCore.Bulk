using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal class SqlServerBulkExecutor<TEntity> : RelationalBulkExecutor<TEntity>, IBulkExecutor<TEntity> where TEntity : class
    {
        public SqlServerBulkExecutor(DbContext dbContext) : base(dbContext)
        {

        }

        public IQueryable<TEntity> Join(DbSet<TEntity> rootQuery, List<object[]> keys, char delimiter)
        {
            ValidateCompositeKeys(keys);

            return rootQuery.FromSqlRaw($@"
                DECLARE @Keys TABLE ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))})

                { CreateBatchInsertStatement("INSERT INTO @Keys VALUES", keys) }

                SELECT a.*
                FROM dbo.{TableName} a
                JOIN @Keys k ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" a.{k.ColumnName} = k.{k.ColumnName}"))}");
        }

        public async Task<int> BulkRemoveAsync(IQueryable<TEntity> queryable, bool filterKeys, List<object[]> keys)
        {
            ValidateCompositeKeys(keys);

            var alias = GetAlias(queryable);

            var qryAlias = $"q_{TableName}";
            var kAlias = $"k_{TableName}";
            var keysParam = $"@keys_{TableName}";

            var qrySql = queryable
                .Select(x => 1)
                .ToSql(out IReadOnlyList<SqlParameter> parameters)
                .Substring(8)
                .TrimStart();

            var keyTblSql = filterKeys
                ? $@"
                SET NOCOUNT ON

                DECLARE @Keys TABLE ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))})

                { CreateBatchInsertStatement("INSERT INTO @Keys VALUES", keys) }

                SET NOCOUNT OFF"
                : string.Empty;

            var keyJoinSql = filterKeys
                ? $@"JOIN @Keys k ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = k.{k.ColumnName}"))}"
                : string.Empty;

            if (filterKeys)
            {
                var fromClause = Regex.Match(qrySql, $"^FROM .* \\{alias}").Value;
                var whereClause = Regex.Replace(qrySql, $"^FROM .* \\{alias}", string.Empty);

                qrySql = $@"
                {fromClause}
                {keyJoinSql}
                {whereClause}";
            }

            var sqlCmd = $@"
                {keyTblSql}

                DELETE {alias}
                {qrySql}";

            return await DbContext.Database.ExecuteSqlRawAsync(sqlCmd, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }

        public async Task BulkAddAsync(List<TEntity> entities)
        {
            var tableVar = "@ToAdd";

            var props = PropertyMappings.Where(p => !p.IsGenerated).ToList();

            var columnsSql = $@"{string.Join(",", props.Select(p => $@"
                    {p.ColumnName}"))}";

            var records = entities.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", props.Select(m => $"{m.GetDbValue(entity)}"))})";
            });

            var sqlCmd = $@"
                DECLARE {tableVar} TABLE ({string.Join(", ", props.Select(m => $"{m.ColumnName} {m.ColumnType}"))})

                SET NOCOUNT ON

                { CreateBatchInsertStatement($"INSERT INTO {tableVar} VALUES", records) }

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
            var alias = GetAlias(queryable);

            var qrySql = queryable
                .Select(x => 1)
                .ToSql(out IReadOnlyList<SqlParameter> parameters)
                .Substring(8)
                .TrimStart();

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
                {qrySql}";

            return await DbContext.Database.ExecuteSqlRawAsync(sqlCmd, sqlParams);
        }

        public async Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, List<object[]> keys, List<string> updateProperties, Func<object[], TEntity> updateFunc)
        {
            ValidateCompositeKeys(keys);

            var alias = GetAlias(queryable);

            var qryAlias = $"q_{TableName}";
            var deltaAlias = $"d_{TableName}";
            var tableVar = "@Delta";

            var qrySql = queryable
                .Select(x => 1)
                .ToSql(out IReadOnlyList<SqlParameter> parameters)
                .Substring(8)
                .TrimStart();

            var fromClause = Regex.Match(qrySql, $"^FROM .* \\{alias}").Value;
            var whereClause = Regex.Replace(qrySql, $"^FROM .* \\{alias}", string.Empty);

            qrySql = $@"
            {fromClause}
            JOIN {tableVar} {deltaAlias} ON {string.Join(@"
                AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {deltaAlias}.{k.ColumnName}"))}
            {whereClause}";

            var propertyMappings = PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .ToDictionary(x => x.PropertyName);

            var records = keys.Select(key =>
            {
                var entity = updateFunc(key);

                return $@"
                    ({string.Join(", ", key.Select(keyVal => $"{StringifyKeyVal(keyVal)}"))}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.GetDbValue(entity)}"))})";
            });

            var sqlCmd = $@"
                DECLARE {tableVar} TABLE ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.ColumnName} {m.Value.ColumnType}"))})

                SET NOCOUNT ON

                { CreateBatchInsertStatement($"INSERT INTO {tableVar} VALUES", records) }

                SET NOCOUNT OFF

                UPDATE {alias}
                SET
                    {string.Join(",", updateProperties.Select(prop => $@"
                    {alias}.{propertyMappings[prop].ColumnName} = {deltaAlias}.{propertyMappings[prop].ColumnName}"))}
                {qrySql}";

            return await DbContext.Database.ExecuteSqlRawAsync(sqlCmd, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }

        public async Task<int> BulkMergeAsync(
            IQueryable<TEntity> queryable,
            List<TEntity> current,
            Func<TEntity, object> updateColumnsToExclude = null,
            BulkMergeNotMatchedBehavior notMatchedBehavior = BulkMergeNotMatchedBehavior.DoNothing,
            Expression<Func<TEntity>> whenNotMatched = null)
        {
            var records = current.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", PropertyMappings.Select(m => $"{m.GetDbValue(entity)}"))})";
            });

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);
            Func<RelationalPropertyMapping, bool> updatePropertyPredicate = p => !p.IsGenerated;
            string updateColumnSql = string.Empty;
            var defaultConstructor = typeof(TEntity).GetConstructor(Type.EmptyTypes);
            if (defaultConstructor != null && updateColumnsToExclude != null)
            {
                var instance = Activator.CreateInstance<TEntity>();
                var properties = updateColumnsToExclude(instance).GetType().GetProperties().Select(x => x.Name).ToList();
                updatePropertyPredicate = p => !p.IsGenerated && !properties.Contains(p.PropertyName);
            }

            updateColumnSql = string.Join(",", PropertyMappings.Where(updatePropertyPredicate).Select(p => $@"
                TARGET.{p.ColumnName} = SOURCE.{p.ColumnName}"));

            var insertColumnSql = string.Join(",", PropertyMappings.Where(p => !p.IsGenerated).Select(p => $@"
                {p.ColumnName}"));

            var insertSourceColumnSql = string.Join(",", PropertyMappings.Where(p => !p.IsGenerated).Select(p => $@"
                SOURCE.{p.ColumnName}"));

            var notMatchedSelector = string.Empty;
            var notMatchedAction = string.Empty;

            switch (notMatchedBehavior)
            {
                case BulkMergeNotMatchedBehavior.Delete:
                    notMatchedAction = "DELETE";
                    notMatchedSelector = "WHEN NOT MATCHED BY SOURCE THEN";
                    break;
                case BulkMergeNotMatchedBehavior.Update when whenNotMatched != null:
                    var notMatchedVals = whenNotMatched.Compile().Invoke();
                    var propNames = whenNotMatched.GetSetPropertyNames();

                    if (propNames.Count == 0)
                    {
                        break;
                    }

                    var propertyMappings = PropertyMappings.Where(x => !x.IsGenerated && propNames.Contains(x.PropertyName));

                    var notMatchedUpdateColumnSql = string.Join(",", propertyMappings.Select(p => $@"
                        TARGET.{p.ColumnName} = {p.GetDbValue(notMatchedVals)}"));

                    notMatchedAction = $"UPDATE SET {notMatchedUpdateColumnSql}";
                    notMatchedSelector = "WHEN NOT MATCHED BY SOURCE THEN";
                    break;
            }

            var sqlCmd = $@"
                DECLARE @merge TABLE ({string.Join(", ", PropertyMappings.Select(m => $"{m.ColumnName} {m.ColumnType}"))})

                SET NOCOUNT ON

                { CreateBatchInsertStatement($"INSERT INTO @merge VALUES", records) }

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
                {notMatchedSelector}
                {notMatchedAction}
                OUTPUT
                    $action AS action, {string.Join(",", PrimaryKey.Keys.Select(k => $@"
                    inserted.{k.ColumnName}"))};";

            return await ExecuteSqlCommandAsync(sqlCmd, current, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }

        private static string GetAlias(IQueryable<TEntity> queryable) => Regex.Match(queryable.ToSql().Split('.').First(), @"\[.*\]$").Value;
    }
}