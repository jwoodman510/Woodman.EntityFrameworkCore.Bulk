using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Woodman.EntityFrameworkCore.Bulk;
using Woodman.EntityFrameworkCore.Bulk.EntityInfo;
using Woodman.EntityFrameworkCore.Bulk.Extensions;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkMerge
    {
        public static async Task<int> BulkMergeAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TEntity> entities) where TEntity : class
        {
            var current = entities?.ToList() ?? new List<TEntity>();

            if (current.Count == 0)
            {
                return 0;
            }

            var dbContext = queryable.GetDbContext();
            var entityInfo = dbContext.GetEntityInfo<TEntity>();

            if (entityInfo is InMemEntityInfo inMemEntityInfo)
            {
                return await queryable.BulkMergeAsync(current, dbContext, inMemEntityInfo);
            }
            else if (entityInfo is NpgSqlEntityInfo npgSqlEntityInfo)
            {
                return await queryable.BulkMergeAsync(current, dbContext, npgSqlEntityInfo);
            }
            else if (entityInfo is SqlEntityInfo sqlEntityInfo)
            {
                return await queryable.BulkMergeAsync(current, dbContext, sqlEntityInfo);
            }
            else
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }
        }

        private static async Task<int> BulkMergeAsync<TEntity>(this IQueryable<TEntity> queryable, List<TEntity> current, DbContext dbContext, InMemEntityInfo entityInfo)
            where TEntity : class
        {
            var previous = await queryable.ToListAsync();

            var toDelete = previous
                .Where(p => current.All(c => !entityInfo.PrimaryKeyEquals(c, p)))
                .ToList();

            var toAdd = current
                .Where(c => previous.All(p => !entityInfo.PrimaryKeyEquals(c, p)))
                .ToList();

            var toUpdate = previous
                .Where(p => current.Any(c => entityInfo.PrimaryKeyEquals(c, p)))
                .ToList();

            dbContext.RemoveRange(toDelete);

            await dbContext.AddRangeAsync(toAdd);

            foreach (var entity in toUpdate)
            {
                var updatedEntity = current.First(c => entityInfo.PrimaryKeyEquals(c, entity));

                foreach (var property in entityInfo.Properties)
                {
                    property.PropertyInfo.SetValue(entity, property.PropertyInfo.GetValue(updatedEntity));
                }
            }

            return await dbContext.SaveChangesAsync();
        }

        private static async Task<int> BulkMergeAsync<TEntity>(this IQueryable<TEntity> queryable, List<TEntity> current, DbContext dbContext, SqlEntityInfo entityInfo)
            where TEntity : class
        {
            var deltaSql = string.Join(",", current.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", entityInfo.PropertyMappings.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var updateColumnSql = string.Join(",", entityInfo.PropertyMappings.Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true).Select(p => $@"
                TARGET.{p.ColumnName} = SOURCE.{p.ColumnName}"));

            var insertColumnSql = string.Join(",", entityInfo.PropertyMappings.Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true).Select(p => $@"
                {p.ColumnName}"));

            var insertSourceColumnSql = string.Join(",", entityInfo.PropertyMappings.Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true).Select(p => $@"
                SOURCE.{p.ColumnName}"));

            var sqlCmd = $@"
                DECLARE @merge TABLE ({string.Join(", ", entityInfo.PropertyMappings.Select(m => $"{m.ColumnName} {m.ColumnType}"))})

                SET NOCOUNT ON

                INSERT INTO @merge VALUES
                    {deltaSql}

                SET NOCOUNT OFF

                ;
                WITH tgt_cte AS (
                    {qrySql}
                )
                MERGE tgt_cte AS TARGET
                USING @merge AS SOURCE ON (TARGET.{entityInfo.PrimaryKeyColumnName} = SOURCE.{entityInfo.PrimaryKeyColumnName})
                WHEN MATCHED THEN
                UPDATE SET {updateColumnSql}
                WHEN NOT MATCHED BY TARGET THEN
                INSERT ({insertColumnSql})
                VALUES ({insertSourceColumnSql})
                WHEN NOT MATCHED BY SOURCE THEN
                DELETE
                OUTPUT inserted.{entityInfo.PrimaryKeyColumnName}, $action;";

            return await ExecuteSqlCmdAsync(current, dbContext, entityInfo, sqlCmd, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }

        private static async Task<int> BulkMergeAsync<TEntity>(this IQueryable<TEntity> queryable, List<TEntity> current, DbContext dbContext, NpgSqlEntityInfo entityInfo)
            where TEntity : class
        {
            var tableVar = $"_m_{typeof(TEntity).Name}";

            var deltaSql = string.Join(",", current.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", entityInfo.PropertyMappings.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var insertProps = entityInfo.PropertyMappings
                .Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var updateProperties = entityInfo.PropertyMappings.Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true);

            var insertColumnSql = $@"{string.Join(",", insertProps.Select(p => $@"
                    {p.ColumnName}"))}";

            var insertSelectColumnSql = $@"{string.Join(",", insertProps.Select(p => $@"
                    i_m.{p.ColumnName}"))}";

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({string.Join(", ", entityInfo.PropertyMappings.Select(m => $"{m.ColumnName} {m.ColumnType}"))});
                CREATE TEMP TABLE _m_results ({entityInfo.PrimaryKeyColumnName} {entityInfo.PrimaryKeyColumnType}, action varchar(10)) ;

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                WITH _out AS
                (
                    DELETE FROM {entityInfo.TableName} d
                    USING (
                        {qrySql}
                    ) AS d_q
                    LEFT JOIN {tableVar} AS d_m ON d_q.{entityInfo.PrimaryKeyColumnName} = d_m.{entityInfo.PrimaryKeyColumnName}
                    WHERE d_q.{entityInfo.PrimaryKeyColumnName} = d.{entityInfo.PrimaryKeyColumnName}
                    AND d_m.{entityInfo.PrimaryKeyColumnName} IS NULL
                    RETURNING 0 AS id, '{MergeActions.Delete}' AS action
                )
                INSERT INTO _m_results SELECT id, action FROM _out;
                
                WITH _out AS
                (
                    UPDATE {entityInfo.TableName} AS u
                    SET
                        {string.Join(",", updateProperties.Select(prop => $@"
                        {prop.ColumnName} = u_m.{prop.ColumnName}"))}
                    FROM (
                        {qrySql}
                    ) as u_q
                    JOIN {tableVar} u_m ON u_m.{entityInfo.PrimaryKeyColumnName} = u_q.{entityInfo.PrimaryKeyColumnName}
                    WHERE u_q.{entityInfo.PrimaryKeyColumnName} = u.{entityInfo.PrimaryKeyColumnName}
                    RETURNING 0 as id, '{MergeActions.Update}' as action
                )
                INSERT INTO _m_results SELECT id, action FROM _out;

                WITH _out AS
                (
                    INSERT INTO {entityInfo.TableName}
                    ({insertColumnSql})
                    SELECT
                    {insertSelectColumnSql}
                    FROM {tableVar} i_m
                    LEFT JOIN {entityInfo.TableName} q_m ON q_m.{entityInfo.PrimaryKeyColumnName} = i_m.{entityInfo.PrimaryKeyColumnName}
                    WHERE q_m.{entityInfo.PrimaryKeyColumnName} IS NULL
                    RETURNING {entityInfo.TableName}.id as id, '{MergeActions.Insert}' as action
                )
                INSERT INTO _m_results SELECT id, action FROM _out;

                SELECT id, action FROM _m_results;";

            return await ExecuteSqlCmdAsync(current, dbContext, entityInfo, sqlCmd, parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));
        }

        private static async Task<int> ExecuteSqlCmdAsync<TEntity>(List<TEntity> current, DbContext dbContext, EntityInfoBase entityInfo, string sqlCmd, IEnumerable<DbParameter> cmdParams)
            where TEntity : class
        {
            var conn = dbContext.Database.GetDbConnection();

            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            var numRecordsAffected = 0;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sqlCmd;
                cmd.CommandType = CommandType.Text;

                cmd.Parameters.AddRange(cmdParams.ToArray());

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var action = reader.GetValue(1).ToString();

                        if (entityInfo.IsPrimaryKeyGenerated && action == MergeActions.Insert)
                        {
                            entityInfo.SetPrimaryKey(current.First(c => entityInfo.IsPrimaryKeyUnset(c)), reader.GetValue(0));
                        }

                        numRecordsAffected++;
                    }
                }
            }

            return numRecordsAffected;
        }
    }
}
