using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Woodman.EntityFrameworkCore.Bulk.Extensions;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkAdd
    {
        public static async Task BulkAddAsync<TEntity>(this DbSet<TEntity> queryable, IEnumerable<TEntity> entities)
            where TEntity : class
        {
            var toAdd = entities?.ToList() ?? new List<TEntity>();

            if (toAdd.Count == 0)
            {
                return;
            }

            var dbContext = queryable.GetDbContext();

            await dbContext
                .GetEntityInfo<TEntity>()
                .BulkAddAsync(toAdd, dbContext);
        }

        internal static async Task BulkAddAsync<TEntity>(IEnumerable<TEntity> entities, DbContext dbContext, InMemEntityInfo entityInfo)
            where TEntity : class
        {
            await dbContext.AddRangeAsync(entities);

            await dbContext.SaveChangesAsync();
        }

        internal static async Task BulkAddAsync<TEntity>(List<TEntity> toAdd, DbContext dbContext, NpgSqlEntityInfo entityInfo)
            where TEntity : class
        {
            var tableVar = $"_ToAdd_{typeof(TEntity).Name}";

            var props = entityInfo.PropertyMappings
                .Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var columnsSql = $@"{string.Join(",", props.Select(p => $@"
                    {p.ColumnName}"))}";

            var deltaSql = string.Join(",", toAdd.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", props.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({string.Join(", ", props.Select(m => $"{m.ColumnName} {m.ColumnType}"))});

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                INSERT INTO {entityInfo.TableName}
                ({columnsSql})
                SELECT
                {columnsSql}
                FROM {tableVar}
                RETURNING {entityInfo.TableName}.id";

            await ExecuteDbCommandAsync(dbContext, sqlCmd, toAdd, entityInfo);
        }

        internal static async Task BulkAddAsync<TEntity>(List<TEntity> toAdd, DbContext dbContext, SqlEntityInfo entityInfo)
            where TEntity : class
        {
            var tableVar = "@ToAdd";

            var props = entityInfo.PropertyMappings
                .Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var columnsSql = $@"{string.Join(",", props.Select(p => $@"
                    {p.ColumnName}"))}";

            var deltaSql = string.Join(",", toAdd.Select(entity =>
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

                INSERT INTO dbo.{entityInfo.TableName}
                ({columnsSql})
                OUTPUT Inserted.{entityInfo.PrimaryKeyColumnName}
                SELECT
                {columnsSql}
                FROM {tableVar}";

            await ExecuteDbCommandAsync(dbContext, sqlCmd, toAdd, entityInfo);
        }

        internal static async Task ExecuteDbCommandAsync<TEntity>(DbContext dbContext, string sqlCmd, List<TEntity> entities, EntityInfoBase entityInfo)
        {
            var conn = dbContext.Database.GetDbConnection();

            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sqlCmd;
                cmd.CommandType = CommandType.Text;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    var index = 0;

                    while(await reader.ReadAsync())
                    {
                        entityInfo.SetPrimaryKey(entities[index], reader[0]);

                        index++;
                    }
                }
            }
        }
    }
}
