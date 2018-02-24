using Microsoft.EntityFrameworkCore;
using System;
using Woodman.EntityFrameworkCore.Bulk.EntityInfo;

namespace Woodman.EntityFrameworkCore.Bulk.Extensions
{
    internal static class DbContextExtensions
    {
        internal static EntityInfoBase GetEntityInfo<TEntity>(this DbContext dbContext) where TEntity : class
        {
            var entityInfo = dbContext.Database.IsSqlServer()
                ? new SqlEntityInfo(dbContext.Model.FindEntityType(typeof(TEntity)))
                : dbContext.Database.IsNpgsql()
                    ? new NpgSqlEntityInfo(dbContext.Model.FindEntityType(typeof(TEntity)))
                    : dbContext.Database.IsInMemory()
                        ? new InMemEntityInfo(dbContext.Model.FindEntityType(typeof(TEntity)))
                        : new EntityInfoBase(dbContext.Model.FindEntityType(typeof(TEntity)));

            if (!entityInfo.HasPrimaryKey)
            {
                throw new Exception($"Unable to determine Primary Key Name for {entityInfo.EntityType.Name}");
            }

            return entityInfo;
        }
    }
}
