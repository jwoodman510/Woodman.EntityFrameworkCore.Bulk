using Microsoft.EntityFrameworkCore;
using System;

namespace Woodman.EntityFrameworkCore.Bulk.Extensions
{
    internal static class DbContextExtensions
    {
        internal static EntityInfoBase GetEntityInfo<TEntity>(this DbContext dbContext) where TEntity : class
        {
            EntityInfoBase entityInfo = null;

            if(dbContext.Database.IsSqlServer())
            {
                entityInfo = new SqlEntityInfo(dbContext.Model.FindEntityType(typeof(TEntity)));
            }
            else if (dbContext.Database.IsNpgsql())
            {
                entityInfo = new NpgSqlEntityInfo(dbContext.Model.FindEntityType(typeof(TEntity)));
            }
            else if (dbContext.Database.IsInMemory())
            {
                entityInfo = new InMemEntityInfo(dbContext.Model.FindEntityType(typeof(TEntity)));
            }

            if(entityInfo == null)
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }

            if (!entityInfo.HasPrimaryKey)
            {
                throw new Exception($"Unable to determine Primary Key Name for {entityInfo.EntityType.Name}");
            }

            return entityInfo;
        }
    }
}
