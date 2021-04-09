using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Woodman.EntityFrameworkCore.Bulk
{
    public static class DbContextExtensions
    {
        public static void ApplyConfigurations<T>(this T dbContext, ModelBuilder modelBuilder) where T : DbContext
        {
            dbContext.ApplyConfigurations(modelBuilder, typeof(T).Assembly);
        }

        public static void ApplyConfigurations<T>(this T dbContext, ModelBuilder modelBuilder, params Assembly[] assembliesToScan) where T : DbContext
        {
            var entityTypes = dbContext.GetEntityTypes();

            var applyMethod = typeof(ModelBuilder)
                .GetMethods()
                .Where(x => x.Name == nameof(ModelBuilder.ApplyConfiguration))
                .Where(x => x.IsGenericMethodDefinition)
                .Where(x => x.GetParameters()[0].ParameterType.ImplementsIEntityTypeConfiguration())
                .Single();

            foreach (var assembly in assembliesToScan)
            {
                foreach (var configType in GetEntityConfigurationTypes(assembly, entityTypes))
                {
                    var config = Activator.CreateInstance(configType.ImplementationType);
                    var entityType = configType.ServiceType.GenericTypeArguments[0];

                    applyMethod.MakeGenericMethod(entityType).Invoke(modelBuilder, new[] { config });
                }
            }
        }

        public static HashSet<Type> GetEntityTypes<T>(this T dbContext) where T : DbContext
        {
            return new HashSet<Type>(typeof(T)
                .GetProperties()
                .Select(x => x.PropertyType)
                .Where(x => x.IsGenericType)
                .Where(x => x.GetGenericTypeDefinition() == typeof(DbSet<>))
                .Select(x => x.GenericTypeArguments[0]));
        }

        private static IServiceCollection GetEntityConfigurationTypes(Assembly assembly, HashSet<Type> entityTypes)
        {
            var sc = new ServiceCollection();

            var entityTypeConfigurationTypes = assembly
                .GetExportedTypes()
                .Where(x => x.IsClass)
                .Where(x => !x.IsAbstract)
                .Select(x => (Type: x, Interfaces: x.GetInterfaces()))
                .Where(x => x.Interfaces.Any(i =>
                               i.ImplementsIEntityTypeConfiguration() &&
                               entityTypes.Contains(i.GenericTypeArguments[0])))
                .Select(x => x.Type);

            foreach (var type in entityTypeConfigurationTypes)
            {
                var interfaces = type.GetInterfaces().Where(i =>
                                i.ImplementsIEntityTypeConfiguration() &&
                                entityTypes.Contains(i.GenericTypeArguments[0]));

                foreach (var iType in interfaces)
                {
                    sc.AddTransient(iType, type);
                }
            }

            return sc;
        }

        private static bool ImplementsIEntityTypeConfiguration(this Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEntityTypeConfiguration<>);
        }
    }
}
