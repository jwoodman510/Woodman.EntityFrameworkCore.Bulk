using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore
{
    internal static class StaticFields
    {
        internal static readonly TypeInfo QueryCompilerTypeInfo = typeof(QueryCompiler).GetTypeInfo();

        internal static readonly PropertyInfo DatabaseDependenciesField = typeof(Database).GetTypeInfo().DeclaredProperties.Single(x => x.Name == "Dependencies");
        internal static readonly PropertyInfo QueryCompilationContextFactoryDependenciesField = typeof(QueryCompilationContextFactory).GetTypeInfo().DeclaredProperties.Single(x => x.Name == "Dependencies");
        internal static readonly PropertyInfo NodeTypeProviderField = QueryCompilerTypeInfo.DeclaredProperties.Single(x => x.Name == "NodeTypeProvider");

        internal static readonly FieldInfo QueryCompilerField = typeof(EntityQueryProvider).GetTypeInfo().DeclaredFields.First(x => x.Name == "_queryCompiler");
        internal static readonly FieldInfo DataBaseField = QueryCompilerTypeInfo.DeclaredFields.Single(x => x.Name == "_database");
        internal static readonly FieldInfo QueryContextFactoryField = QueryCompilerTypeInfo.DeclaredFields.Single(x => x.Name == "_queryContextFactory");
        internal static readonly FieldInfo EvaluatableExpressionFilterField = QueryCompilerTypeInfo.DeclaredFields.Single(x => x.Name == "_evaluatableExpressionFilter");

        internal static readonly MethodInfo CreateQueryParserMethod = QueryCompilerTypeInfo.DeclaredMethods.First(x => x.Name == "CreateQueryParser");
    }

    internal static class MergeActions
    {
        internal static string Insert => "INSERT";
        internal static string Update => "UPDATE";
        internal static string Delete => "DELETE";
    }
}
