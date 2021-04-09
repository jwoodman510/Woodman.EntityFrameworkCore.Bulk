using System.Reflection;

namespace Microsoft.EntityFrameworkCore
{
    internal static class ObjectExtensions
    {
        internal static T Get<T>(this object obj, FieldInfo fieldInfo) where T : class
        {
            return (T)fieldInfo.GetValue(obj);
        }

        internal static T Get<T>(this object obj, PropertyInfo propertyInfo) where T : class
        {
            return (T)propertyInfo.GetValue(obj);
        }

        internal static object Get(this object obj, FieldInfo fieldInfo)
        {
            return fieldInfo.GetValue(obj);
        }

        internal static object Get(this object obj, PropertyInfo propertyInfo)
        {
            return propertyInfo.GetValue(obj);
        }
    }
}
