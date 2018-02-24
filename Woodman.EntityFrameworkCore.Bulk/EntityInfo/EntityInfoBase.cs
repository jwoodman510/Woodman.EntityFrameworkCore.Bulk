using Microsoft.EntityFrameworkCore.Metadata;

namespace Woodman.EntityFrameworkCore.Bulk.EntityInfo
{
    internal class EntityInfoBase
    {
        public bool HasPrimaryKey => EntityType?.FindPrimaryKey() != null;

        public IEntityType EntityType { get; }

        public EntityInfoBase(IEntityType entityType)
        {
            EntityType = entityType;
        }
    }
}
