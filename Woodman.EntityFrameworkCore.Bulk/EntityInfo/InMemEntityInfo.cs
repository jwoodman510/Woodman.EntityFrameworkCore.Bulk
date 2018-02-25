using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;
using System.Linq;

namespace Woodman.EntityFrameworkCore.Bulk.EntityInfo
{
    internal class InMemEntityInfo : EntityInfoBase
    {
        public List<IProperty> Properties => EntityType?.GetProperties()?.ToList();

        public InMemEntityInfo(IEntityType entityType)
            : base(entityType)
        {

        }
    }
}
