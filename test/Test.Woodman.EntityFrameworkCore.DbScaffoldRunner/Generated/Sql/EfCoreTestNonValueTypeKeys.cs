using System;
using System.Collections.Generic;
using System.Text;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql
{
    public class EfCoreTestNonValueTypeKeys
    {
        public int Id { get; set; }
        public string Tier1Id { get; set; }
        public string Tier2Id { get; set; }
        public string Name { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime ModifiedDate { get; set; }

        public bool IsArchived { get; set; }
    }
}
