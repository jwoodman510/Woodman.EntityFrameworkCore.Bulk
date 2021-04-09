using System;
using System.Collections.Generic;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql
{
    public partial class EfCoreTest
    {
        public EfCoreTest()
        {
            EfCoreTestChild = new HashSet<EfCoreTestChild>();
        }

        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime ModifiedDate { get; set; }

        public ICollection<EfCoreTestChild> EfCoreTestChild { get; set; }
    }
}
