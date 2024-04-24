using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DummyData
{
    public class Company
    {
        public Guid CompanyId { get; set; }
        public string CompanyName { get; set; } = default!;
        public string Business { get; set; } = default!;

        public List<Employee> Employees { get; } = new();
    }
}