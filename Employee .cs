using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DummyData;

public class Employee
{
    public int EmployeeId { get; set; }
    public string FirstName { get; set; } = default!;
    public string LastName { get; set; } = default!;
    public string Email { get; set; } = default!;
    public string Position { get; set; } = default!;
    public int Age { get; set; }
    public List<Company> Companies { get; } = new();
}