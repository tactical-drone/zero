using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace zero.test.xunit
{
    /// <summary>
    /// TODO, this does not work
    /// </summary>
    public class AlphabeticalOrderer : ITestCaseOrderer
    {
        public IEnumerable<TTestCase> OrderTestCases<TTestCase>(IEnumerable<TTestCase> testCases)
            where TTestCase : ITestCase =>testCases.OrderByDescending(testCase => testCase.TestMethod.Method.Name);
    }
}