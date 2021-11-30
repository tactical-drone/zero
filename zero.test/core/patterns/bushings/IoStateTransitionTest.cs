using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using zero.core.patterns.bushings;

namespace zero.test.core.patterns.bushings
{
    public class IoStateTransitionTest
    {
        private readonly int _count;

        enum TestStates
        {
            State0,
            State1,
            State2,
            State3,
            State4,
            State5,
            State6,
            State7,
            State8,
            State9,
            State10,
            State11,
            State12,
            State13,
            State14,
        }

        public IoStateTransitionTest()
        {
            _count = Enum.GetNames(typeof(TestStates)).Length;
        }

        [Fact]
        void EventChain()
        {
            var state = new IoStateTransition<TestStates>((int)TestStates.State1);

            for (int i = 1; i < _count; i++)
            {
                state.CompareAndEnterState(i, i - 1);
                Assert.Equal(i,(int)state.Value);
            }
        }
    }
}
