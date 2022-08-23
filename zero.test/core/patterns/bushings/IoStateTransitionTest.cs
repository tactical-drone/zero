using System;
using Xunit;
using zero.core.patterns.bushings;

namespace zero.test.core.patterns.bushings
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "<Pending>")]
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
        private void EventChain()
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
