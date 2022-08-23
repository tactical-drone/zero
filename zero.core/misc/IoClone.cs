namespace zero.core.misc
{
    /// <summary>
    /// Clones
    /// </summary>
    public static class IoClone
    {
        //Clone a struct
        public static T Clone<T>(this T source) where T:struct => source;
    }
}