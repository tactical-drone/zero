namespace zero.core.api
{
    public class IoApiReturn
    {
        public static IoApiReturn Result(bool success, string message = null, object rows = null)
        {
            return new IoApiReturn{success = success, message = message, rows = rows};
        }

        public bool success;
        public string message;
        public object rows;
    }
}
