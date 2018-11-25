namespace zero.core.api
{
    public class IoApiReturn
    {
        public static IoApiReturn Result(bool success, string message = null, object data = null)
        {
            return new IoApiReturn{success = success, message = message, data = data};
        }

        public bool success;
        public string message;
        public object data;
    }
}
