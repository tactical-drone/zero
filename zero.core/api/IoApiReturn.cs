namespace zero.core.api
{
    public class IoApiReturn
    {
        public static IoApiReturn Result(bool success, string message = null)
        {
            return new IoApiReturn{success = success, message = message};
        }

        public bool success;
        public string message;
    }
}
