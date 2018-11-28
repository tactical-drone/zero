namespace zero.core.api
{
    public class IoApiReturn
    {
        public static IoApiReturn Result(bool success, string message = null, object rows = null)
        {
            return new IoApiReturn{Success = success, Message = message, Rows = rows};
        }

        public bool Success;
        public string Message;
        public object Rows;
    }
}
