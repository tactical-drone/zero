namespace zero.core.feat.api;

public class IoApiReturn //: IActionResult
{
    public string Message;
    public object Rows;

    public bool Success;
    public long Time;

    public static IoApiReturn Result(bool success, string message = null, object rows = null, long time = 0)
    {
        return new IoApiReturn { Success = success, Time = time, Message = message, Rows = rows };
    }
}