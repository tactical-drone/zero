namespace zero.cocoon.models.services
{
    /// <summary>
    /// A service description
    /// </summary>
    public class CcService
    {
        public CcRecord CcRecord = new ();

        public enum Keys
        {
            Peering,
            Fpc,
            Gossip
        }
    }
}
