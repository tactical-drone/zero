namespace zero.cocoon.models.services
{
    /// <summary>
    /// A service description
    /// </summary>
    public class CcService
    {
        public CcRecord CcRecord = new CcRecord();
        public CcService()
        {
            
        }

        public enum Keys
        {
            peering,
            fpc,
            gossip
        }
    }
}
