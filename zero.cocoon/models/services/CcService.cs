namespace zero.cocoon.models.services;

/// <summary>
///     A service description
/// </summary>
public class CcService
{
    public enum Keys
    {
        Peering,
        Fpc,
        Gossip
    }

    public CcRecord CcRecord = new();
}