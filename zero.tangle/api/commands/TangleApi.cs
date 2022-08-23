
// ReSharper disable ObjectCreationAsStatement

namespace zero.tangle.api.commands
{
    public static class TangleApi
    {
        static TangleApi()
        {            
            new getNodeInfo();
            new getNeighbors();
            new addNeighbors();
            new removeNeighbors();
            new getTips();
            new findTransactions();
            new getTrytes();
            new getInclusionStates();
            new getBalances();
            new getTransactionsToApprove();
            new attachToTangle();
            new interruptAttachingToTangle();
            new broadcastTransactions();
            new storeTransactions();
        }
    }
}
