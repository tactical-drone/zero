﻿namespace Tangle.Net.Mam.Services
{
  using Tangle.Net.Entity;
  using Tangle.Net.Mam.Entity;
  using Tangle.Net.Mam.Merkle;

  /// <summary>
  /// The MaskedAuthenticatedMessageFactory interface.
  /// </summary>
  public interface IMamFactory
  {
    /// <summary>
    /// The create.
    /// </summary>
    /// <param name="tree">
    /// The tree.
    /// </param>
    /// <param name="index">
    /// The index.
    /// </param>
    /// <param name="message">
    /// The message.
    /// </param>
    /// <param name="nextRoot">
    /// The next Root.
    /// </param>
    /// <param name="channelKey">
    /// The channel Key.
    /// </param>
    /// <param name="mode">
    /// The mode.
    /// </param>
    /// <param name="securityLevel">
    /// The security Level.
    /// </param>
    /// <returns>
    /// The <see cref="MaskedAuthenticatedMessage"/>.
    /// </returns>
    MaskedAuthenticatedMessage Create(MerkleTree tree, int index, TryteString message, Hash nextRoot, TryteString channelKey, Mode mode, int securityLevel);
  }
}