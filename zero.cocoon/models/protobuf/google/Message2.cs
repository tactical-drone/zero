// <auto-generated>
//     Generated by the protocol buffer compiler.  DO NOT EDIT!
//     source: autopeering/discover/proto/message.proto
// </auto-generated>
#pragma warning disable 1591, 0612, 3021
#region Designer generated code

using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;
namespace Proto {

  /// <summary>Holder for reflection information generated from autopeering/discover/proto/message.proto</summary>
  public static partial class Message2Reflection {

    #region Descriptor
    /// <summary>File descriptor for autopeering/discover/proto/message.proto</summary>
    public static pbr::FileDescriptor Descriptor {
      get { return descriptor; }
    }
    private static pbr::FileDescriptor descriptor;

    static Message2Reflection() {
      byte[] descriptorData = global::System.Convert.FromBase64String(
          string.Concat(
            "CihhdXRvcGVlcmluZy9kaXNjb3Zlci9wcm90by9tZXNzYWdlLnByb3RvEgVw",
            "cm90bxohYXV0b3BlZXJpbmcvcGVlci9wcm90by9wZWVyLnByb3RvGixhdXRv",
            "cGVlcmluZy9wZWVyL3NlcnZpY2UvcHJvdG8vc2VydmljZS5wcm90byJ0CgRQ",
            "aW5nEg8KB3ZlcnNpb24YASABKA0SEgoKbmV0d29ya19pZBgCIAEoDRIRCgl0",
            "aW1lc3RhbXAYAyABKAMSEAoIc3JjX2FkZHIYBCABKAkSEAoIc3JjX3BvcnQY",
            "BSABKA0SEAoIZHN0X2FkZHIYBiABKAkiTwoEUG9uZxIQCghyZXFfaGFzaBgB",
            "IAEoDBIjCghzZXJ2aWNlcxgCIAEoCzIRLnByb3RvLlNlcnZpY2VNYXASEAoI",
            "ZHN0X2FkZHIYAyABKAkiJQoQRGlzY292ZXJ5UmVxdWVzdBIRCgl0aW1lc3Rh",
            "bXAYASABKAMiQQoRRGlzY292ZXJ5UmVzcG9uc2USEAoIcmVxX2hhc2gYASAB",
            "KAwSGgoFcGVlcnMYAiADKAsyCy5wcm90by5QZWVyQjpaOGdpdGh1Yi5jb20v",
            "aW90YWxlZGdlci9oaXZlLmdvL2F1dG9wZWVyaW5nL2Rpc2NvdmVyL3Byb3Rv",
            "YgZwcm90bzM="));
      descriptor = pbr::FileDescriptor.FromGeneratedCode(descriptorData,
          new pbr::FileDescriptor[] { global::Proto.PeerReflection.Descriptor, global::Proto.ServiceReflection.Descriptor, },
          new pbr::GeneratedClrTypeInfo(null, new pbr::GeneratedClrTypeInfo[] {
            new pbr::GeneratedClrTypeInfo(typeof(global::Proto.Ping), global::Proto.Ping.Parser, new[]{ "Version", "NetworkId", "Timestamp", "SrcAddr", "SrcPort", "DstAddr" }, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::Proto.Pong), global::Proto.Pong.Parser, new[]{ "ReqHash", "Services", "DstAddr" }, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::Proto.DiscoveryRequest), global::Proto.DiscoveryRequest.Parser, new[]{ "Timestamp" }, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::Proto.DiscoveryResponse), global::Proto.DiscoveryResponse.Parser, new[]{ "ReqHash", "Peers" }, null, null, null)
          }));
    }
    #endregion

  }
  #region Messages
  public sealed partial class Ping : pb::IMessage<Ping> {
    private static readonly pb::MessageParser<Ping> _parser = new pb::MessageParser<Ping>(() => new Ping());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<Ping> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Proto.MessageReflection.Descriptor.MessageTypes[0]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Ping() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Ping(Ping other) : this() {
      version_ = other.version_;
      networkId_ = other.networkId_;
      timestamp_ = other.timestamp_;
      srcAddr_ = other.srcAddr_;
      srcPort_ = other.srcPort_;
      dstAddr_ = other.dstAddr_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Ping Clone() {
      return new Ping(this);
    }

    /// <summary>Field number for the "version" field.</summary>
    public const int VersionFieldNumber = 1;
    private uint version_;
    /// <summary>
    /// version number and network ID to classify the protocol
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public uint Version {
      get { return version_; }
      set {
        version_ = value;
      }
    }

    /// <summary>Field number for the "network_id" field.</summary>
    public const int NetworkIdFieldNumber = 2;
    private uint networkId_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public uint NetworkId {
      get { return networkId_; }
      set {
        networkId_ = value;
      }
    }

    /// <summary>Field number for the "timestamp" field.</summary>
    public const int TimestampFieldNumber = 3;
    private long timestamp_;
    /// <summary>
    /// unix time
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public long Timestamp {
      get { return timestamp_; }
      set {
        timestamp_ = value;
      }
    }

    /// <summary>Field number for the "src_addr" field.</summary>
    public const int SrcAddrFieldNumber = 4;
    private string srcAddr_ = "";
    /// <summary>
    /// endpoint of the sender; port and string form of the return IP address (e.g. "192.0.2.1", "[2001:db8::1]")
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public string SrcAddr {
      get { return srcAddr_; }
      set {
        srcAddr_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "src_port" field.</summary>
    public const int SrcPortFieldNumber = 5;
    private uint srcPort_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public uint SrcPort {
      get { return srcPort_; }
      set {
        srcPort_ = value;
      }
    }

    /// <summary>Field number for the "dst_addr" field.</summary>
    public const int DstAddrFieldNumber = 6;
    private string dstAddr_ = "";
    /// <summary>
    /// string form of receiver's IP
    /// This provides a way to discover the the external address (after NAT).
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public string DstAddr {
      get { return dstAddr_; }
      set {
        dstAddr_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as Ping);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(Ping other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (Version != other.Version) return false;
      if (NetworkId != other.NetworkId) return false;
      if (Timestamp != other.Timestamp) return false;
      if (SrcAddr != other.SrcAddr) return false;
      if (SrcPort != other.SrcPort) return false;
      if (DstAddr != other.DstAddr) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (Version != 0) hash ^= Version.GetHashCode();
      if (NetworkId != 0) hash ^= NetworkId.GetHashCode();
      if (Timestamp != 0L) hash ^= Timestamp.GetHashCode();
      if (SrcAddr.Length != 0) hash ^= SrcAddr.GetHashCode();
      if (SrcPort != 0) hash ^= SrcPort.GetHashCode();
      if (DstAddr.Length != 0) hash ^= DstAddr.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (Version != 0) {
        output.WriteRawTag(8);
        output.WriteUInt32(Version);
      }
      if (NetworkId != 0) {
        output.WriteRawTag(16);
        output.WriteUInt32(NetworkId);
      }
      if (Timestamp != 0L) {
        output.WriteRawTag(24);
        output.WriteInt64(Timestamp);
      }
      if (SrcAddr.Length != 0) {
        output.WriteRawTag(34);
        output.WriteString(SrcAddr);
      }
      if (SrcPort != 0) {
        output.WriteRawTag(40);
        output.WriteUInt32(SrcPort);
      }
      if (DstAddr.Length != 0) {
        output.WriteRawTag(50);
        output.WriteString(DstAddr);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (Version != 0) {
        size += 1 + pb::CodedOutputStream.ComputeUInt32Size(Version);
      }
      if (NetworkId != 0) {
        size += 1 + pb::CodedOutputStream.ComputeUInt32Size(NetworkId);
      }
      if (Timestamp != 0L) {
        size += 1 + pb::CodedOutputStream.ComputeInt64Size(Timestamp);
      }
      if (SrcAddr.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(SrcAddr);
      }
      if (SrcPort != 0) {
        size += 1 + pb::CodedOutputStream.ComputeUInt32Size(SrcPort);
      }
      if (DstAddr.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(DstAddr);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(Ping other) {
      if (other == null) {
        return;
      }
      if (other.Version != 0) {
        Version = other.Version;
      }
      if (other.NetworkId != 0) {
        NetworkId = other.NetworkId;
      }
      if (other.Timestamp != 0L) {
        Timestamp = other.Timestamp;
      }
      if (other.SrcAddr.Length != 0) {
        SrcAddr = other.SrcAddr;
      }
      if (other.SrcPort != 0) {
        SrcPort = other.SrcPort;
      }
      if (other.DstAddr.Length != 0) {
        DstAddr = other.DstAddr;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 8: {
            Version = input.ReadUInt32();
            break;
          }
          case 16: {
            NetworkId = input.ReadUInt32();
            break;
          }
          case 24: {
            Timestamp = input.ReadInt64();
            break;
          }
          case 34: {
            SrcAddr = input.ReadString();
            break;
          }
          case 40: {
            SrcPort = input.ReadUInt32();
            break;
          }
          case 50: {
            DstAddr = input.ReadString();
            break;
          }
        }
      }
    }

  }

  public sealed partial class Pong : pb::IMessage<Pong> {
    private static readonly pb::MessageParser<Pong> _parser = new pb::MessageParser<Pong>(() => new Pong());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<Pong> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Proto.MessageReflection.Descriptor.MessageTypes[1]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Pong() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Pong(Pong other) : this() {
      reqHash_ = other.reqHash_;
      services_ = other.services_ != null ? other.services_.Clone() : null;
      dstAddr_ = other.dstAddr_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Pong Clone() {
      return new Pong(this);
    }

    /// <summary>Field number for the "req_hash" field.</summary>
    public const int ReqHashFieldNumber = 1;
    private pb::ByteString reqHash_ = pb::ByteString.Empty;
    /// <summary>
    /// hash of the ping packet
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pb::ByteString ReqHash {
      get { return reqHash_; }
      set {
        reqHash_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "services" field.</summary>
    public const int ServicesFieldNumber = 2;
    private global::Proto.ServiceMap services_;
    /// <summary>
    /// services supported by the sender
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public global::Proto.ServiceMap Services {
      get { return services_; }
      set {
        services_ = value;
      }
    }

    /// <summary>Field number for the "dst_addr" field.</summary>
    public const int DstAddrFieldNumber = 3;
    private string dstAddr_ = "";
    /// <summary>
    /// string form of receiver's IP
    /// This should mirror the source IP of the Ping's IP packet. It provides a way to discover the the external address (after NAT).
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public string DstAddr {
      get { return dstAddr_; }
      set {
        dstAddr_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as Pong);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(Pong other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (ReqHash != other.ReqHash) return false;
      if (!object.Equals(Services, other.Services)) return false;
      if (DstAddr != other.DstAddr) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (ReqHash.Length != 0) hash ^= ReqHash.GetHashCode();
      if (services_ != null) hash ^= Services.GetHashCode();
      if (DstAddr.Length != 0) hash ^= DstAddr.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (ReqHash.Length != 0) {
        output.WriteRawTag(10);
        output.WriteBytes(ReqHash);
      }
      if (services_ != null) {
        output.WriteRawTag(18);
        output.WriteMessage(Services);
      }
      if (DstAddr.Length != 0) {
        output.WriteRawTag(26);
        output.WriteString(DstAddr);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (ReqHash.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeBytesSize(ReqHash);
      }
      if (services_ != null) {
        size += 1 + pb::CodedOutputStream.ComputeMessageSize(Services);
      }
      if (DstAddr.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(DstAddr);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(Pong other) {
      if (other == null) {
        return;
      }
      if (other.ReqHash.Length != 0) {
        ReqHash = other.ReqHash;
      }
      if (other.services_ != null) {
        if (services_ == null) {
          services_ = new global::Proto.ServiceMap();
        }
        Services.MergeFrom(other.Services);
      }
      if (other.DstAddr.Length != 0) {
        DstAddr = other.DstAddr;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 10: {
            ReqHash = input.ReadBytes();
            break;
          }
          case 18: {
            if (services_ == null) {
              services_ = new global::Proto.ServiceMap();
            }
            input.ReadMessage(services_);
            break;
          }
          case 26: {
            DstAddr = input.ReadString();
            break;
          }
        }
      }
    }

  }

  public sealed partial class DiscoveryRequest : pb::IMessage<DiscoveryRequest> {
    private static readonly pb::MessageParser<DiscoveryRequest> _parser = new pb::MessageParser<DiscoveryRequest>(() => new DiscoveryRequest());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<DiscoveryRequest> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Proto.MessageReflection.Descriptor.MessageTypes[2]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public DiscoveryRequest() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public DiscoveryRequest(DiscoveryRequest other) : this() {
      timestamp_ = other.timestamp_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public DiscoveryRequest Clone() {
      return new DiscoveryRequest(this);
    }

    /// <summary>Field number for the "timestamp" field.</summary>
    public const int TimestampFieldNumber = 1;
    private long timestamp_;
    /// <summary>
    /// unix time
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public long Timestamp {
      get { return timestamp_; }
      set {
        timestamp_ = value;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as DiscoveryRequest);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(DiscoveryRequest other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (Timestamp != other.Timestamp) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (Timestamp != 0L) hash ^= Timestamp.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (Timestamp != 0L) {
        output.WriteRawTag(8);
        output.WriteInt64(Timestamp);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (Timestamp != 0L) {
        size += 1 + pb::CodedOutputStream.ComputeInt64Size(Timestamp);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(DiscoveryRequest other) {
      if (other == null) {
        return;
      }
      if (other.Timestamp != 0L) {
        Timestamp = other.Timestamp;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 8: {
            Timestamp = input.ReadInt64();
            break;
          }
        }
      }
    }

  }

  public sealed partial class DiscoveryResponse : pb::IMessage<DiscoveryResponse> {
    private static readonly pb::MessageParser<DiscoveryResponse> _parser = new pb::MessageParser<DiscoveryResponse>(() => new DiscoveryResponse());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<DiscoveryResponse> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Proto.MessageReflection.Descriptor.MessageTypes[3]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public DiscoveryResponse() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public DiscoveryResponse(DiscoveryResponse other) : this() {
      reqHash_ = other.reqHash_;
      peers_ = other.peers_.Clone();
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public DiscoveryResponse Clone() {
      return new DiscoveryResponse(this);
    }

    /// <summary>Field number for the "req_hash" field.</summary>
    public const int ReqHashFieldNumber = 1;
    private pb::ByteString reqHash_ = pb::ByteString.Empty;
    /// <summary>
    /// hash of the corresponding request
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pb::ByteString ReqHash {
      get { return reqHash_; }
      set {
        reqHash_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "peers" field.</summary>
    public const int PeersFieldNumber = 2;
    private static readonly pb::FieldCodec<global::Proto.Peer> _repeated_peers_codec
        = pb::FieldCodec.ForMessage(18, global::Proto.Peer.Parser);
    private readonly pbc::RepeatedField<global::Proto.Peer> peers_ = new pbc::RepeatedField<global::Proto.Peer>();
    /// <summary>
    /// list of peers
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pbc::RepeatedField<global::Proto.Peer> Peers {
      get { return peers_; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as DiscoveryResponse);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(DiscoveryResponse other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (ReqHash != other.ReqHash) return false;
      if(!peers_.Equals(other.peers_)) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (ReqHash.Length != 0) hash ^= ReqHash.GetHashCode();
      hash ^= peers_.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (ReqHash.Length != 0) {
        output.WriteRawTag(10);
        output.WriteBytes(ReqHash);
      }
      peers_.WriteTo(output, _repeated_peers_codec);
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (ReqHash.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeBytesSize(ReqHash);
      }
      size += peers_.CalculateSize(_repeated_peers_codec);
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(DiscoveryResponse other) {
      if (other == null) {
        return;
      }
      if (other.ReqHash.Length != 0) {
        ReqHash = other.ReqHash;
      }
      peers_.Add(other.peers_);
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 10: {
            ReqHash = input.ReadBytes();
            break;
          }
          case 18: {
            peers_.AddEntriesFrom(input, _repeated_peers_codec);
            break;
          }
        }
      }
    }

  }

  #endregion

}

#endregion Designer generated code
