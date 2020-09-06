// <auto-generated>
//     Generated by the protocol buffer compiler.  DO NOT EDIT!
//     source: autopeering/peer/service/proto/service.proto
// </auto-generated>
#pragma warning disable 1591, 0612, 3021
#region Designer generated code

using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;
namespace Proto {

  /// <summary>Holder for reflection information generated from autopeering/peer/service/proto/service.proto</summary>
  public static partial class ServiceReflection {

    #region Descriptor
    /// <summary>File descriptor for autopeering/peer/service/proto/service.proto</summary>
    public static pbr::FileDescriptor Descriptor {
      get { return descriptor; }
    }
    private static pbr::FileDescriptor descriptor;

    static ServiceReflection() {
      byte[] descriptorData = global::System.Convert.FromBase64String(
          string.Concat(
            "CixhdXRvcGVlcmluZy9wZWVyL3NlcnZpY2UvcHJvdG8vc2VydmljZS5wcm90",
            "bxIFcHJvdG8ieAoKU2VydmljZU1hcBInCgNtYXAYASADKAsyGi5wcm90by5T",
            "ZXJ2aWNlTWFwLk1hcEVudHJ5GkEKCE1hcEVudHJ5EgsKA2tleRgBIAEoCRIk",
            "CgV2YWx1ZRgCIAEoCzIVLnByb3RvLk5ldHdvcmtBZGRyZXNzOgI4ASIvCg5O",
            "ZXR3b3JrQWRkcmVzcxIPCgduZXR3b3JrGAEgASgJEgwKBHBvcnQYAiABKA1C",
            "Plo8Z2l0aHViLmNvbS9pb3RhbGVkZ2VyL2hpdmUuZ28vYXV0b3BlZXJpbmcv",
            "cGVlci9zZXJ2aWNlL3Byb3RvYgZwcm90bzM="));
      descriptor = pbr::FileDescriptor.FromGeneratedCode(descriptorData,
          new pbr::FileDescriptor[] { },
          new pbr::GeneratedClrTypeInfo(null, new pbr::GeneratedClrTypeInfo[] {
            new pbr::GeneratedClrTypeInfo(typeof(global::Proto.ServiceMap), global::Proto.ServiceMap.Parser, new[]{ "Map" }, null, null, new pbr::GeneratedClrTypeInfo[] { null, }),
            new pbr::GeneratedClrTypeInfo(typeof(global::Proto.NetworkAddress), global::Proto.NetworkAddress.Parser, new[]{ "Network", "Port" }, null, null, null)
          }));
    }
    #endregion

  }
  #region Messages
  /// <summary>
  /// Mapping between a service ID and its tuple network_address
  /// e.g., map[autopeering:&amp;{tcp, 198.51.100.1:80}]
  /// </summary>
  public sealed partial class ServiceMap : pb::IMessage<ServiceMap> {
    private static readonly pb::MessageParser<ServiceMap> _parser = new pb::MessageParser<ServiceMap>(() => new ServiceMap());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<ServiceMap> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Proto.ServiceReflection.Descriptor.MessageTypes[0]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public ServiceMap() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public ServiceMap(ServiceMap other) : this() {
      map_ = other.map_.Clone();
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public ServiceMap Clone() {
      return new ServiceMap(this);
    }

    /// <summary>Field number for the "map" field.</summary>
    public const int MapFieldNumber = 1;
    private static readonly pbc::MapField<string, global::Proto.NetworkAddress>.Codec _map_map_codec
        = new pbc::MapField<string, global::Proto.NetworkAddress>.Codec(pb::FieldCodec.ForString(10), pb::FieldCodec.ForMessage(18, global::Proto.NetworkAddress.Parser), 10);
    private readonly pbc::MapField<string, global::Proto.NetworkAddress> map_ = new pbc::MapField<string, global::Proto.NetworkAddress>();
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pbc::MapField<string, global::Proto.NetworkAddress> Map {
      get { return map_; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as ServiceMap);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(ServiceMap other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (!Map.Equals(other.Map)) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      hash ^= Map.GetHashCode();
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
      map_.WriteTo(output, _map_map_codec);
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      size += map_.CalculateSize(_map_map_codec);
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(ServiceMap other) {
      if (other == null) {
        return;
      }
      map_.Add(other.map_);
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
            map_.AddEntriesFrom(input, _map_map_codec);
            break;
          }
        }
      }
    }

  }

  /// <summary>
  /// The service type (e.g., tcp, upd) and the address (e.g., 198.51.100.1:80)
  /// </summary>
  public sealed partial class NetworkAddress : pb::IMessage<NetworkAddress> {
    private static readonly pb::MessageParser<NetworkAddress> _parser = new pb::MessageParser<NetworkAddress>(() => new NetworkAddress());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<NetworkAddress> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Proto.ServiceReflection.Descriptor.MessageTypes[1]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public NetworkAddress() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public NetworkAddress(NetworkAddress other) : this() {
      network_ = other.network_;
      port_ = other.port_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public NetworkAddress Clone() {
      return new NetworkAddress(this);
    }

    /// <summary>Field number for the "network" field.</summary>
    public const int NetworkFieldNumber = 1;
    private string network_ = "";
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public string Network {
      get { return network_; }
      set {
        network_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "port" field.</summary>
    public const int PortFieldNumber = 2;
    private uint port_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public uint Port {
      get { return port_; }
      set {
        port_ = value;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as NetworkAddress);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(NetworkAddress other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (Network != other.Network) return false;
      if (Port != other.Port) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (Network.Length != 0) hash ^= Network.GetHashCode();
      if (Port != 0) hash ^= Port.GetHashCode();
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
      if (Network.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(Network);
      }
      if (Port != 0) {
        output.WriteRawTag(16);
        output.WriteUInt32(Port);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (Network.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(Network);
      }
      if (Port != 0) {
        size += 1 + pb::CodedOutputStream.ComputeUInt32Size(Port);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(NetworkAddress other) {
      if (other == null) {
        return;
      }
      if (other.Network.Length != 0) {
        Network = other.Network;
      }
      if (other.Port != 0) {
        Port = other.Port;
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
            Network = input.ReadString();
            break;
          }
          case 16: {
            Port = input.ReadUInt32();
            break;
          }
        }
      }
    }

  }

  #endregion

}

#endregion Designer generated code
