// <auto-generated>
//     Generated by the protocol buffer compiler.  DO NOT EDIT!
//     source: autopeering/salt/proto/salt.proto
// </auto-generated>
#pragma warning disable 1591, 0612, 3021
#region Designer generated code

using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;
namespace Proto {

  /// <summary>Holder for reflection information generated from autopeering/salt/proto/salt.proto</summary>
  public static partial class SaltReflection {

    #region Descriptor
    /// <summary>File descriptor for autopeering/salt/proto/salt.proto</summary>
    public static pbr::FileDescriptor Descriptor {
      get { return descriptor; }
    }
    private static pbr::FileDescriptor descriptor;

    static SaltReflection() {
      byte[] descriptorData = global::System.Convert.FromBase64String(
          string.Concat(
            "CiFhdXRvcGVlcmluZy9zYWx0L3Byb3RvL3NhbHQucHJvdG8SBXByb3RvIicK",
            "BFNhbHQSDQoFYnl0ZXMYASABKAwSEAoIZXhwX3RpbWUYAiABKAZCNlo0Z2l0",
            "aHViLmNvbS9pb3RhbGVkZ2VyL2hpdmUuZ28vYXV0b3BlZXJpbmcvc2FsdC9w",
            "cm90b2IGcHJvdG8z"));
      descriptor = pbr::FileDescriptor.FromGeneratedCode(descriptorData,
          new pbr::FileDescriptor[] { },
          new pbr::GeneratedClrTypeInfo(null, null, new pbr::GeneratedClrTypeInfo[] {
            new pbr::GeneratedClrTypeInfo(typeof(global::Proto.Salt), global::Proto.Salt.Parser, new[]{ "Bytes", "ExpTime" }, null, null, null, null)
          }));
    }
    #endregion

  }
  #region Messages
  public sealed partial class Salt : pb::IMessage<Salt>
  #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      , pb::IBufferMessage
  #endif
  {
    private static readonly pb::MessageParser<Salt> _parser = new pb::MessageParser<Salt>(() => new Salt());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<Salt> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Proto.SaltReflection.Descriptor.MessageTypes[0]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Salt() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Salt(Salt other) : this() {
      bytes_ = other.bytes_;
      expTime_ = other.expTime_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Salt Clone() {
      return new Salt(this);
    }

    /// <summary>Field number for the "bytes" field.</summary>
    public const int BytesFieldNumber = 1;
    private pb::ByteString bytes_ = pb::ByteString.Empty;
    /// <summary>
    /// value of the salt
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pb::ByteString Bytes {
      get { return bytes_; }
      set {
        bytes_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }

    /// <summary>Field number for the "exp_time" field.</summary>
    public const int ExpTimeFieldNumber = 2;
    private ulong expTime_;
    /// <summary>
    /// expiration time of the salt
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public ulong ExpTime {
      get { return expTime_; }
      set {
        expTime_ = value;
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as Salt);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(Salt other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (Bytes != other.Bytes) return false;
      if (ExpTime != other.ExpTime) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (Bytes.Length != 0) hash ^= Bytes.GetHashCode();
      if (ExpTime != 0UL) hash ^= ExpTime.GetHashCode();
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
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      output.WriteRawMessage(this);
    #else
      if (Bytes.Length != 0) {
        output.WriteRawTag(10);
        output.WriteBytes(Bytes);
      }
      if (ExpTime != 0UL) {
        output.WriteRawTag(17);
        output.WriteFixed64(ExpTime);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    void pb::IBufferMessage.InternalWriteTo(ref pb::WriteContext output) {
      if (Bytes.Length != 0) {
        output.WriteRawTag(10);
        output.WriteBytes(Bytes);
      }
      if (ExpTime != 0UL) {
        output.WriteRawTag(17);
        output.WriteFixed64(ExpTime);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(ref output);
      }
    }
    #endif

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (Bytes.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeBytesSize(Bytes);
      }
      if (ExpTime != 0UL) {
        size += 1 + 8;
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(Salt other) {
      if (other == null) {
        return;
      }
      if (other.Bytes.Length != 0) {
        Bytes = other.Bytes;
      }
      if (other.ExpTime != 0UL) {
        ExpTime = other.ExpTime;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      input.ReadRawMessage(this);
    #else
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 10: {
            Bytes = input.ReadBytes();
            break;
          }
          case 17: {
            ExpTime = input.ReadFixed64();
            break;
          }
        }
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    void pb::IBufferMessage.InternalMergeFrom(ref pb::ParseContext input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, ref input);
            break;
          case 10: {
            Bytes = input.ReadBytes();
            break;
          }
          case 17: {
            ExpTime = input.ReadFixed64();
            break;
          }
        }
      }
    }
    #endif

  }

  #endregion

}

#endregion Designer generated code
