// <auto-generated>
//   This file was generated by a tool; you should avoid making direct changes.
//   Consider using 'partial classes' to extend these types
//   Input: packet.proto
// </auto-generated>

#region Designer generated code
#pragma warning disable CS0612, CS0618, CS1591, CS3021, IDE1006, RCS1036, RCS1057, RCS1085, RCS1192
namespace Proto
{

    [global::ProtoBuf.ProtoContract()]
    public partial class Packet : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"type")]
        public uint Type { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"data")]
        public byte[] Data { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"public_key")]
        public byte[] PublicKey { get; set; }

        [global::ProtoBuf.ProtoMember(4, Name = @"signature")]
        public byte[] Signature { get; set; }

    }

}

#pragma warning restore CS0612, CS0618, CS1591, CS3021, IDE1006, RCS1036, RCS1057, RCS1085, RCS1192
#endregion
