using ProtoBuf;

namespace KafkaSchemaGenerator.Tests.Proto;

[ProtoContract]
public class SampleEventKey
{
    [ProtoMember(1)] public Guid SampleId { get; set; }
}

[ProtoContract]
public class SampleRebuiltEvent
{
    [ProtoMember(1)] public Guid SampleId { get; set; }
    [ProtoMember(2)] public string Name { get; set; }
    [ProtoMember(3)] public string Description { get; set; }
    [ProtoMember(4)] public SampleAddress Address { get; set; }
    [ProtoMember(5)] public List<SampleAttachment> Attachments { get; set; }
    [ProtoMember(6)] public DateTime OccurredAt { get; set; }
    [ProtoMember(7)] public DateTime? OptionalOccurredAt { get; set; }

    [ProtoMember(8, IsRequired = false)] public SampleAddress OptionalAddress2 { get; set; }
}

[ProtoContract]
public class SampleAddress
{
    [ProtoMember(1)] public Guid Id { get; set; }
    [ProtoMember(2)] public string Street { get; set; }
    [ProtoMember(3)] public string City { get; set; }
    [ProtoMember(4)] public string Country { get; set; }
    [ProtoMember(5)] public Guid? OptionalId { get; set; }
}

[ProtoContract]
public class SampleAttachment
{
    [ProtoMember(1)] public Guid Id { get; set; }
    [ProtoMember(2)] public string FileName { get; set; }
    [ProtoMember(3)] public string Url { get; set; }

    [ProtoMember(4, IsRequired = false)] public Guid? OptionalId { get; set; }
}