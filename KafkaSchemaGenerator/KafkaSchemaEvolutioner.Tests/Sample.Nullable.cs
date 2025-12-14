using ProtoBuf;
using System.ComponentModel.DataAnnotations;

namespace KafkaSchemaEvolutioner.Tests.Nullable;

[ProtoContract]
public class SampleNullable1
{
    [Required, ProtoMember(1)] public Guid SampleId { get; set; }
    [Required, ProtoMember(2)] public string Name { get; set; }
}

[ProtoContract]
public class SampleNullable2
{
    [ProtoMember(1)] public Guid? SampleId { get; set; }
    [Required, ProtoMember(2)] public string Name { get; set; }
}
