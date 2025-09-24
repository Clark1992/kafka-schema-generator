using System.Text.Json.Serialization;

namespace KafkaSchemaEvolutioner.Tests;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(SampleCreatedEvent), nameof(SampleCreatedEvent))]
[JsonDerivedType(typeof(SampleNameChangedEvent), nameof(SampleNameChangedEvent))]
[JsonDerivedType(typeof(SampleDescriptionChangedEvent), nameof(SampleDescriptionChangedEvent))]
[JsonDerivedType(typeof(SampleAddressChangedEvent), nameof(SampleAddressChangedEvent))]
[JsonDerivedType(typeof(SampleAttachmentAddedEvent), nameof(SampleAttachmentAddedEvent))]
[JsonDerivedType(typeof(SampleAttachmentRemovedEvent), nameof(SampleAttachmentRemovedEvent))]
[JsonDerivedType(typeof(SampleAttachmentUpdatedEvent), nameof(SampleAttachmentUpdatedEvent))]
public interface ISampleEvent
{
    Guid SampleId { get; }
    DateTimeOffset NewOccurredAt { get; set; }
}

public record SampleEventKey(Guid SampleId);

public record SampleEvent(Guid SampleId) : ISampleEvent
{
    public DateTimeOffset NewOccurredAt { get; set; }
}

public record SampleCreatedEvent(Guid SampleId, string Name, string Description, SampleAddress Address) : SampleEvent(SampleId);
public record SampleNameChangedEvent(Guid SampleId, string NewName) : SampleEvent(SampleId);
public record SampleDescriptionChangedEvent(Guid SampleId, string NewDescription) : SampleEvent(SampleId);
public record SampleAddressChangedEvent(Guid SampleId, SampleAddress NewAddress) : SampleEvent(SampleId);
public record SampleAttachmentAddedEvent(Guid SampleId, SampleAttachment AddedAttachment) : SampleEvent(SampleId);
public record SampleAttachmentRemovedEvent(Guid SampleId, Guid AttachmentId) : SampleEvent(SampleId);
public record SampleAttachmentUpdatedEvent(Guid SampleId, Guid AttachmentId, string NewFileName, string NewUrl) : SampleEvent(SampleId);


public class SampleAddress
{
    public Guid Id { get; set; }
    public string Street { get; set; }
    public string City { get; set; }
    public string NewCountry { get; set; }
}

public class SampleAttachment
{
    public Guid Id { get; set; }
    public string FileName { get; set; }
    public string NewUrl { get; set; }
}