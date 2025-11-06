using System.ComponentModel.DataAnnotations;

namespace KafkaSchemaGenerator.Tests.Avro;

public class SampleEventKey
{
    [Required]
    public Guid SampleId { get; set; }
}

public class SampleRebuiltEvent
{
    [Required]
    public Guid SampleId { get; set; }
    [Required]
    public string Name { get; set; }
    [Required]
    public string Description { get; set; }
    [Required]
    public SampleAddress Address { get; set; }
    [Required]
    public List<SampleAttachment> Attachments { get; set; }
    [Required]
    public DateTime OccurredAt { get; set; }
}

public class SampleAddress
{
    [Required]
    public Guid Id { get; set; }
    [Required]
    public string Street { get; set; }
    [Required]
    public string City { get; set; }
    [Required]
    public string Country { get; set; }
}

public class SampleAttachment
{
    [Required]
    public Guid Id { get; set; }
    [Required]
    public string FileName { get; set; }
    [Required]
    public string Url { get; set; }
}