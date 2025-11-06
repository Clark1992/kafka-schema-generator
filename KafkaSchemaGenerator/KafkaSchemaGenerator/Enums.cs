namespace KafkaSchemaGenerator;

public enum SubjectNameStrategy
{
    Topic,
    Record
}

public enum Format
{
    UNKNOWN,
    JSON,
    AVRO,
    PROTO,
}