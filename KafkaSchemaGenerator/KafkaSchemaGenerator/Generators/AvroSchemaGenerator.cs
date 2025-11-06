using AvroSchemaGenerator;

namespace KafkaSchemaGenerator.Generators;

public class AvroSchemaGenerator : ISchemaGenerator
{
    public bool AppliesTo(Format format) => format is Format.AVRO;

    public string GenerateSchema(Type type) => type.GetSchema();

    public Dictionary<string, string> GenerateAvroSchemas(Type baseType) => throw new NotImplementedException();
}
