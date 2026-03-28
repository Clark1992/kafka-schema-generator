using Chr.Avro.Abstract;
using Chr.Avro.Representation;

namespace KafkaSchemaGenerator.Generators;

public class AvroSchemaGenerator : ISchemaGenerator
{
    private readonly SchemaBuilder _builder = new(
        temporalBehavior: TemporalBehavior.EpochMilliseconds,
        nullableReferenceTypeBehavior: NullableReferenceTypeBehavior.None);

    private readonly JsonSchemaWriter _writer = new();

    public bool AppliesTo(Format format) => format is Format.AVRO;

    public string GenerateSchema(Type type)
    {
        var schema = _builder.BuildSchema(type);
        return _writer.Write(schema);
    }
}
