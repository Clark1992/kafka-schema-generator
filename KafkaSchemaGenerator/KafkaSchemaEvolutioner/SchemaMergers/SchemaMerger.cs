using KafkaSchemaGenerator;
using Newtonsoft.Json.Linq;

namespace KafkaSchemaEvolutioner.SchemaMergers;

public static class SchemaMerger
{
    public static JObject MergeSchemas(JObject oldSchema, JObject newSchema, Format format) => 
        format switch
        {
            Format.JSON => JsonSchemaMerger.MergeSchemas(oldSchema, newSchema),
            Format.AVRO => AvroSchemaMerger.MergeSchemas(oldSchema, newSchema),
            _ => throw new InvalidOperationException("Wrong format")
        };
}
