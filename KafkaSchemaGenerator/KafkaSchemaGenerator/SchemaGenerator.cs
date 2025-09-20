using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using NJsonSchema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace KafkaSchemaGenerator;

public interface ISchemaGenerator
{
    string GenerateJsonSchema(Type type);

    string GenerateAvroSchema(Type type);

    Dictionary<string, string> GenerateAvroSchemas(Type baseType);
}

public class SchemaGenerator : ISchemaGenerator
{
    public string GenerateJsonSchema(Type type) => JsonSchema.FromType(type).ToJson();

    public Dictionary<string, string> GenerateAvroSchemas(Type baseType)
    {
        var context = new SchemaBuilderContext();
        var builder = new SchemaBuilder();

        var derivedTypes = baseType.Assembly
            .GetTypes()
            .Where(t => baseType.IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract);

        var allTypes = derivedTypes.ToList();

        if (!baseType.IsInterface && !baseType.IsAbstract)
        {
            allTypes.Add(baseType);
        }

        var results = new Dictionary<string, string>();

        foreach (var type in allTypes)
        {
            var schema = builder.BuildSchema(type, context);

            using var ms = new MemoryStream();
            var jsonWriter = new JsonSchemaWriter();

            // canonical=false -> human readable
            jsonWriter.Write(schema, ms, canonical: false);

            ms.Position = 0;
            using var sr = new StreamReader(ms, Encoding.UTF8);
            var schemaJson = sr.ReadToEnd();

            results[type.Name] = schemaJson;
        }

        return results;
    }

    public string GenerateAvroSchema(Type type)
    {
        var c = new SchemaBuilderContext();
        var builder = new SchemaBuilder();

        Schema schema = builder.BuildSchema(type);

        using var ms = new MemoryStream();
        var jsonWriter = new JsonSchemaWriter();

        // canonical=false -> human readable
        jsonWriter.Write(schema, ms, canonical: false);

        ms.Position = 0;
        using var sr = new StreamReader(ms, Encoding.UTF8);
        return sr.ReadToEnd();

    }
}
