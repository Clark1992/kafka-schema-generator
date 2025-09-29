using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using NJsonSchema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;

namespace KafkaSchemaGenerator;

public interface ISchemaGenerator
{
    string GenerateJsonSchema(Type type);

    string GenerateAvroSchema(Type type);

    Dictionary<string, string> GenerateAvroSchemas(Type baseType);
}

public class SchemaGenerator : ISchemaGenerator
{
    public string GenerateJsonSchema(Type type)
    {
        var discriminator = type
            .GetCustomAttributes(false)
            .OfType<JsonPolymorphicAttribute>()
            .Select(x => x.TypeDiscriminatorPropertyName)
            .FirstOrDefault();

        var configedDerivedTypes = type
            .GetCustomAttributes(false)
            .OfType<JsonDerivedTypeAttribute>()
            .Select(x => x.DerivedType.Name);

        var sourceTypeNames = type.Assembly.GetTypes()
            .Where(t =>
                t.IsClass &&
                !t.IsAbstract &&
                type.IsAssignableFrom(t) &&
                configedDerivedTypes.Contains(t.Name))
            .Select(x => x.Name)
            .ToHashSet();

        var schema = JsonSchema.FromType(type);

        foreach (var def in schema.Definitions)
        {
            if (!sourceTypeNames.Contains(def.Key))
                continue;
            
            BuildOneOf(schema, def);
            AddConstTypeProp(schema, def, discriminator);
            schema.Properties.Clear();
        }

        return schema.ToJson();
    }

    private static void AddConstTypeProp(JsonSchema schema, KeyValuePair<string, JsonSchema> def, string discriminator)
    {
        if (string.IsNullOrEmpty(discriminator))
            return;

        def.Value.Properties[discriminator] = new JsonSchemaProperty
        {
            Type = JsonObjectType.String,
            Enumeration = { def.Key }
        };
        def.Value.RequiredProperties.Add(discriminator);
    }

    private static void BuildOneOf(JsonSchema schema, KeyValuePair<string, JsonSchema> def)
    {
        schema.OneOf.Add(new JsonSchema
        {
            Reference = def.Value
        });
    }

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

            results[type.FullName] = schemaJson;
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
