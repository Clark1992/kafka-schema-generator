using AvroSchemaGenerator;
using NJsonSchema;
using NJsonSchema.NewtonsoftJson.Generation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace KafkaSchemaGenerator;

public interface ISchemaGenerator
{
    string GenerateJsonSchema(Type type);

    string GenerateAvroSchema(Type type);
}

public class SchemaGenerator : ISchemaGenerator
{
    public string GenerateJsonSchema(Type type)
    {
        var schema = JsonSchema.FromType(
            type, 
            new NewtonsoftJsonSchemaGeneratorSettings 
            { 
                FlattenInheritanceHierarchy = true,
            });

        var polymorphic = type
            .GetCustomAttributes(false)
            .OfType<JsonPolymorphicAttribute>()
            .FirstOrDefault();

        if (polymorphic is not null)
        {
            ProcessPolymorphic(type, schema, polymorphic);
        }

        return schema.ToJson();
    }

    private static void ProcessPolymorphic(Type type, JsonSchema schema, JsonPolymorphicAttribute polymorphic)
    {
        var discriminator = polymorphic.TypeDiscriminatorPropertyName;

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

        foreach (var def in schema.Definitions)
        {
            if (!sourceTypeNames.Contains(def.Key))
                continue;

            BuildOneOf(schema, def);
            AddConstTypeProp(def, discriminator);
            FixGuids(def.Value);
        }

        CleanRoot(schema);
    }

    private static void FixGuids(JsonSchema def)
    {
        foreach (var property in def.Properties.Values)
        {
            FixGuid(property);
        }
    }

    private static void FixGuid(JsonSchema schema)
    {
        if (schema == null)
            return;

        if (schema.Format == "guid")
            schema.Format = "uuid";

        foreach (var property in schema.Properties.Values)
        {
            FixGuid(property);
        }

        foreach (var definition in schema.Definitions.Values)
        {
            FixGuid(definition);
        }

        if (schema.Item != null)
            FixGuid(schema.Item);

        if (schema.AdditionalPropertiesSchema != null)
            FixGuid(schema.AdditionalPropertiesSchema);

        foreach (var subSchema in schema.AllOf)
        {
            FixGuid(subSchema);
        }

        foreach (var subSchema in schema.AnyOf)
        {
            FixGuid(subSchema);
        }

        foreach (var subSchema in schema.OneOf)
        {
            FixGuid(subSchema);
        }

        if (schema.Reference != null)
            FixGuid(schema.Reference);
    }

    private static void CleanRoot(JsonSchema schema)
    {
        schema.Properties.Clear();
        schema.RequiredProperties.Clear();
        schema.DiscriminatorObject = null;
        schema.IsAbstract = false;
        schema.AllowAdditionalProperties = true;
    }

    private static void AddConstTypeProp(KeyValuePair<string, JsonSchema> def, string discriminator)
    {
        if (string.IsNullOrEmpty(discriminator))
            return;

        var typeSchemaProp = new JsonSchemaProperty
        {
            Type = JsonObjectType.String,
            Enumeration = { def.Key }
        };

        def.Value.Properties[discriminator] = typeSchemaProp;
        def.Value.RequiredProperties.Add(discriminator);
    }

    private static void BuildOneOf(JsonSchema schema, KeyValuePair<string, JsonSchema> def)
    {
        schema.OneOf.Add(new JsonSchema
        {
            Reference = def.Value
        });
    }

    public string GenerateAvroSchema(Type type) => type.GetSchema();

    public Dictionary<string, string> GenerateAvroSchemas(Type baseType) => throw new NotImplementedException();
}
