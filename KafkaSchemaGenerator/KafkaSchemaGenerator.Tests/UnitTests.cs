using KafkaSchemaGenerator.Tests.Avro;
using KafkaSchemaGenerator.Tests.Json;
using Newtonsoft.Json.Linq;

namespace KafkaSchemaGenerator.Tests.UnitTests;

[Collection(nameof(SchemaGeneratorTests))]
public class SchemaGeneratorTests
{
    private readonly SchemaGenerator _generator;

    public SchemaGeneratorTests()
    {
        _generator = new SchemaGenerator();
    }

    [Fact]
    public void GenerateJsonSchema_ShouldGenerateSchema()
    {
        // Act
        var schema = _generator.GenerateJsonSchema(typeof(ISampleEvent));

        // Assert
        var expected = File.ReadAllText("expectedJSON-value.json");
        Assert.NotNull(schema);
        Assert.Equal(expected, schema);
    }

    [Fact(Skip = "Avromulti not supported currently")]
    public void GenerateAvroSchemas_ShouldGenerateSchemas()
    {
        // Act
        var schemaJsons = _generator.GenerateAvroSchemas(typeof(ISampleEvent));

        // Assert
        var expected = File.ReadAllText("expectedAVROMULTI.avsc");
        var expectedSchemas = JArray.Parse(expected);

        Assert.NotNull(schemaJsons);
        Assert.Equal(schemaJsons.Count, expectedSchemas.Count);

        foreach (var actual in schemaJsons)
        {
            var actualJson = JObject.Parse(actual.Value);

            string targetName = actualJson.Value<string>("name");

            var expectedJson = expectedSchemas
                .OfType<JObject>()
                .FirstOrDefault(o => o["name"]?.Value<string>() == targetName);

            Assert.True(JToken.DeepEquals(actualJson, expectedJson));
        }
    }

    [Fact]
    public void GenerateAvroSchema_ShouldGenerateSchema()
    {
        // Act
        var schema = _generator.GenerateAvroSchema(typeof(SampleRebuiltEvent));

        // Assert
        var actualJson = JObject.Parse(schema);
        var expected = File.ReadAllText("expectedAVRO-value.avsc");
        var expectedJson = JObject.Parse(expected);

        Assert.NotNull(schema);
        Assert.True(JToken.DeepEquals(actualJson, expectedJson));
    }
}
