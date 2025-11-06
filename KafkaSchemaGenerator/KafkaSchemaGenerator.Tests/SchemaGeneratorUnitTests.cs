using KafkaSchemaGenerator.Generators;
using KafkaSchemaGenerator.Tests.Json;
using Newtonsoft.Json.Linq;

using AVRO_SampleRebuiltEvent = KafkaSchemaGenerator.Tests.Avro.SampleRebuiltEvent;
using PROTO_SampleRebuiltEvent = KafkaSchemaGenerator.Tests.Proto.SampleRebuiltEvent;

namespace KafkaSchemaGenerator.Tests;

[Collection(nameof(SchemaGeneratorTests))]
public class SchemaGeneratorTests
{
    private readonly SchemaGeneratorFactory _generatorFactory;

    public SchemaGeneratorTests()
    {
        _generatorFactory = new SchemaGeneratorFactory([
            new JsonSchemaGenerator(),
            new Generators.AvroSchemaGenerator(),
            new ProtoSchemaGenerator()]);
    }

    [Fact]
    public void GenerateJsonSchema_ShouldGenerateSchema()
    {
        // Act
        var schema = _generatorFactory.GetGenerator(Format.JSON).GenerateSchema(typeof(ISampleEvent));

        // Assert
        var expected = File.ReadAllText("expectedJSON-value.json");
        Assert.NotNull(schema);
        Assert.Equal(expected, schema);
    }

    //[Fact(Skip = "Avromulti not supported currently")]
    //public void GenerateAvroSchemas_ShouldGenerateSchemas()
    //{
    //    // Act
    //    var schemaJsons = _generatorFactory.GetGenerator(Format.AVRO).GenerateSchema(typeof(ISampleEvent));

    //    // Assert
    //    var expected = File.ReadAllText("expectedAVROMULTI.avsc");
    //    var expectedSchemas = JArray.Parse(expected);

    //    Assert.NotNull(schemaJsons);
    //    Assert.Equal(schemaJsons.Count, expectedSchemas.Count);

    //    foreach (var actual in schemaJsons)
    //    {
    //        var actualJson = JObject.Parse(actual.Value);

    //        string targetName = actualJson.Value<string>("name");

    //        var expectedJson = expectedSchemas
    //            .OfType<JObject>()
    //            .FirstOrDefault(o => o["name"]?.Value<string>() == targetName);

    //        Assert.True(JToken.DeepEquals(actualJson, expectedJson));
    //    }
    //}

    [Fact]
    public void GenerateAvroSchema_ShouldGenerateSchema()
    {
        // Act
        var schema = _generatorFactory.GetGenerator(Format.AVRO).GenerateSchema(typeof(AVRO_SampleRebuiltEvent));

        // Assert
        var actualJson = JObject.Parse(schema);
        var expected = File.ReadAllText("expectedAVRO-value.avsc");
        var expectedJson = JObject.Parse(expected);

        Assert.NotNull(schema);
        Assert.True(JToken.DeepEquals(actualJson, expectedJson));
    }

    [Fact]
    public void GenerateProtoSchema_ShouldGenerateSchema()
    {
        // Act
        var actual = _generatorFactory.GetGenerator(Format.PROTO).GenerateSchema(typeof(PROTO_SampleRebuiltEvent));

        // Assert
        var expected = File.ReadAllText("expectedPROTO-value.proto");

        Assert.NotNull(actual);
        Assert.Equal(actual, expected, ignoreLineEndingDifferences: true);
    }
}