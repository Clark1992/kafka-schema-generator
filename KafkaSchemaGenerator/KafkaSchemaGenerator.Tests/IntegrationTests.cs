using KafkaSchemaGenerator.Generators;
using KafkaSchemaGenerator.Tests.Common;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json.Linq;
using System.Reflection;

namespace KafkaSchemaGenerator.Tests.IntegrationTests;

[Collection(nameof(SchemaGeneratorTests))]
public class SchemaGeneratorTests
{
    private readonly SchemaGeneratorJob _sut;

    public SchemaGeneratorTests()
    {
        _sut = new SchemaGeneratorJob(new SchemaGeneratorFactory([
            new JsonSchemaGenerator(),
            new Generators.AvroSchemaGenerator(),
            new ProtoSchemaGenerator()]),
            new Mock<ILogger<SchemaGeneratorJob>>().Object);

        List<string> dirs = ["avro_schema", "avromulti_schema", "json_schema", "proto_schema"];

        foreach (var dir in dirs)
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
    }

    [Theory]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Json.ISampleEvent",
        null,
        "KafkaSchemaGenerator.Tests.Json.ISampleEvent-value.json",
        "expectedJSON-value.json")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Json.SampleEventKey",
        null,
        "KafkaSchemaGenerator.Tests.Json.SampleEventKey-key.json",
        "expectedJSON-key.json")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Json.ISampleEvent",
        "someTopic",
        "someTopic-value.json",
        "expectedJSON-value.json")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Json.SampleEventKey",
        "someTopic",
        "someTopic-key.json",
        "expectedJSON-key.json")]
    public void GenerateJsonSchema_ShouldGenerateSchema(
        string type, 
        string topic,
        string actualJsonFile,
        string expectedJsonFile)
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = _sut.Execute(pathToAssembly, type, "json", "json_schema", topic);

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText(expectedJsonFile);
        var actual = File.ReadAllText($"json_schema/{actualJsonFile}");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
    }

    [Theory(Skip = "Avromulti not supported currently")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.ISampleEvent",
        null,
        "",
        "-value")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.ISampleEvent",
        "someTopic",
        "someTopic-",
        "-value")]
    public void GenerateAvroSchemas_ShouldGenerateSchemas(
        string type,
        string topic,
        string prefix,
        string suffix)
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        string outputFolder = "avromulti_schema";
        var result = _sut.Execute(pathToAssembly, type, "avromulti", outputFolder, topic);

        // Assert
        var expected = File.ReadAllText("expectedAVROMULTI.avsc");
        var expectedSchemas = JArray.Parse(expected);

        var actualSchemas = TestUtils.LoadFilesFromDirectory(outputFolder);

        Assert.NotNull(actualSchemas);
        Assert.Equal(actualSchemas.Count, expectedSchemas.Count);

        foreach (var actual in actualSchemas)
        {
            var actualJson = JObject.Parse(actual.Value);

            string targetName = actualJson.Value<string>("name");

            var expectedJson = expectedSchemas
                .OfType<JObject>()
                .FirstOrDefault(o => o["name"]?.Value<string>() == targetName);

            Assert.True(JToken.DeepEquals(actualJson, expectedJson));
            TestUtils.AssertFileName(actual.Key, targetName, prefix, suffix, "avsc");
        }
    }

    [Theory]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Avro.SampleRebuiltEvent",
        null,
        "KafkaSchemaGenerator.Tests.Avro.SampleRebuiltEvent-value.avsc",
        "expectedAVRO-value.avsc")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Avro.SampleEventKey",
        null,
        "KafkaSchemaGenerator.Tests.Avro.SampleEventKey-key.avsc",
        "expectedAVRO-key.avsc")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Avro.SampleRebuiltEvent",
        "someTopic",
        "someTopic-value.avsc",
        "expectedAVRO-value.avsc")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Avro.SampleEventKey",
        "someTopic",
        "someTopic-key.avsc",
        "expectedAVRO-key.avsc")]
    public void GenerateAvroSchema_ShouldGenerateSchema(
        string type,
        string topic,
        string actualAvroFile,
        string expectedAvroFile)
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = _sut.Execute(pathToAssembly, type, "avro", "avro_schema", topic);

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText(expectedAvroFile);
        var actual = File.ReadAllText($"avro_schema/{actualAvroFile}");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
    }

    [Theory]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Proto.SampleRebuiltEvent",
        null,
        "KafkaSchemaGenerator.Tests.Proto.SampleRebuiltEvent-value.proto",
        "expectedPROTO-value.proto")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Proto.SampleEventKey",
        null,
        "KafkaSchemaGenerator.Tests.Proto.SampleEventKey-key.proto",
        "expectedPROTO-key.proto")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Proto.SampleRebuiltEvent",
        "someTopic",
        "someTopic-value.proto",
        "expectedPROTO-value.proto")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.Proto.SampleEventKey",
        "someTopic",
        "someTopic-key.proto",
        "expectedPROTO-key.proto")]
    public void GenerateProtoSchema_ShouldGenerateSchema(
        string type,
        string topic,
        string actualProtoFile,
        string expectedProtoFile)
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = _sut.Execute(pathToAssembly, type, "proto", "proto_schema", topic);

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText(expectedProtoFile);
        var actual = File.ReadAllText($"proto_schema/{actualProtoFile}");
        Assert.NotNull(actual);
        Assert.Equal(actual, expected, ignoreLineEndingDifferences: true);
    }
}
