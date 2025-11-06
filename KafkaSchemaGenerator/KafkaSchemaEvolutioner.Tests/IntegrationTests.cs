using KafkaSchemaEvolutioner.SchemaMergers;
using KafkaSchemaGenerator;
using KafkaSchemaGenerator.Generators;
using KafkaSchemaGenerator.Tests.Common;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json.Linq;
using System.Reflection;

namespace KafkaSchemaEvolutioner.Tests.IntegrationTests;

[Collection(nameof(SchemaEvolutionerTests))]
public class SchemaEvolutionerTests
{
    private readonly SchemaEvolutionJob _sut;

    public SchemaEvolutionerTests()
    {
        _sut = new SchemaEvolutionJob(
            new SchemaGeneratorJob(
                new SchemaGeneratorFactory([
                new JsonSchemaGenerator(),
                new KafkaSchemaGenerator.Generators.AvroSchemaGenerator(),
                new ProtoSchemaGenerator()]),
                new Mock<ILogger<SchemaGeneratorJob>>().Object),
            new SchemaMergerFactory(
            [
                new JsonSchemaMerger(),
                new AvroSchemaMerger(),
                new ProtoSchemaMerger(new Mock<ILogger<ProtoSchemaMerger>>().Object)
            ]),
            new Mock<ILogger<SchemaEvolutionJob>>().Object);

        List<string> dirs = ["avro_evolved_schema", "avromulti_evolved_schema", "json_evolved_schema", "generated"];

        foreach (var dir in dirs)
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
    }

    [Theory]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Json.ISampleEvent",
        null,
        "KafkaSchemaEvolutioner.Tests.Json.ISampleEvent-value.json",
        "expectedJSON-value.json")]
    [InlineData(
       "KafkaSchemaEvolutioner.Tests.Json.SampleEventKey",
       null,
       "KafkaSchemaEvolutioner.Tests.Json.SampleEventKey-key.json",
       "expectedJSON-key.json")]
    [InlineData(
       "KafkaSchemaEvolutioner.Tests.Json.ISampleEvent",
       "someTopic",
       "someTopic-value.json",
       "expectedJSON-value.json")]
    [InlineData(
       "KafkaSchemaEvolutioner.Tests.Json.SampleEventKey",
       "someTopic",
       "someTopic-key.json",
       "expectedJSON-key.json")]

    public void GenerateJsonSchema_ShouldGenerateEvolvedSchema(
        string type,
        string topic,
        string actualJsonFile,
        string expectedJsonFile)
    {
        // Arrange
        var format = "json";
        var outputFolder = $"{format}_evolved_schema";

        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = _sut.Execute(
            pathToAssembly,
            type,
            format,
            $"downloaded_{format}",
            outputFolder,
            topic);

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText(expectedJsonFile);
        var actual = File.ReadAllText($"{outputFolder}/{actualJsonFile}");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
    }

    [Theory(Skip = "Avromulti not supported currently")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.ISampleEvent",
        null,
        "",
        "-value")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.ISampleEvent",
        "someTopic",
        "someTopic-",
        "-value")]
    public void GenerateAvroSchemas_ShouldGenerateMultipleEvolvedSchemas(
        string type,
        string topic,
        string prefix,
        string suffix)
    {
        // Arrange
        var format = "avromulti";
        var outputFolder = $"{format}_evolved_schema";

        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = _sut.Execute(
            pathToAssembly,
            type,
            format,
            $"downloaded_avro",
            outputFolder,
            topic);

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
        "KafkaSchemaEvolutioner.Tests.Avro.SampleRebuiltEvent",
        null,
        "KafkaSchemaEvolutioner.Tests.Avro.SampleRebuiltEvent-value.avsc",
        "expectedAVRO-value.avsc")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Avro.SampleEventKey",
        null,
        "KafkaSchemaEvolutioner.Tests.Avro.SampleEventKey-key.avsc",
        "expectedAVRO-key.avsc")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Avro.SampleRebuiltEvent",
        "someTopic",
        "someTopic-value.avsc",
        "expectedAVRO-value.avsc")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Avro.SampleEventKey",
        "someTopic",
        "someTopic-key.avsc",
        "expectedAVRO-key.avsc")]
    public void GenerateAvroSchema_ShouldGenerateEvolvedSchema(
        string type,
        string topic,
        string actualAvroFile,
        string expectedAvroFile)
    {
        // Arrange
        var format = "avro";
        var outputFolder = $"{format}_evolved_schema";

        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = _sut.Execute(
            pathToAssembly,
            type,
            format,
            $"downloaded_{format}",
            outputFolder,
            topic);

        // Assert
        Assert.True(result);

        var expected = File.ReadAllText(expectedAvroFile);

        var actual = File.ReadAllText($"{outputFolder}/{actualAvroFile}");

        var actualJson = JObject.Parse(actual);

        var expectedJson = JObject.Parse(expected);

        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(actualJson, expectedJson));
    }

    [Theory]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Proto.SampleRebuiltEvent",
        null,
        "KafkaSchemaEvolutioner.Tests.Proto.SampleRebuiltEvent-value.proto",
        "expectedPROTO-value.proto")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Proto.SampleEventKey",
        null,
        "KafkaSchemaEvolutioner.Tests.Proto.SampleEventKey-key.proto",
        "expectedPROTO-key.proto")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Proto.SampleRebuiltEvent",
        "someTopic",
        "someTopic-value.proto",
        "expectedPROTO-value.proto")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.Proto.SampleEventKey",
        "someTopic",
        "someTopic-key.proto",
        "expectedPROTO-key.proto")]
    public void GenerateProtoSchema_ShouldGenerateEvolvedSchema(
        string type,
        string topic,
        string actualProtoFile,
        string expectedProtoFile)
    {
        // Arrange
        var format = "proto";
        var outputFolder = $"{format}_evolved_schema";

        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = _sut.Execute(
            pathToAssembly,
            type,
            format,
            $"downloaded_{format}",
            outputFolder,
            topic);

        // Assert
        Assert.True(result);

        var expected = File.ReadAllText(expectedProtoFile);

        var actual = File.ReadAllText($"{outputFolder}/{actualProtoFile}");

        Assert.NotNull(actual);
        Assert.Equal(expected, actual, ignoreLineEndingDifferences: true);
    }
}
