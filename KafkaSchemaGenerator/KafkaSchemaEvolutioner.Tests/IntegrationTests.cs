using KafkaSchemaGenerator.Tests.Common;
using Newtonsoft.Json.Linq;
using System.Reflection;

namespace KafkaSchemaEvolutioner.Tests.IntegrationTests;

[Collection(nameof(SchemaEvolutionerTests))]
public class SchemaEvolutionerTests
{
    public SchemaEvolutionerTests()
    {
        List<string> dirs = ["avro_evolved_schema", "avromulti_evolved_schema", "json_evolved_schema", "generated"];

        foreach (var dir in dirs)
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
    }

    [Theory]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.ISampleEvent",
        null,
        "KafkaSchemaEvolutioner.Tests.ISampleEvent-value.json",
        "expectedJSON-value.json")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.SampleEventKey",
        null,
        "KafkaSchemaEvolutioner.Tests.SampleEventKey-key.json",
        "expectedJSON-key.json")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.ISampleEvent",
        "someTopic",
        "someTopic-value.json",
        "expectedJSON-value.json")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.SampleEventKey",
        "someTopic",
        "someTopic-key.json",
        "expectedJSON-key.json")]

    public void GenerateJsonSchema_ShouldGenerateSchema(
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
        var result = SchemaEvolutionJob.Execute(
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

    [Theory]
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
    public void GenerateAvroSchemas_ShouldGenerateMultipleSchemas(
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
        var result = SchemaEvolutionJob.Execute(
            pathToAssembly,
            type,
            format,
            $"downloaded_avro",
            outputFolder,
            topic);

        // Assert
        var expected = File.ReadAllText("expectedAVROMULTI.avsc");
        var expectedSchemas = JArray.Parse(expected);

        var actualSchemas = Utils.LoadFilesFromDirectory(outputFolder);

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

            Utils.AssertFileName(actual.Key, targetName, prefix, suffix, "avsc");
        }
    }

    [Theory]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.SampleCreatedEvent",
        null,
        "KafkaSchemaEvolutioner.Tests.SampleCreatedEvent-value.avsc",
        "expectedAVROMULTI.avsc")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.SampleEventKey",
        null,
        "KafkaSchemaEvolutioner.Tests.SampleEventKey-key.avsc",
        "expectedAVRO-key.avsc")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.SampleCreatedEvent",
        "someTopic",
        "someTopic-value.avsc",
        "expectedAVROMULTI.avsc")]
    [InlineData(
        "KafkaSchemaEvolutioner.Tests.SampleEventKey",
        "someTopic",
        "someTopic-key.avsc",
        "expectedAVRO-key.avsc")]
    public void GenerateAvroSchema_ShouldGenerateSchema(
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
        var result = SchemaEvolutionJob.Execute(
            pathToAssembly,
            type,
            format,
            $"downloaded_{format}",
            outputFolder,
            topic);

        // Assert
        Assert.True(result);

        var expected = File.ReadAllText(expectedAvroFile);
        var expectedSchemas = JArray.Parse(expected);

        var actualSchemas = Utils.LoadFilesFromDirectory(outputFolder);

        var actual = File.ReadAllText($"{outputFolder}/{actualAvroFile}");

        var actualJson = JObject.Parse(actual);
        string targetName = actualJson.Value<string>("name");

        var expectedJson = expectedSchemas
                .OfType<JObject>()
                .FirstOrDefault(o => o["name"]?.Value<string>() == targetName);

        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(actualJson, expectedJson));
    }
}
