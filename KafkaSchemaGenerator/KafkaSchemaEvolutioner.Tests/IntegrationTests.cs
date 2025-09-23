using Newtonsoft.Json.Linq;
using System.Reflection;

namespace KafkaSchemaEvolutioner.Tests.IntegrationTests;

[Collection(nameof(SchemaEvolutionerTests))]
public class SchemaEvolutionerTests
{
    public SchemaEvolutionerTests()
    {
        var defaultTmpFolder = "generated";
        if (Directory.Exists(defaultTmpFolder)) Directory.Delete(defaultTmpFolder, true);
    }

    [Fact]

    public void GenerateJsonSchema_ShouldGenerateSchema()
    {
        // Arrange
        var format = "json";
        var outputFolder = $"{format}_evolved_schema";
        var typeName = "ISampleEvent";

        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = SchemaEvolutionJob.Execute(
            pathToAssembly,
            $"KafkaSchemaEvolutioner.Tests.{typeName}",
            format,
            $"downloaded_{format}",
            outputFolder);

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText("expectedJSON.json");
        var actual = File.ReadAllText($"{outputFolder}/{typeName}.json");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
    }

    [Fact]
    public void GenerateAvroSchemas_ShouldGeneratedMultipleSchemas()
    {
        // Arrange
        var format = "avromulti";
        var outputFolder = $"{format}_evolved_schema";
        var typeName = "ISampleEvent";

        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = SchemaEvolutionJob.Execute(
            pathToAssembly,
            $"KafkaSchemaEvolutioner.Tests.{typeName}",
            format,
            $"downloaded_avro",
            outputFolder);

        // Assert
        var expected = File.ReadAllText("expectedAVROMULTI.avsc");
        var expectedSchemas = JArray.Parse(expected);

        var actualSchemas = LoadFilesFromDirectory(outputFolder);

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
        }
    }

    [Theory]
    [InlineData(nameof(SampleEvent))]
    [InlineData(nameof(SampleNameChangedEvent))]
    [InlineData(nameof(SampleDescriptionChangedEvent))]
    [InlineData(nameof(SampleAddressChangedEvent))]
    [InlineData(nameof(SampleAttachmentAddedEvent))]
    [InlineData(nameof(SampleAttachmentRemovedEvent))]
    [InlineData(nameof(SampleAttachmentUpdatedEvent))]
    [InlineData(nameof(SampleCreatedEvent))]
    public void GenerateAvroSchema_ShouldGenerateSchema(string typeName)
    {
        // Arrange
        var format = "avro";
        var outputFolder = $"{format}_evolved_schema";

        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = SchemaEvolutionJob.Execute(
            pathToAssembly,
            $"KafkaSchemaEvolutioner.Tests.{typeName}",
            format,
            $"downloaded_{format}",
            outputFolder);

        // Assert
        Assert.True(result);

        var expected = File.ReadAllText("expectedAVROMULTI.avsc");
        var expectedSchemas = JArray.Parse(expected);

        var actualSchemas = LoadFilesFromDirectory(outputFolder);

        var actual = File.ReadAllText($"{outputFolder}/{typeName}.avsc");

        var actualJson = JObject.Parse(actual);
        string targetName = actualJson.Value<string>("name");

        var expectedJson = expectedSchemas
                .OfType<JObject>()
                .FirstOrDefault(o => o["name"]?.Value<string>() == targetName);

        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(actualJson, expectedJson));
    }


    public static Dictionary<string, string> LoadFilesFromDirectory(string directoryPath)
    {
        if (!Directory.Exists(directoryPath))
            throw new DirectoryNotFoundException($"Directory not found: {directoryPath}");

        var result = new Dictionary<string, string>();

        foreach (var filePath in Directory.GetFiles(directoryPath))
        {
            string fileName = Path.GetFileName(filePath);
            string content = File.ReadAllText(filePath);

            result[fileName] = content;
        }

        return result;
    }
}
