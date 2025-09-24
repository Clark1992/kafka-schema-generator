using Newtonsoft.Json.Linq;
using System.Reflection;

namespace KafkaSchemaGenerator.Tests.IntegrationTests;

[Collection(nameof(SchemaGeneratorTests))]
public class SchemaGeneratorTests : IDisposable
{
    [Theory]
    [InlineData(
        "KafkaSchemaGenerator.Tests.ISampleEvent",
        null,
        "KafkaSchemaGenerator.Tests.ISampleEvent-value.json",
        "expectedJSON-value.json")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.SampleEventKey",
        null,
        "KafkaSchemaGenerator.Tests.SampleEventKey-key.json",
        "expectedJSON-key.json")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.ISampleEvent",
        "someTopic",
        "someTopic-value.json",
        "expectedJSON-value.json")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.SampleEventKey",
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
        var result = SchemaGeneratorJob.Execute(pathToAssembly, type, "json", "json_schema", topic);

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText(expectedJsonFile);
        var actual = File.ReadAllText($"json_schema/{actualJsonFile}");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
    }

    [Theory]
    [InlineData(
        "KafkaSchemaGenerator.Tests.ISampleEvent",
        null,
        "",
        "-value",
        "expectedAVROMULTI.avsc")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.ISampleEvent",
        "someTopic",
        "someTopic-",
        "-value",
        "expectedAVROMULTI.avsc")]
    public void GenerateAvroSchemas_ShouldGenerateSchemas(
        string type,
        string topic,
        string prefix,
        string suffix,
        string expectedAvroFile)
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        string outputFolder = "avromulti_schema";
        var result = SchemaGeneratorJob.Execute(pathToAssembly, type, "avromulti", outputFolder, topic);

        // Assert
        var expected = File.ReadAllText(expectedAvroFile);
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
            AssertFileName(actual.Key, targetName, prefix, suffix, "avsc");
        }
    }

    [Theory]
    [InlineData(
        "KafkaSchemaGenerator.Tests.SampleCreatedEvent",
        null,
        "KafkaSchemaGenerator.Tests.SampleCreatedEvent-value.avsc",
        "expectedAVRO-value.avsc")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.SampleEventKey",
        null,
        "KafkaSchemaGenerator.Tests.SampleEventKey-key.avsc",
        "expectedAVRO-key.avsc")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.SampleCreatedEvent",
        "someTopic",
        "someTopic-value.avsc",
        "expectedAVRO-value.avsc")]
    [InlineData(
        "KafkaSchemaGenerator.Tests.SampleEventKey",
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
        var result = SchemaGeneratorJob.Execute(pathToAssembly, type, "avro", "avro_schema", topic);

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText(expectedAvroFile);
        var actual = File.ReadAllText($"avro_schema/{actualAvroFile}");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
    }

    public void Dispose()
    {
        List<string> dirs = ["avro_schema", "avromulti_schema", "json_schema"];

        foreach (var dir in dirs)
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
    }

    private static Dictionary<string, string> LoadFilesFromDirectory(string directoryPath)
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

    private static void AssertFileName(string fileName, string typeName, string prefix, string suffix, string ext) =>
        Assert.Equal(fileName, $"{prefix}{typeName}{suffix}.{ext}");
}
