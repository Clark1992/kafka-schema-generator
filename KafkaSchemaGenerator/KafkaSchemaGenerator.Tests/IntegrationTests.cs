using Newtonsoft.Json.Linq;
using System.Reflection;

namespace KafkaSchemaGenerator.Tests.IntegrationTests;

[Collection(nameof(SchemaGeneratorTests))]
public class SchemaGeneratorTests
{
    [Fact]
    public void GenerateJsonSchema_ShouldGenerateSchema()
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = SchemaGeneratorJob.Execute(pathToAssembly, "KafkaSchemaGenerator.Tests.ISampleEvent", "json", "json_schema");

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText("expectedJSON.json");
        var actual = File.ReadAllText("json_schema/ISampleEvent.json");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
    }

    [Fact]
    public void GenerateAvroSchemas_ShouldGeneratedDictionaryOfSchemas()
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        string outputFolder = "avromulti_schema";
        var result = SchemaGeneratorJob.Execute(pathToAssembly, "KafkaSchemaGenerator.Tests.ISampleEvent", "avromulti", outputFolder);

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

    [Fact]
    public void GenerateAvroSchema_ShouldGenerateSchema()
    {
        // Act
        string pathToAssembly = Assembly.GetExecutingAssembly().Location;
        var result = SchemaGeneratorJob.Execute(pathToAssembly, "KafkaSchemaGenerator.Tests.SampleCreatedEvent", "avro", "avro_schema");

        // Assert
        Assert.True(result);
        var expected = File.ReadAllText("expectedAVRO.avsc");
        var actual = File.ReadAllText("avro_schema/SampleCreatedEvent.avsc");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expected)));
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
