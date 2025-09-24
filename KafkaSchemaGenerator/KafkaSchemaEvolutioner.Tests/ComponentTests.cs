using KafkaSchemaGenerator.Tests.Common;
using Newtonsoft.Json.Linq;
using System.Diagnostics;

namespace KafkaSchemaEvolutioner.Tests.ComponentTests;

[Collection(nameof(SchemaEvolutionerTests))]
public class SchemaEvolutionerTests
{
    public SchemaEvolutionerTests()
    {
        List<string> dirs = ["avro_evolved_schema", "avromulti_evolved_schema", "json_evolved_schema", "generated"];

        foreach (var dir in dirs)
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
    }

    [Fact]

    public async Task RunApp_WithParamFile_ShouldIterateAndGenerateSchemas()
    {
        // Arrange
        var jsonOutput = "json_evolved_schema";
        if (Directory.Exists(jsonOutput)) Directory.Delete(jsonOutput, true);

        var avroOutput = "avromulti_evolved_schema";
        if (Directory.Exists(avroOutput)) Directory.Delete(avroOutput, true);

        // Act
        var process = Process.Start(new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = "KafkaSchemaEvolutioner.dll params.json",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        });

        string output = await process.StandardOutput.ReadToEndAsync();
        string error = await process.StandardError.ReadToEndAsync();

        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            Console.WriteLine("Error:");
            Console.WriteLine(error);
        }
        else
        {
            Console.WriteLine("Success:");
            Console.WriteLine(output);
        }

        // Assert

        // json
        Assert.True(process.ExitCode == 0);
        var expectedJSON = File.ReadAllText("expectedJSON-value.json");
        var actual = File.ReadAllText($"json_evolved_schema/someTopic-value.json");
        Assert.NotNull(actual);
        Assert.True(JToken.DeepEquals(JObject.Parse(actual), JObject.Parse(expectedJSON)));

        // avromulti
        var expected = File.ReadAllText("expectedAVROMULTI.avsc");
        var expectedSchemas = JArray.Parse(expected);

        var actualSchemas = Utils.LoadFilesFromDirectory("avromulti_evolved_schema");

        Assert.NotNull(actualSchemas);
        Assert.Equal(actualSchemas.Count, expectedSchemas.Count);

        foreach (var actualAvro in actualSchemas)
        {
            var actualJson = JObject.Parse(actualAvro.Value);

            string targetName = actualJson.Value<string>("name");

            var expectedJson = expectedSchemas
                .OfType<JObject>()
                .FirstOrDefault(o => o["name"]?.Value<string>() == targetName);

            Assert.True(JToken.DeepEquals(actualJson, expectedJson));
        }
    }
}
