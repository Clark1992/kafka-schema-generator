using KafkaSchemaEvolutioner.SchemaMergers;
using KafkaSchemaGenerator;
using Newtonsoft.Json.Linq;
using System.Text;

namespace KafkaSchemaEvolutioner;

public class SchemaEvolutionJob
{
    public static bool Execute(
            string assemblyPath,
            string typeName,
            string format,
            string currentLatestSchemaPath,
            string outputFolder,
            string topic)
    {
        string generatedTmp = "generated";

        if (Directory.Exists(generatedTmp)) Directory.Delete(generatedTmp, true);

        Validate(currentLatestSchemaPath);

        bool generated = SchemaGeneratorJob.Execute(assemblyPath, typeName, format, generatedTmp, topic);
        if (!generated)
            throw new InvalidOperationException("Schema generation failed");

        var newSchemaFiles = Directory.GetFiles(generatedTmp, "*.*")
                                          .Where(f => f.EndsWith(".json") || f.EndsWith(".avsc"))
                                          .ToArray();

        if (newSchemaFiles.Length == 0)
            throw new FileNotFoundException("No generated schema files found");

        foreach (var newSchemaPath in newSchemaFiles)
        {
            var fileName = Path.GetFileName(newSchemaPath);
            var newSchemaText = File.ReadAllText(newSchemaPath, Encoding.UTF8);

            var oldSchemaPath = Path.Combine(currentLatestSchemaPath, fileName);

            if (!File.Exists(oldSchemaPath))
            {
                Console.WriteLine($"⚠️ Old schema (from registry) {fileName} now found. Considering this is new schema.");
                SaveOutput(newSchemaText, fileName, outputFolder);
                continue;
            }

            var oldJson = JObject.Parse(File.ReadAllText(oldSchemaPath, Encoding.UTF8));
            var newJson = JObject.Parse(newSchemaText);

            var formatType = format switch
            {
                "json" => Format.JSON,
                "avro" or "avromulti" => Format.AVRO,
                _ => throw new InvalidOperationException("Wrong format param.")
            };

            var merged = SchemaMerger.MergeSchemas(oldJson, newJson, formatType);

            SaveOutput(merged.ToString(), fileName, outputFolder);
        }

        if (Directory.Exists(generatedTmp)) Directory.Delete(generatedTmp, true);

        return true;
    }

    private static void Validate(string currentLatestSchemaPath)
    {
        string errorFormat = "{0} not set";
        if (string.IsNullOrEmpty(currentLatestSchemaPath)) throw new ArgumentException(string.Format(errorFormat, nameof(currentLatestSchemaPath)));
    }

    private static void SaveOutput(string schema, string fileName, string outputFolder)
    {
        Directory.CreateDirectory(outputFolder);
        File.WriteAllText(Path.Combine(outputFolder, fileName), schema, Encoding.UTF8);
    }
}
