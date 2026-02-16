using KafkaSchemaEvolutioner.SchemaMergers;
using KafkaSchemaGenerator;
using Microsoft.Extensions.Logging;
using System.Text;

namespace KafkaSchemaEvolutioner;

public class SchemaEvolutionJob(SchemaGeneratorJob generatorJob, ISchemaMergerFactory factory, ILogger<SchemaEvolutionJob> logger)
{
    public bool Execute(
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

        bool generated = generatorJob.Execute(assemblyPath, typeName, format, generatedTmp, topic);
        if (!generated)
            throw new InvalidOperationException("Schema generation failed");

        var newSchemaFiles = Directory.GetFiles(generatedTmp, "*.*")
                                          .Where(f => f.EndsWith(".json") || f.EndsWith(".avsc") || f.EndsWith(".proto"))
                                          .ToArray();

        if (newSchemaFiles.Length == 0)
            throw new FileNotFoundException("No generated schema files found");

        foreach (var newSchemaPath in newSchemaFiles)
        {
            var fileName = Path.GetFileName(newSchemaPath);

            logger.LogInformation("Processing {fileName}...", fileName);

            var newSchemaText = File.ReadAllText(newSchemaPath, Encoding.UTF8);

            var oldSchemaPath = Path.Combine(currentLatestSchemaPath, fileName);

            if (!File.Exists(oldSchemaPath))
            {
                logger.LogInformation("⚠️ Old schema {fileName} not found. Considering this is new schema.", fileName);
                SaveOutput(newSchemaText, fileName, outputFolder);
                continue;
            }

            var oldSchemaText = File.ReadAllText(oldSchemaPath, Encoding.UTF8);

            var formatType = format switch
            {
                "json" => Format.JSON,
                "avro" or "avromulti" => Format.AVRO,
                "proto" => Format.PROTO,
                _ => throw new InvalidOperationException("Wrong format param.")
            };

            var merged = factory.GetMerger(formatType).MergeSchemas(oldSchemaText, newSchemaText);

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
