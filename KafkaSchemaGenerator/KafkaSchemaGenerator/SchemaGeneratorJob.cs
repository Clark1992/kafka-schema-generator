using System;
using System.IO;
using System.Reflection;

namespace KafkaSchemaGenerator;

public enum Format
{
    JSON,
    AVRO,
}

public static class Validator
{
    public static void Validate(string assemblyPath, string typeName, string format, string outputFolder = null)
    {
        string errorFormat = "{0} not set";
        if (string.IsNullOrEmpty(assemblyPath)) throw new ArgumentException(string.Format(errorFormat, nameof(assemblyPath)));
        if (string.IsNullOrEmpty(typeName)) throw new ArgumentException(string.Format(errorFormat, nameof(typeName)));
        if (string.IsNullOrEmpty(format)) throw new ArgumentException(string.Format(errorFormat, nameof(assemblyPath)));
        if (string.IsNullOrEmpty(outputFolder)) throw new ArgumentException(string.Format(errorFormat, nameof(outputFolder)));
    } 
}

public static class SchemaGeneratorJob
{
    public static bool Execute(string assemblyPath, string typeName, string format, string outputFolder = null)
    {
        Validator.Validate(assemblyPath, typeName, format, outputFolder);

        format = format.ToLower();
        outputFolder = outputFolder ?? "output";

        if (!File.Exists(assemblyPath))
        {
            Console.WriteLine($"Assembly not found: {assemblyPath}");
            return false;
        }

        Assembly asm = Assembly.LoadFrom(assemblyPath);
        Type type = asm.GetType(typeName);
        if (type == null)
        {
            Console.WriteLine($"Type not found: {typeName}");
            return false;
        }

        string schemaJson;

        var schemaGenerator = new SchemaGenerator();
        switch (format)
        {
            case "json":
                schemaJson = schemaGenerator.GenerateJsonSchema(type);
                SaveTofile(outputFolder, type, schemaJson, Format.JSON);
                break;

            case "avro":
                schemaJson = schemaGenerator.GenerateAvroSchema(type);
                SaveTofile(outputFolder, type, schemaJson, Format.AVRO);
                break;

            case "avromulti":
                GenerateAvroMulti(schemaGenerator, type, outputFolder);
                break;

            default:
                Console.WriteLine("Unknown format, use 'json' or 'avro'");
                return false;
        }

        return true;
    }

    private static void SaveTofile(string outputFolder, Type type, string schemaJson, Format format)
    {
        FileWriter writer = new FileWriter(Path.ChangeExtension(Path.Combine(outputFolder, type.Name), GetExt(format)));
        writer.WriteSchema(schemaJson);
    }

    private static void GenerateAvroMulti(SchemaGenerator schemaGenerator, Type type, string outputFolder)
    {
        var schemaJsons = schemaGenerator.GenerateAvroSchemas(type);

        foreach (var schemaJson in schemaJsons)
        {
            var writer = new FileWriter(Path.ChangeExtension(Path.Combine(outputFolder, schemaJson.Key), GetExt(Format.AVRO)));
            writer.WriteSchema(schemaJson.Value);
        }
    }

    private static string GetExt(Format format) => format switch
    {
        Format.JSON => "json",
        Format.AVRO => "avsc",
        _ => throw new InvalidOperationException("Wrong format")
    };
}
