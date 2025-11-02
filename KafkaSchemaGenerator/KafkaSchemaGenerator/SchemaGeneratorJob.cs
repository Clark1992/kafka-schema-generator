using System;
using System.IO;
using System.Reflection;

namespace KafkaSchemaGenerator;

public enum Format
{
    JSON,
    AVRO,
}

public enum SubjectNameStrategy
{
    Topic,
    Record
}

public static class Validator
{
    public static void Validate(string assemblyPath, string typeName, string format, string outputFolder = null, string topic = null)
    {
        string errorFormat = "{0} not set";
        if (string.IsNullOrEmpty(assemblyPath)) throw new ArgumentException(string.Format(errorFormat, nameof(assemblyPath)));
        if (string.IsNullOrEmpty(typeName)) throw new ArgumentException(string.Format(errorFormat, nameof(typeName)));
        if (string.IsNullOrEmpty(format)) throw new ArgumentException(string.Format(errorFormat, nameof(assemblyPath)));
        if (string.IsNullOrEmpty(outputFolder)) throw new ArgumentException(string.Format(errorFormat, nameof(outputFolder)));
        if (topic != null && topic.Trim().Length == 0) throw new ArgumentException(string.Format(errorFormat, nameof(outputFolder)));
    } 
}

public static class SchemaGeneratorJob
{
    public static bool Execute(string assemblyPath, string typeName, string format, string outputFolder = null, string topic = null)
    {
        Validator.Validate(assemblyPath, typeName, format, outputFolder, topic);

        format = format.ToLower();

        var subjectNameStrategy = SubjectNameStrategy.Record;
        if (topic != null)
        {
            subjectNameStrategy = SubjectNameStrategy.Topic;
            topic = topic.Trim();
        }

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
                SaveTofile(outputFolder, type, schemaJson, Format.JSON, subjectNameStrategy, topic);
                break;

            case "avro":
                schemaJson = schemaGenerator.GenerateAvroSchema(type);
                SaveTofile(outputFolder, type, schemaJson, Format.AVRO, subjectNameStrategy, topic);
                break;

            case "avromulti":
                Console.WriteLine("'avromulti' - not supported right now as it is hard to make deserializer to deserialize polymorphic type configed on interface, i.e. make Deserializer<TAbstraction>.Deserialize(byte[]) to return TImplementation as TAbstraction like in Json schema");
                break;

            default:
                Console.WriteLine("Unknown format, use 'json' or 'avro'");
                return false;
        }

        return true;
    }

    private static void SaveTofile(
        string outputFolder,
        Type type,
        string schemaJson,
        Format format,
        SubjectNameStrategy strategy,
        string topic)
    {
        var fileName = FileNameBuilder.BuildFileName(strategy, type, topic);
        FileWriter writer = new($"{Path.Combine(outputFolder, fileName)}.{GetExt(format)}");
        writer.WriteSchema(schemaJson);
    }

    private static string GetExt(Format format) => format switch
    {
        Format.JSON => "json",
        Format.AVRO => "avsc",
        _ => throw new InvalidOperationException("Wrong format")
    };
}
