using KafkaSchemaGenerator.Generators;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace KafkaSchemaGenerator;

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

public class SchemaGeneratorJob(ISchemaGeneratorFactory factory, ILogger<SchemaGeneratorJob> logger)
{
    public bool Execute(string assemblyPath, string typeName, string format, string outputFolder = null, string topic = null)
    {
        Validator.Validate(assemblyPath, typeName, format, outputFolder, topic);

        format = format.ToLower();

        var subjectNameStrategy = SubjectNameStrategy.Record;
        if (topic != null)
        {
            subjectNameStrategy = SubjectNameStrategy.Topic;
            topic = topic.Trim();
        }

        outputFolder ??= "output";

        if (!File.Exists(assemblyPath))
        {
            logger.LogError("Assembly not found: {assemblyPath}", assemblyPath);
            return false;
        }

        Assembly asm = Assembly.LoadFrom(assemblyPath);
        Type type = asm.GetType(typeName);
        if (type == null)
        {
            logger.LogError("Type not found: {typeName}", typeName);
            return false;
        }

        string schemaJson;

        if (format.Equals("avromulti", StringComparison.OrdinalIgnoreCase))
        {
            logger.LogError("'avromulti' - not supported right now as it is hard to make deserializer to deserialize polymorphic type configed on interface, i.e. make Deserializer<TAbstraction>.Deserialize(byte[]) to return TImplementation as TAbstraction like in Json schema");
            return false;
        }

        var formatEnum = format switch
        {
            "json" => Format.JSON,
            "avro" => Format.AVRO,
            "proto" => Format.PROTO,
            _ => Format.UNKNOWN,
        };

        if (formatEnum is Format.UNKNOWN)
        {
            logger.LogError("Unknown format, use json/avro/proto");
            return false;
        }

        var schemaGenerator = factory.GetGenerator(formatEnum);

        schemaJson = schemaGenerator.GenerateSchema(type);
        SaveTofile(outputFolder, type, schemaJson, formatEnum, subjectNameStrategy, topic);
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
        Format.PROTO => "proto",
        _ => throw new InvalidOperationException("Wrong format")
    };
}
