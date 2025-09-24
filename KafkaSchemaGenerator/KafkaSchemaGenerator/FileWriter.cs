using System;

namespace KafkaSchemaGenerator;

using System.IO;

public interface IFileWriter
{
    string OutputPath { get; }
    void WriteSchema(string schemaJson);
}

public class FileWriter : IFileWriter
{
    public string OutputPath { get; }

    public FileWriter(string outputPath) => OutputPath = outputPath;

    public void WriteSchema(string schemaJson)
    {
        if (string.IsNullOrWhiteSpace(schemaJson))
            throw new ArgumentException("Schema is empty", nameof(schemaJson));

        string directory = Path.GetDirectoryName(OutputPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(OutputPath, schemaJson);
    }
}

public static class FileNameBuilder
{
    public static string BuildFileName(SubjectNameStrategy strategy, Type type, string topic)
    {
        string suffix = GetSuffix(type.FullName);
        return strategy switch
        {
            SubjectNameStrategy.Topic => $"{topic}-{suffix}",
            SubjectNameStrategy.Record => $"{type.FullName}-{suffix}",
            _ => throw new InvalidOperationException("Wrong SubjectNameStrategy")
        };
    }

    public static string BuildAvroMultiFileName(SubjectNameStrategy strategy, string fullTypeName, string topic)
    {
        string suffix = GetSuffix(fullTypeName);
        return strategy switch
        {
            SubjectNameStrategy.Topic => $"{topic}-{fullTypeName}-{suffix}",
            SubjectNameStrategy.Record => $"{fullTypeName}-{suffix}",
            _ => throw new InvalidOperationException("Wrong SubjectNameStrategy")
        };
    }

    private static string GetSuffix(string fullTypeName)
    {
        var isKey = fullTypeName.EndsWith("key", StringComparison.OrdinalIgnoreCase);
        var suffix = isKey ? "key" : "value";
        return suffix;
    }
}