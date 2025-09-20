using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaSchemaGenerator;

using System.IO;

public interface IFileWriter
{
    string OutputPath { get; }

    bool Exists();
    string Read();
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

    public bool Exists() => File.Exists(OutputPath);

    public string Read() => File.ReadAllText(OutputPath);
}
