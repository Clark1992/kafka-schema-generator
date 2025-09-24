using KafkaSchemaGenerator;
using System;

class Program
{
    const int numArgs = 4;
    static int Main(string[] args)
    {
        if (args.Length < numArgs)
        {
            Console.WriteLine("Usage: dotnet run -- <AssemblyPath> <TypeName> <Format: json|avro|avromulti> <OutputFolder> [Topic]");
            return 1;
        }

        string assemblyPath = args[0];
        string typeName = args[1];
        string format = args[2].ToLower();
        string outputFolder = args[3];
        string topic = args.Length > numArgs ? args[4] : null;

        return SchemaGeneratorJob.Execute(assemblyPath, typeName, format, outputFolder, topic) ? 0 : 1;
    }
}

