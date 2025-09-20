using KafkaSchemaGenerator;
using System;

class Program
{
    const int numArgs = 3;
    static int Main(string[] args)
    {
        if (args.Length < numArgs)
        {
            Console.WriteLine("Usage: dotnet run -- <AssemblyPath> <TypeName> <Format: json|avro|avromulti> [OutputFolder]");
            return 1;
        }

        string assemblyPath = args[0];
        string typeName = args[1];
        string format = args[2].ToLower();
        string outputFolder = args.Length > numArgs ? args[3] : "output";

        return SchemaGeneratorJob.Execute(assemblyPath, typeName, format, outputFolder) ? 0 : 1;
    }
}

