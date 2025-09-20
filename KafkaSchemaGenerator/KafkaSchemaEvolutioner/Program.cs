using KafkaSchemaEvolutioner;

class Program
{
    const int numArgs = 4;
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
        string downloadedSchemas = args[3];
        string outputFolder = args.Length > numArgs ? args[4] : "output";

        return SchemaEvolutionJob.Execute(assemblyPath, typeName, format, downloadedSchemas, outputFolder) ? 0 : 1;
    }
}