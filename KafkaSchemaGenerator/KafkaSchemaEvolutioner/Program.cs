using KafkaSchemaEvolutioner;
using System.Diagnostics;
using System.Text.Json;

class JobArgs
{
    public string AssemblyPath { get; set; }
    public string TypeName { get; set; }
    public string CurrentLatestSchemaPath { get; set; }
    public string Format { get; set; }
    public string OutputPath {  get; set;  }
}

class Program
{
    const int numArgs = 4;
    private static readonly JsonSerializerOptions options = new()
    {
        PropertyNameCaseInsensitive = true
    };

    static int Main(string[] args)
    {
        if (args.Length < numArgs && args.Length != 1)
        {
            Console.WriteLine("Usage: dotnet run -- <AssemblyPath> <TypeName> <Format: json|avro|avromulti> <CurrentLatestSchemaPath> [OutputPath]");
            Console.WriteLine("Usage: dotnet run -- params.json");
            return 1;
        }

        if (args.Length == 1)
        {
            return ProcessForParamsFileArg(args);
        }
        else
        {
            return ProcessForCommandArgs(args);
        }
    }

    private static int ProcessForParamsFileArg(string[] args)
    {
        string paramsFilePath = args[0];
        var paramsText = File.Exists(paramsFilePath) ? File.ReadAllText(paramsFilePath) : throw new InvalidOperationException("Couldn't find params file.");

        var paramObjects = JsonSerializer.Deserialize<List<JobArgs>>(paramsText, options);

        foreach (var paramObject in paramObjects)
        {
            var success = SchemaEvolutionJob.Execute(
                paramObject.AssemblyPath,
                paramObject.TypeName,
                paramObject.Format,
                paramObject.CurrentLatestSchemaPath,
                paramObject.OutputPath);

            if (!success)
            {
                Console.WriteLine("SchemaEvolutionJob.Execute return failure. Stopping...");
                return 1;
            }
        }

        return 0;
    }

    private static int ProcessForCommandArgs(string[] args)
    {
        string assemblyPath = args[0];
        string typeName = args[1];
        string format = args[2].ToLower();
        string currentLatestSchemaPath = args[3];
        string outputPath = args.Length > numArgs ? args[4] : "output";

        return SchemaEvolutionJob.Execute(assemblyPath, typeName, format, currentLatestSchemaPath, outputPath) ? 0 : 1;
    }
}