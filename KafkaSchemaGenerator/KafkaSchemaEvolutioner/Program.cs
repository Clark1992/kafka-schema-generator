using KafkaSchemaEvolutioner;
using System.Text.Json;

class JobArgs
{
    public string AssemblyPath { get; set; }
    public string TypeName { get; set; }
    public string CurrentLatestSchemaPath { get; set; }
    public string Format { get; set; }
    public string OutputPath {  get; set;  }
    public string Topic {  get; set;  }
}

class Program
{
    const int numArgs = 5;
    private static readonly JsonSerializerOptions options = new()
    {
        PropertyNameCaseInsensitive = true
    };

    static int Main(string[] args)
    {
        if (args.Length < numArgs && args.Length != 1)
        {
            Console.WriteLine("Usage: dotnet run -- <AssemblyPath> <TypeName> <Format: json|avro|avromulti> <CurrentLatestSchemaPath> <OutputPath> [Topic]");
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
                paramObject.OutputPath,
                paramObject.Topic);

            if (!success)
            {
                Console.WriteLine("SchemaEvolutionJob.Execute returns failure. Stopping...");
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
        string outputPath = args[4];
        string topic = args.Length > numArgs ? args[5] : null;

        return SchemaEvolutionJob.Execute(assemblyPath, typeName, format, currentLatestSchemaPath, outputPath, topic) ? 0 : 1;
    }
}