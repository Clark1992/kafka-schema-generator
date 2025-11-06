using KafkaSchemaEvolutioner;
using KafkaSchemaEvolutioner.SchemaMergers;
using KafkaSchemaGenerator;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

const int numArgs = 5;

if (args.Length < numArgs && args.Length != 1)
{
    Console.WriteLine("Usage: dotnet run -- <AssemblyPath> <TypeName> <Format: json|avro|avromulti> <CurrentLatestSchemaPath> <OutputPath> [Topic]");
    Console.WriteLine("Usage: dotnet run -- params.json");
    return 1;
}

using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration(CommonSetup.ConfigureAppConfig(args))
    .ConfigureServices((c, s) => CommonSetup.AddGenerators(c, s)
        .AddSingleton<ISchemaMerger, JsonSchemaMerger>()
        .AddSingleton<ISchemaMerger, AvroSchemaMerger>()
        .AddSingleton<ISchemaMerger, ProtoSchemaMerger>()
        .AddSingleton<ISchemaMergerFactory, SchemaMergerFactory>()
        .AddSingleton<SchemaEvolutionJob>())
    .ConfigureLogging((c, l) => CommonSetup.ConfigureLogging(c, l))
    .Build();

var runner = host.Services.GetRequiredService<SchemaEvolutionJob>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();

if (args.Length == 1)
{
    return ProcessForParamsFileArg(runner, logger, args);
}
else
{
    return ProcessForCommandArgs(runner, args);
}

static int ProcessForParamsFileArg(SchemaEvolutionJob runner, ILogger<Program> logger, string[] args)
{
    string paramsFilePath = args[0];
    var paramsText = File.Exists(paramsFilePath) ? File.ReadAllText(paramsFilePath) : throw new InvalidOperationException("Couldn't find params file.");

    var paramObjects = JsonSerializer.Deserialize<List<JobArgs>>(paramsText, new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    });

    foreach (var paramObject in paramObjects)
    {
        var success = runner.Execute(
            paramObject.AssemblyPath,
            paramObject.TypeName,
            paramObject.Format,
            paramObject.CurrentLatestSchemaPath,
            paramObject.OutputPath,
            paramObject.Topic);

        if (!success)
        {
            logger.LogError("SchemaEvolutionJob.Execute returns failure. Stopping...");
            return 1;
        }
    }

    return 0;
}

static int ProcessForCommandArgs(SchemaEvolutionJob runner, string[] args)
{
    string assemblyPath = args[0];
    string typeName = args[1];
    string format = args[2].ToLower();
    string currentLatestSchemaPath = args[3];
    string outputPath = args[4];
    string topic = args.Length > numArgs ? args[5] : null;

    return runner.Execute(assemblyPath, typeName, format, currentLatestSchemaPath, outputPath, topic) ? 0 : 1;
}
