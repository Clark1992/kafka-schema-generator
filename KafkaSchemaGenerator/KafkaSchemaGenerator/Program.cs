using KafkaSchemaGenerator;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

const int numArgs = 4;
if (args.Length < numArgs)
{
    Console.WriteLine("Usage: dotnet run -- <AssemblyPath> <TypeName> <Format: json|avro|proto> <OutputFolder> [Topic]");
    return 1;
}

using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration(CommonSetup.ConfigureAppConfig(args))
    .ConfigureServices((c, s) => CommonSetup.AddGenerators(c, s))
    .ConfigureLogging((c, l) => CommonSetup.ConfigureLogging(c, l))
    .Build();

string assemblyPath = args[0];
string typeName = args[1];
string format = args[2].ToLower();
string outputFolder = args[3];
string topic = args.Length > numArgs ? args[4] : null;

var runner = host.Services.GetRequiredService<SchemaGeneratorJob>();

return runner.Execute(assemblyPath, typeName, format, outputFolder, topic) ? 0 : 1;