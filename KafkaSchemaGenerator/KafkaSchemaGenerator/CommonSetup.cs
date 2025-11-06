using KafkaSchemaGenerator.Generators;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaSchemaGenerator;

public static class CommonSetup
{
    public static Action<HostBuilderContext, IConfigurationBuilder> ConfigureAppConfig(string[] args) => 
        (context, config) =>
        {
            config.SetBasePath(Directory.GetCurrentDirectory());
            config.AddEnvironmentVariables();
            if (args != null)
                config.AddCommandLine(args);
        };

    public static IServiceCollection AddGenerators(HostBuilderContext _, IServiceCollection services) => 
        services
            .AddSingleton<ISchemaGenerator, JsonSchemaGenerator>()
            .AddSingleton<ISchemaGenerator, Generators.AvroSchemaGenerator>()
            .AddSingleton<ISchemaGenerator, ProtoSchemaGenerator>()
            .AddSingleton<ISchemaGeneratorFactory, SchemaGeneratorFactory>()
            .AddSingleton<SchemaGeneratorJob>();

    public static ILoggingBuilder ConfigureLogging(HostBuilderContext _, ILoggingBuilder logging) => 
        logging.ClearProviders()
            .AddConsole()
            .AddDebug();
}
