namespace KafkaSchemaGenerator.Generators;

public interface ISchemaGenerator
{
    string GenerateSchema(Type type);

    bool AppliesTo(Format format);
}

public interface ISchemaGeneratorFactory
{
    ISchemaGenerator GetGenerator(Format format);
}

public class SchemaGeneratorFactory(IEnumerable<ISchemaGenerator> generators) : ISchemaGeneratorFactory
{
    public ISchemaGenerator GetGenerator(Format format) => generators.First(x => x.AppliesTo(format));
}