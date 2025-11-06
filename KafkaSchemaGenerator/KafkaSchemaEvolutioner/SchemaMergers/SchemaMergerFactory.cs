using KafkaSchemaGenerator;

namespace KafkaSchemaEvolutioner.SchemaMergers;

public interface ISchemaMerger
{
    bool AppliesTo(Format format);
    string MergeSchemas(string oldSchemaText, string newSchemaText);
}

public interface ISchemaMergerFactory
{
    ISchemaMerger GetMerger(Format format);
}

public class SchemaMergerFactory(IEnumerable<ISchemaMerger> mergers) : ISchemaMergerFactory
{
    public ISchemaMerger GetMerger(Format format) => mergers.FirstOrDefault(x => x.AppliesTo(format)) 
        ?? throw new InvalidOperationException("Wrong format");
}
