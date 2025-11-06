using KafkaSchemaGenerator.Common.Utils;


namespace KafkaSchemaGenerator.Tests.Common;

[Collection(nameof(StringExtensionsUnitTests))]
public class StringExtensionsUnitTests
{
    [Fact]
    public void ReInsertDeleted_ShouldReInsertRemovedAsDeprecated()
    {
        string oldSchema = File.ReadAllText("ReinsertDeleted/old.proto");
        string newSchema = File.ReadAllText("ReinsertDeleted/new.proto");

        string expected = File.ReadAllText("ReinsertDeleted/new_expected.proto");

        string actual = newSchema.ReInsertDeleted(oldSchema);

        Assert.Equal(expected, actual, ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void AddOptionals_ShouldReInsertRemovedAsDeprecated()
    {
        string schema = File.ReadAllText("AddOptionals/schema.proto");

        string expected = File.ReadAllText("AddOptionals/expected.proto");

        Dictionary<string, HashSet<NameNumber>> nullables = new()
        {
            ["SampleEventKey"] = [
                new NameNumber("Prop2", 2),
                new NameNumber("Prop3", 3),
            ],
            ["SampleEventKey:SampleEventKey2"] = [
                new NameNumber("Prop2", 2),
                new NameNumber("Prop3", 3),
            ]
        };

        string actual = schema.AddOptionals(nullables);

        Assert.Equal(expected, actual, ignoreLineEndingDifferences: true);
    }
}