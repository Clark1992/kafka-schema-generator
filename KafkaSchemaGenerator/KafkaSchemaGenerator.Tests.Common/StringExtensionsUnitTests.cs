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
        public void ReInsertDeleted_NameRemoved_ShouldKeepEnumBlocks()
        {
                const string oldSchema = """
                        syntax = "proto3";

                        message Sample {
                            string Name = 1;
                            SampleStatus Status = 2;
                        }

                        message Sample2 {
                            string Name2 = 1;
                        }

                        enum SampleStatus {
                            Unknown = 0;
                            Active = 1;
                        }
                        """;

                const string newSchema = """
                        syntax = "proto3";

                        message Sample {
                            SampleStatus Status = 2;
                        }

                        enum SampleStatus {
                            Unknown = 0;
                            Active = 1;
                        }
                        """;

                var actual = newSchema.ReInsertDeleted(oldSchema);

                Assert.Equal("""
                        syntax = "proto3";

                        message Sample {
                            string Name = 1 [deprecated = true];
                            SampleStatus Status = 2;
                        }

                        enum SampleStatus {
                            Unknown = 0;
                            Active = 1;
                        }
                        """, actual, ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void ReInsertDeleted_EnumRemoved_ShouldKeepEnumBlocks()
    {
        const string oldSchema = """
                        syntax = "proto3";

                        message Sample {
                            string Name = 1;
                            SampleStatus Status = 2;
                        }

                        message Sample2 {
                            string Name2 = 1;
                        }

                        enum SampleStatus {
                            Unknown = 0;
                            Active = 1;
                        }
                        """;

        const string newSchema = """
                        syntax = "proto3";

                        message Sample {
                            string Name = 1;
                        }
                        """;

        var actual = newSchema.ReInsertDeleted(oldSchema);

        Assert.Equal("""
                        syntax = "proto3";

                        message Sample {
                            string Name = 1;
                            SampleStatus Status = 2 [deprecated = true];
                        }

                        enum SampleStatus {
                            Unknown = 0;
                            Active = 1;
                        }
                        """, actual, ignoreLineEndingDifferences: true);
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