using ProtoBuf;
using ProtoBuf.Meta;
using System.Text.RegularExpressions;
using KafkaSchemaGenerator.Common.Utils;

namespace KafkaSchemaGenerator.Generators;

public class ProtoSchemaGenerator : ISchemaGenerator
{
    private Dictionary<string, HashSet<NameNumber>> nullables = new();

    public bool AppliesTo(Format format) => format is Format.PROTO;

    public string GenerateSchema(Type type)
    {
        var options = new SchemaGenerationOptions
        {
            Syntax = ProtoSyntax.Proto3,
            Package = $"{type.Namespace}.Generated",
        };

        options.Types.Add(type);

        var schema = RuntimeTypeModel.Default.GetSchema(options);

        var allTypes = RuntimeTypeModel.Default.GetTypes();

        foreach (var typeFromSchema in allTypes)
        {
            if (typeFromSchema is not MetaType meta) continue;

            AnalyzeMetaType(meta);
        }

        schema = schema
            .AddOptionals(nullables);
            //.Select(RemoveImports);
            //.Select(PrependProtoBufNetDefaultTypes);

        return schema;
    }

    private void AnalyzeMetaType(MetaType meta)
    {
        var fields = meta.GetFields();
        for (int i = 0; i < fields.Length; i++)
        {
            var field = fields[i];
            var isNullable = IsNullable(field.MemberType);
            var isNotRequired = GetIsRequiredValue(field) == false;

            if (isNullable || isNotRequired)
            {
                var name = field.Member.Name;
                var parent = field.ParentType.Name;
                var index = field.FieldNumber;

                if (!nullables.TryGetValue(parent, out HashSet<NameNumber> value))
                {
                    value = [];
                    nullables[parent] = value;
                }

                value.Add(new NameNumber(name, index));
            }
        }
    }

    private static bool IsNullable(Type type) => Nullable.GetUnderlyingType(type) != null;

    private static bool? GetIsRequiredValue(ValueMember field)
    {
        var att = field.Member.CustomAttributes.Where(x => x.AttributeType == typeof(ProtoMemberAttribute)).FirstOrDefault();

        if (att is null)
            return null;

        var args = att.NamedArguments.Where(na => na.MemberName == nameof(ProtoMemberAttribute.IsRequired));

        if (!args.Any())
            return null;

        var arg = args.FirstOrDefault();
        var value = arg.TypedValue.Value as bool?;

        return value;
    }

    private static string PrependProtoBufNetDefaultTypes(string schema)
    {
        var contents = File.ReadAllText("protobuf-net/bcl.proto");

        return $"//--------------- DEFAULT PROTOBUF-NET TYPES ---------------\n\n{contents}\n\n//--------------- ACTUAL SCHEMA ---------------\n\n{schema}";
    }

    private static string RemoveImports(string schema)
    {
        var regex = new Regex(@"^\s*import\b", RegexOptions.IgnoreCase);
        var lines = schema.Split(["\r\n", "\n"], StringSplitOptions.None);
        var filtered = lines.Where(line => !regex.IsMatch(line));
        return string.Join("\n", filtered);
    }
}
