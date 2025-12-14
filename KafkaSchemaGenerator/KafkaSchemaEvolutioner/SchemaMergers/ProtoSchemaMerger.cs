extern alias ProtobufNetReflection;

using KafkaSchemaGenerator;
using KafkaSchemaGenerator.Common.Utils;
using Microsoft.Extensions.Logging;
using System;
using ProtobufNet = ProtobufNetReflection::Google.Protobuf.Reflection;

namespace KafkaSchemaEvolutioner.SchemaMergers;

public class ProtoSchemaMerger(ILogger<ProtoSchemaMerger> logger) : ISchemaMerger
{
    public bool AppliesTo(Format format) => format == Format.PROTO;

    public string MergeSchemas(string oldProtoText, string newProtoText)
    {
        var oldFile = ParseFile(oldProtoText);
        var newFile = ParseFile(newProtoText);

        var newFields = new Dictionary<string, HashSet<NameNumber>>();
        var removedFields = new Dictionary<string, HashSet<NameNumber>>();
        var oldOptionalFields = new Dictionary<string, HashSet<NameNumber>>();

        foreach (var oldMsg in oldFile.MessageTypes)
        {
            var newMsg = newFile.MessageTypes.FirstOrDefault(m => m.Name == oldMsg.Name);
            if (newMsg is null)
            {
                logger.LogInformation("Message '{oldMsg.Name}' missing in new schema. Continuing...", oldMsg.Name);
                continue;
            }

            DetectNewAndRemovedFields(oldMsg, newMsg, newFields, removedFields);
        }

        oldProtoText.GetOptionals(oldOptionalFields);

        newProtoText = newProtoText
            .AddOptionals(newFields)
            .AddOptionals(oldOptionalFields)
            .ReInsertDeleted(oldProtoText);

        return newProtoText;
    }

    private static void DetectNewAndRemovedFields(
        ProtobufNet.DescriptorProto oldMsg,
        ProtobufNet.DescriptorProto newMsg,
        Dictionary<string, HashSet<NameNumber>> allNewFields,
        Dictionary<string, HashSet<NameNumber>> allRemovedFields,
        string parentPath = null)
    {
        var oldSchemaFields = oldMsg.Fields.ToDictionary(f => f.Number);
        var newSchemaFields = newMsg.Fields.ToDictionary(f => f.Number);

        var maxOldFieldNum = oldSchemaFields.Keys.Count != 0 ? oldSchemaFields.Keys.Max() : 0;

        var fullMsgPath = oldMsg.Name.WithPath(parentPath);

        // check matching & detect removed fields
        foreach (var oldField in oldSchemaFields.Values)
        {
            if (!newSchemaFields.TryGetValue(oldField.Number, out var newField))
            {
                AddToDict(allRemovedFields, oldField, fullMsgPath);
                continue;
            }

            if (oldField.TypeName != newField.TypeName)
                throw new InvalidOperationException(
                    $"Message '{fullMsgPath}': field #{oldField.Number} '{oldField.Name}' type changed from {oldField.TypeName}({oldField.type.ToString()}) to {newField.TypeName}({newField.type.ToString()})");

            if (oldField.label != newField.label)
                throw new InvalidOperationException(
                    $"Message '{fullMsgPath}': field #{oldField.Number} '{oldField.Name}' label changed from {oldField.label} to {newField.label}");
        }

        // New fields
        foreach (var newField in newSchemaFields.Values)
        {
            if (!oldSchemaFields.ContainsKey(newField.Number))
            {
                if (newField.Number <= maxOldFieldNum)
                    throw new InvalidOperationException(
                        $"Message '{fullMsgPath}': new field #{newField.Number} ('{newField.Name}') must have number greater than {maxOldFieldNum}");

                // Cant be optional if it is list (repeated)
                if (newField.label != ProtobufNet.FieldDescriptorProto.Label.LabelRepeated)
                {
                    AddToDict(allNewFields, newField, fullMsgPath);
                }
            }
        }

        // Nested types
        var oldNested = oldMsg.NestedTypes.ToDictionary(m => m.Name);
        foreach (var newNestedMsg in newMsg.NestedTypes)
        {
            if (oldNested.TryGetValue(newNestedMsg.Name, out var oldNestedMsg))
                DetectNewAndRemovedFields(oldNestedMsg, newNestedMsg, allNewFields, allRemovedFields, fullMsgPath);

            // TODO: cover case when old schema has nested type but new schema doesnt
        }
    }

    private static void AddToDict(
        Dictionary<string, HashSet<NameNumber>> dict,
        ProtobufNet.FieldDescriptorProto newField,
        string parentPath)
    {
        if (!dict.TryGetValue(parentPath, out HashSet<NameNumber> value))
        {
            value = [];
            dict[parentPath] = value;
        }

        value.Add(new NameNumber(newField.Name, newField.Number));
    }

    private static ProtobufNet.FileDescriptorProto ParseFile(string protoText)
    {
        var schema = "schema";
        var set = new ProtobufNet.FileDescriptorSet();
        using var reader = new StringReader(protoText);
        set.Add(schema, source: reader);
        set.Process();

        var errors = set.GetErrors().ToList();
        if (errors.Count > 0)
        {
            var msg = string.Join("\n", errors).Select(e => e.ToString());
            throw new InvalidOperationException($"Failed to parse .proto:\n{msg}");
        }

        return set.Files.FirstOrDefault(x => x.Name == schema);
    }
}
