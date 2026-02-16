using KafkaSchemaGenerator.Common.Utils;
using System.Text;
using System.Text.RegularExpressions;

namespace KafkaSchemaGenerator.Common.Utils;

public static partial class StringExtensions
{
    public static string Select(this string input, Func<string, string> selector) => selector(input);

    public static string AddOptionals(this string schema, Dictionary<string, HashSet<NameNumber>> nullables) => 
        schema.ProcessSchemaLines(
            (currentMessage, nn, line) =>
                ShouldBeOptional(nullables, currentMessage, nn.Name, nn.Number) && !IsOptionalRegex().IsMatch(line),
            line => Regex.Replace(line, @"^(\s*)", "$1optional "));

    private static string ProcessSchemaLines(
        this string schema,
        Func<string, NameNumber, string, bool> predicate,
        Func<string, string> replaceFunc = null)
    {
        var output = new List<string>();

        WalkProtoSchema(
            schema,
            onLine: output.Add,
            onMessageEnter: _ => { },
            onMessageExit: () => { },
            onField: (msg, field, line) =>
            predicate(msg, field, line)
                ? replaceFunc?.Invoke(line)
                : line);

        return string.Join("\n", output);
    }

    public static void GetOptionals(this string protoText, Dictionary<string, HashSet<NameNumber>> result) =>
        protoText.ProcessSchemaLines(
             (_, _, line) => IsOptionalRegex().IsMatch(line),
             (msg, nameNum) =>
             {
                 if (!result.TryGetValue(msg, out var set))
                 {
                     set = [];
                     result[msg] = set;
                 }

                 set.Add(nameNum);
             });

    private static void ProcessSchemaLines(
        this string schema,
        Func<string, NameNumber, string, bool> predicate,
        Action<string, NameNumber> action = null)
    {
        WalkProtoSchema(
            schema,
            onLine: _ => { },
            onMessageEnter: _ => { },
            onMessageExit: () => { },
            onField: (msg, field, line) =>
            {
                if (predicate(msg, field, line))
                {
                    action?.Invoke(msg, field);
                }

                return line;
            });
    }

    private static void WalkProtoSchema(
        string schema,
        Action<string> onLine,
        Action<string> onMessageEnter,
        Action onMessageExit,
        Func<string, NameNumber, string, string> onField)
    {
        var lines = schema.GetLines();

        string currentMessage = null;
        bool insideMessage = false;

        var messageRegex = new Regex(@"^\s*message\s+(\w+)");
        var fieldRegex = new Regex(
            @"^\s*(?:optional\s+)?([\w\.]+)\s+(\w+)\s*=\s*(\d+)(.*)$",
            RegexOptions.Compiled);

        foreach (var rawLine in lines)
        {
            var line = rawLine;

            var messageMatch = messageRegex.Match(line);
            if (messageMatch.Success)
            {
                currentMessage = messageMatch.Groups[1].Value.WithPath(currentMessage);
                insideMessage = true;
                onMessageEnter?.Invoke(currentMessage);
                onLine?.Invoke(line);
                continue;
            }

            if (insideMessage && line.TrimStart().StartsWith("}"))
            {
                onMessageExit?.Invoke();
                currentMessage = currentMessage.PopPath();
                insideMessage = currentMessage != null;
                onLine?.Invoke(line);
                continue;
            }

            if (insideMessage && currentMessage != null)
            {
                var fieldMatch = fieldRegex.Match(line);
                if (fieldMatch.Success)
                {
                    var fieldName = fieldMatch.Groups[2].Value;
                    var fieldNumber = int.Parse(fieldMatch.Groups[3].Value);

                    line = onField?.Invoke(
                        currentMessage,
                        new NameNumber(fieldName, fieldNumber),
                        line
                    ) ?? line;
                }
            }

            onLine?.Invoke(line);
        }
    }

    public static string WithPath(this string messageName, string path) => path is null ? messageName : $"{path}:{messageName}";

    public static string PopPath(this string messagePath) => messagePath is not null && messagePath.Contains(':') ? messagePath[..messagePath.LastIndexOf(':')] : null;

    private static string[] GetLines(this string schema) => schema.Split(["\r\n", "\r", "\n"], StringSplitOptions.None);

    private static bool  ShouldBeOptional(
        Dictionary<string, HashSet<NameNumber>> nullables,
        string message,
        string field,
        int number) =>
        nullables.TryGetValue(message, out var fields) && fields.Contains(new NameNumber(field, number));

    public static string ReInsertDeleted(this string newSchema, string oldSchema)
    {
        // 1. Extract header
        var headerMatch = Regex.Match(newSchema, @"^([\s\S]*?)(?=^message\s)", RegexOptions.Multiline);
        string header = headerMatch.Success ? headerMatch.Groups[1].Value.TrimEnd() + "\n\n" : "";

        // 2. Parse schemas
        var oldTree = ParseMessage(oldSchema, true);
        var newTree = ParseMessage(newSchema, false);

        // 3. Merge
        MergeMessages(oldTree, newTree);

        // 4. Build result
        var result = new StringBuilder();
        result.Append(header);

        foreach (var msg in newTree.Nested.Values)
        {
            result.AppendLine($"message {msg.Name} {{");
            result.Append(BuildSchema(msg, 1));
            result.AppendLine("}");
            result.AppendLine();
        }

        return result.ToString().TrimEnd();
    }

    static string BuildSchema(MessageNode node, int indent = 0)
    {
        var sb = new StringBuilder();
        string pad = new string(' ', indent * 4);

        foreach (var nested in node.Nested.Values)
        {
            sb.AppendLine($"{pad}message {nested.Name} {{");
            sb.Append(BuildSchema(nested, indent + 1));
            sb.AppendLine($"{pad}}}");
            sb.AppendLine();
        }

        foreach (var field in node.Fields.Values.OrderBy(f => f.Number))
        {
            string prefix = field.Optional ? "optional " :
                            field.Repeated ? "repeated " :
                            field.Required ? "required " : "";
            string suffix = field.IsFromPrevSchema ? " [deprecated = true]" : "";
            sb.AppendLine($"{pad}{prefix}{field.Type} {field.Name} = {field.Number}{suffix};");
        }

        return sb.ToString();
    }

    static void MergeMessages(MessageNode oldMsg, MessageNode newMsg)
    {
        // Get removed fields back
        foreach (var (name, field) in oldMsg.Fields)
        {
            if (!newMsg.Fields.ContainsKey(name))
                newMsg.Fields[name] = field;
        }

        // nested fields
        foreach (var (nestedName, oldNested) in oldMsg.Nested)
        {
            if (!newMsg.Nested.TryGetValue(nestedName, out var newNested))
            {
                newMsg.Nested[nestedName] = oldNested;
            }
            else
            {
                MergeMessages(oldNested, newNested);
            }
        }
    }

    static MessageNode ParseMessage(string schema, bool isPrevSchema)
    {
        var root = new MessageNode { Name = "root" };
        var stack = new Stack<MessageNode>();
        stack.Push(root);
        var enumDepth = 0;

        var lines = schema.GetLines()
            .Select(l => l.Trim())
            .Where(l => !string.IsNullOrWhiteSpace(l))
            .ToArray();

        var msgRegex = new Regex(@"^message\s+(\w+)\s*\{", RegexOptions.Compiled);
        var fieldRegex = new Regex(@"^(optional|required|repeated)?\s*([\.\w]+)\s+(\w+)\s*=\s*(\d+);", RegexOptions.Compiled);

        foreach (var line in lines)
        {
            if (msgRegex.IsMatch(line))
            {
                var msgName = msgRegex.Match(line).Groups[1].Value;
                var newNode = new MessageNode { Name = msgName };
                stack.Peek().Nested[msgName] = newNode;
                stack.Push(newNode);
                continue;
            }

            if (line.StartsWith("enum ", StringComparison.Ordinal))
            {
                enumDepth += line.Count(static c => c == '{');
            }

            if (line == "}")
            {
                if (enumDepth > 0)
                {
                    enumDepth--;
                    continue;
                }

                stack.Pop();
                continue;
            }

            if (fieldRegex.IsMatch(line))
            {
                var m = fieldRegex.Match(line);
                string modifier = m.Groups[1].Value;
                string type = m.Groups[2].Value;
                string name = m.Groups[3].Value;
                int number = int.Parse(m.Groups[4].Value);

                bool optional = modifier == "optional";
                bool repeated = modifier == "repeated";
                bool required = modifier == "required";

                stack.Peek().Fields[name] = new FieldInfo(type, name, number, optional, repeated, required, isPrevSchema);
            }
        }

        return root;
    }

    record FieldInfo(string Type, string Name, int Number, bool Optional, bool Repeated, bool Required, bool IsFromPrevSchema);

    class MessageNode
    {
        public string Name { get; set; } = "";
        public Dictionary<string, FieldInfo> Fields { get; } = [];
        public Dictionary<string, MessageNode> Nested { get; } = [];
    }

    [GeneratedRegex(@"\boptional\b")]
    private static partial Regex IsOptionalRegex();
}
