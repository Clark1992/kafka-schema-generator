using KafkaSchemaGenerator;
using Newtonsoft.Json.Linq;

namespace KafkaSchemaEvolutioner.SchemaMergers;

public class AvroSchemaMerger: ISchemaMerger
{
    public bool AppliesTo(Format format) => format == Format.AVRO;

    public string MergeSchemas(string oldSchemaText, string newSchemaText)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(oldSchemaText);
        ArgumentException.ThrowIfNullOrWhiteSpace(newSchemaText);

        JObject oldSchema = JObject.Parse(oldSchemaText);
        JObject newSchema = JObject.Parse(newSchemaText);

        if (!IsRecord(oldSchema) || !IsRecord(newSchema))
            throw new InvalidOperationException("Expecting record types");

        return MergeTypes(oldSchema, newSchema).ToString();
    }

    private static JToken MergeTypes(JToken oldSchema, JToken newSchema)
    {
        if (oldSchema == null) return newSchema;

        // --- record ---
        if (IsRecord(oldSchema) && IsRecord(newSchema))
        {
            var oldRec = oldSchema as JObject ?? throw new InvalidCastException("AVRO: Expected JObject");
            var newRec = newSchema as JObject ?? throw new InvalidCastException("AVRO: Expected JObject");

            var oldFields = oldRec["fields"].Cast<JObject>()
                .ToDictionary(f => f["name"]!.ToString());

            foreach (var nf in newRec["fields"].Cast<JObject>())
            {
                var name = nf["name"]!.ToString();

                if (oldFields.TryGetValue(name, out var of))
                {
                    nf["type"] = MergeTypes(of["type"], nf["type"]);
                    MergeNullablesIfNeeded(of, nf);
                }
                else
                {
                    MakeNullable(nf);
                }
            }

            return newSchema;
        }

        // --- array ---
        if (IsArray(oldSchema) && IsArray(newSchema))
        {
            var oldItems = oldSchema["items"];
            var newItems = newSchema["items"];
            newSchema["items"] = MergeTypes(oldItems, newItems);
            return newSchema;
        }

        // --- map ---
        if (IsMap(oldSchema) && IsMap(newSchema))
        {
            var oldValues = oldSchema["values"];
            var newValues = newSchema["values"];
            newSchema["values"] = MergeTypes(oldValues, newValues);
            return newSchema;
        }

        // --- primitives / unions ---
        if (AreTypesCompatible(oldSchema, newSchema))
        {
            return newSchema;
        }

        throw new InvalidOperationException(
            $"AVRO: Incompatible types: old={oldSchema} new={newSchema}");
    }

    private static bool IsRecord(JToken t) =>
        t?.Type == JTokenType.Object && t["type"]?.ToString() == "record";

    private static bool IsArray(JToken t) =>
        t?.Type == JTokenType.Object && t["type"]?.ToString() == "array";

    private static bool IsMap(JToken t) =>
        t?.Type == JTokenType.Object && t["type"]?.ToString() == "map";

    private static void MakeNullable(JObject field)
    {
        var t = field["type"];
        if (t.Type == JTokenType.String)
        {
            field["type"] = new JArray("null", t);
        }
        else if (t.Type == JTokenType.Array)
        {
            var arr = (JArray)t;
            if (!arr.Any(x => x.ToString() == "null"))
                arr.Insert(0, "null");
            field["type"] = arr;
        }
        else if (t.Type == JTokenType.Object)
        {
            field["type"] = new JArray("null", t);
        }

        if (field["default"] == null)
        {
            field["default"] = JValue.CreateNull();
        }
    }

    private static bool AreTypesCompatible(JToken oldType, JToken newType)
    {
        static string Norm(JToken t)
        {
            if (t.Type == JTokenType.String) return t.ToString();
            if (t.Type == JTokenType.Object) return t["type"]?.ToString() ?? "";
            if (t.Type == JTokenType.Array) return string.Join("|", ((JArray)t)
                .Where(i => i.ToString() is not "null").Select(Norm).OrderBy(i => i));
            return t.ToString();
        }

        var o = Norm(oldType!);
        var n = Norm(newType!);

        if (o == n) return true;
        if (o == "int" && n == "long") return true;
        if (o == "float" && n == "double") return true;
        if (o.Contains("null") && n.Contains("null")) return true;

        return false;
    }

    private static bool IsNullable(JToken field)
    {
        var t = field["type"];
        if (t.Type == JTokenType.Array)
            return ((JArray)t).Any(x => x.ToString() == "null");

        return false;
    }

    private static void MergeNullablesIfNeeded(JToken oldField, JToken newField)
    {
        var oldNullable = IsNullable(oldField);
        var newNullable = IsNullable(newField);

        if (oldNullable == newNullable)
            return;

        if (oldNullable && !newNullable)
        {
            MakeNullable(newField as JObject);
        }
    }
}