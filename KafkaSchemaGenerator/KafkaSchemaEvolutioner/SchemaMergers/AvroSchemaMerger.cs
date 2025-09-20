using Newtonsoft.Json.Linq;

namespace KafkaSchemaEvolutioner.SchemaMergers;

public static class AvroSchemaMerger
{
    public static JObject MergeSchemas(JObject oldSchema, JObject newSchema)
    {
        if (!IsRecord(oldSchema) || !IsRecord(newSchema))
            throw new InvalidOperationException("Expecting record types");

        return (JObject)MergeTypes(oldSchema, newSchema);
    }

    private static JToken MergeTypes(JToken oldSchema, JToken newSchema)
    {
        if (oldSchema == null) return newSchema;

        // --- record ---
        if (IsRecord(oldSchema) && IsRecord(newSchema))
        {
            var oldRec = oldSchema as JObject ?? throw new InvalidCastException("Expected JObject");
            var newRec = newSchema as JObject ?? throw new InvalidCastException("Expected JObject");

            var oldFields = oldRec["fields"].Cast<JObject>()
                .ToDictionary(f => f["name"]!.ToString());

            foreach (var nf in newRec["fields"].Cast<JObject>())
            {
                var name = nf["name"]!.ToString();

                if (oldFields.TryGetValue(name, out var of))
                {
                    nf["type"] = MergeTypes(of["type"], nf["type"]);
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
            $"Incompatible types: old={oldSchema} new={newSchema}");
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
            if (t.Type == JTokenType.Array) return string.Join("|", ((JArray)t).Select(Norm));
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
}