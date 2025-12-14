using KafkaSchemaGenerator;
using Newtonsoft.Json.Linq;

namespace KafkaSchemaEvolutioner.SchemaMergers;

public class JsonSchemaMerger: ISchemaMerger
{
    public bool AppliesTo(Format format) => format == Format.JSON;

    /// <summary>
    /// Merge new JSON Schema against old one with BACKWARD-like rules,
    /// recursively applying to definitions.
    /// - Removed properties are allowed.
    /// - New properties become optional with default=null if needed.
    /// - Existing properties are checked for tightening constraints (maxLength, enum).
    /// </summary>
    public string MergeSchemas(string oldSchemaText, string newSchemaText)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(oldSchemaText);
        ArgumentException.ThrowIfNullOrWhiteSpace(newSchemaText);

        JObject oldSchema = JObject.Parse(oldSchemaText);
        JObject newSchema = JObject.Parse(newSchemaText);

        // Merge properties at top level
        MergePropertiesSchema(oldSchema, newSchema);

        // Recurse into definitions
        if (newSchema.TryGetValue("definitions", out var defs) && defs is JObject newDefs)
        {
            var oldDefs = oldSchema["definitions"] as JObject ?? [];
            foreach (var nd in newDefs.Properties())
            {
                if (oldDefs.TryGetValue(nd.Name, out var od))
                {
                    var oldDef = od as JObject ?? throw new InvalidOperationException("Definition is not an JObject");
                    var newDef = nd.Value as JObject ?? throw new InvalidOperationException("Definition is not an JObject");
                    
                    if (newDef.ContainsKey("properties") && oldDef.ContainsKey("properties"))
                    {
                        MergePropertiesSchema(oldDef, newDef);
                    } 
                    if (newDef.ContainsKey("allOf") && oldDef.ContainsKey("allOf"))
                    {
                        MergePropertiesInAllOf(oldDef, newDef);
                    }
                }
                
            }
        }

        return newSchema.ToString();
    }

    private static void MergePropertiesSchema(JObject oldPropsContainer, JObject newPropsContainer)
    {
        var oldProps = oldPropsContainer["properties"] as JObject ?? [];
        var newProps = newPropsContainer["properties"] as JObject ?? [];

        foreach (var np in newProps.Properties())
        {
            var propObj = np.Value as JObject ?? throw new InvalidOperationException("Prop is null");

            if (!oldProps.ContainsKey(np.Name))
            {
                // New property → optional, default=null, type=["null", originalType]
                MakeNullable(newPropsContainer, np.Name, propObj);
            }
            else
            {
                // Existing property → check constraints
                if (oldProps[np.Name] is JObject oldPropObj && propObj != null)
                {
                    AreJsonTypesCompatible(oldPropObj, propObj, np.Name);
                    CheckConstraints(oldPropObj, propObj, np.Name);

                    MergeNullablesIfNeeded(newPropsContainer, np.Name, oldPropObj, propObj);
                }
            }

            // Recurse into nested objects
            if (propObj.TryGetValue("properties", out var nestedProps) && nestedProps is JObject)
            {
                var oldNestedProps = oldProps.ContainsKey(np.Name) ? oldProps[np.Name] as JObject ?? [] : [];
                MergePropertiesSchema(oldNestedProps, propObj);
            }
        }
    }

    private static void EnsureTypeHasNull(JObject propObj)
    {
        var originalNewPropType = propObj["type"];
        if (originalNewPropType != null)
        {
            if (originalNewPropType.Type != JTokenType.Array || !originalNewPropType.Any(t => t.ToString() == "null"))
                propObj["type"] = new JArray("null", originalNewPropType);
        }
        // If "type" is missing but "oneOf" exists, ensure oneOf contains null
        else if (propObj.TryGetValue("oneOf", out var oneOfToken) && oneOfToken is JArray oneOfArray)
        {
            // Check if null is already present
            if (!oneOfArray.Any(t => t["type"]?.ToString() == "null"))
            {
                oneOfArray.Insert(0, new JObject { ["type"] = "null" });
            }
        }
    }

    private static void EnsureDefaultNull(JObject propObj)
    {
        if (propObj["default"] == null)
            propObj["default"] = JValue.CreateNull();
    }

    private static void EnsureNotInRequired(JObject newPropsContainer, string name)
    {
        if (!newPropsContainer.ContainsKey("required") || newPropsContainer["required"] is not JArray newRequired)
        {
            return;
        }

        var newRequiredSet = new HashSet<string>(newRequired.Select(x => x.ToString()));
        if (!newRequiredSet.Contains(name))
        {
            return;
        }

        newRequiredSet.Remove(name);
        newPropsContainer["required"] = new JArray(newRequiredSet);
    }

    private static void MergePropertiesInAllOf(JObject oldObj, JObject newObj)
    {
        if (!newObj.TryGetValue("allOf", out var allOfToken) || allOfToken is not JArray allOfArray)
        {
            return;
        }

        if (!oldObj.TryGetValue("allOf", out var oldAllOfToken) || oldAllOfToken is not JArray oldAllOfArray)
        {
            throw new InvalidOperationException("Expected allOf in old schema.");
        }

        var newAllOfItem = allOfArray.OfType<JObject>().Single(x => x["type"]?.ToString() == "object");

        var oldAllOfItem = oldAllOfArray.OfType<JObject>().Single(x => x["type"]?.ToString() == "object");

        MergePropertiesSchema(oldAllOfItem, newAllOfItem);
    }

    private static void CheckConstraints(JObject oldProp, JObject newProp, string propName)
    {
        if (oldProp.TryGetValue("maxLength", out var oldMax) &&
            newProp.TryGetValue("maxLength", out var newMax))
        {
            if (newMax.Value<int>() < oldMax.Value<int>())
                throw new InvalidOperationException($"Property '{propName}' maxLength tightened.");
        }

        if (oldProp.TryGetValue("enum", out var oldEnum) &&
            newProp.TryGetValue("enum", out var newEnum))
        {
            var oldVals = oldEnum.Select(x => x.ToString()).ToHashSet();
            var newVals = newEnum.Select(x => x.ToString()).ToHashSet();
            if (!oldVals.IsSubsetOf(newVals))
                throw new InvalidOperationException($"Property '{propName}' enum values reduced.");
        }
    }

    private static bool AreJsonTypesCompatible(JToken oldSchema, JToken newSchema, string propName)
    {
        static HashSet<string> ExtractTypes(JToken schema)
        {
            var result = new HashSet<string>();

            void Visit(JToken t)
            {
                if (t == null) return;

                // type: "string" | ["string","null"]
                if (t["type"] != null)
                {
                    var type = t["type"];

                    if (type.Type == JTokenType.String)
                    {
                        result.Add(type.ToString());
                    }
                    else if (type.Type == JTokenType.Array)
                    {
                        foreach (var x in (JArray)type)
                            result.Add(x.ToString());
                    }
                }

                // oneOf / anyOf
                if (t["oneOf"] is JArray oneOf)
                    foreach (var s in oneOf) Visit(s);

                if (t["anyOf"] is JArray anyOf)
                    foreach (var s in anyOf) Visit(s);
            }

            Visit(schema);
            return result;
        }

        var oldTypes = ExtractTypes(oldSchema);
        var newTypes = ExtractTypes(newSchema);

        oldTypes.Remove("null");
        newTypes.Remove("null");

        // strict equal
        if (oldTypes.SetEquals(newTypes))
            return true;

        // integer -> number (extension)
        if (oldTypes.SetEquals(new[] { "integer" }) &&
            newTypes.SetEquals(new[] { "number" }))
            return true;

        throw new InvalidOperationException($"Property '{propName}' enum values reduced.");
    }

    private static void MakeNullable(JObject newPropsContainer, string name, JObject propObj)
    {
        EnsureNotInRequired(newPropsContainer, name);
        EnsureDefaultNull(propObj);
        EnsureTypeHasNull(propObj);
    }

    private static bool IsNullable(JToken field)
    {
        if (field == null)
            return false;

        // type: ["null", ...]
        if (field["type"] is JToken type)
        {
            if (type.Type == JTokenType.String)
            {
                if (type.ToString() == "null")
                    return true;
            }
            else if (type.Type == JTokenType.Array)
            {
                if (((JArray)type).Any(t => t.ToString() == "null"))
                    return true;
            }
        }

        // oneOf / anyOf
        if (field["oneOf"] is JArray oneOf)
        {
            if (oneOf.Any(IsNullable))
                return true;
        }

        if (field["anyOf"] is JArray anyOf)
        {
            if (anyOf.Any(IsNullable))
                return true;
        }

        return false;
    }

    private static void MergeNullablesIfNeeded(JObject newPropsContainer, string name, JToken oldField, JToken newField)
    {
        var oldNullable = IsNullable(oldField);
        var newNullable = IsNullable(newField);

        if (oldNullable == newNullable)
            return;

        if (oldNullable && !newNullable)
        {
            MakeNullable(newPropsContainer, name, newField as JObject);
        }
    }
}
