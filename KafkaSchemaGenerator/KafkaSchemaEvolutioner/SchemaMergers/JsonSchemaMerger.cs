using Newtonsoft.Json.Linq;

namespace KafkaSchemaEvolutioner.SchemaMergers;

public static class JsonSchemaMerger
{
    /// <summary>
    /// Merge new JSON Schema against old one with BACKWARD-like rules,
    /// recursively applying to definitions.
    /// - Removed properties are allowed.
    /// - New properties become optional with default=null if needed.
    /// - Existing properties are checked for tightening constraints (maxLength, enum).
    /// </summary>
    public static JObject MergeSchemas(JObject oldSchema, JObject newSchema)
    {
        ArgumentNullException.ThrowIfNull(oldSchema);
        ArgumentNullException.ThrowIfNull(newSchema);

        // Merge properties at top level
        MergePropertiesSchema(oldSchema, newSchema);

        // Recurse into definitions
        if (newSchema.TryGetValue("definitions", out var defs) && defs is JObject newDefs)
        {
            var oldDefs = oldSchema["definitions"] as JObject ?? new JObject();
            foreach (var nd in newDefs.Properties())
            {
                if (oldDefs.TryGetValue(nd.Name, out var od))
                {
                    var oldDef = od as JObject ?? throw new InvalidOperationException("Definition is not an JObject");
                    var newDef = nd.Value as JObject ?? throw new InvalidOperationException("Definition is not an JObject");
                    
                    if (newDef.ContainsKey("properties") && oldDef.ContainsKey("properties"))
                    {
                        MergePropertiesSchema(oldDef, newDef);
                    } else if (newDef.ContainsKey("allOf") && oldDef.ContainsKey("allOf"))
                    {
                        MergePropertiesInAllOf(oldDef, newDef);
                    } else
                    {
                        throw new InvalidOperationException("Schema mismatch. Dont know how to merge this.");
                    }
                }
                
            }
        }

        return newSchema;
    }

    private static void MergePropertiesSchema(JObject oldPropsContainer, JObject newPropsContainer)
    {
        var oldProps = oldPropsContainer["properties"] as JObject ?? new JObject();
        var newProps = newPropsContainer["properties"] as JObject ?? new JObject();


        foreach (var np in newProps.Properties())
        {
            var propObj = np.Value as JObject ?? throw new InvalidOperationException("Prop is null");

            if (!oldProps.ContainsKey(np.Name))
            {
                // New property → optional, default=null, type=["null", originalType]
                EnsureNotInRequired(newPropsContainer, np.Name);
                EnsureDefaultNull(propObj);
                EnsureTypeHasNull(propObj);
            }
            else
            {
                // Existing property → check constraints
                var oldPropObj = oldProps[np.Name] as JObject;
                if (oldPropObj != null && propObj != null)
                {
                    CheckConstraints(oldPropObj, propObj, np.Name);
                }
            }

            // Recurse into nested objects
            if (propObj.TryGetValue("properties", out var nestedProps) && nestedProps is JObject)
            {
                var oldNestedProps = oldProps.ContainsKey(np.Name) ? oldProps[np.Name] as JObject ?? new JObject() : new JObject();
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
}
