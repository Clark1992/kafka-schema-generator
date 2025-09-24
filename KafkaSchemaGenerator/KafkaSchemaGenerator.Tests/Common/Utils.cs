namespace KafkaSchemaGenerator.Tests.Common;

public class Utils
{
    public static Dictionary<string, string> LoadFilesFromDirectory(string directoryPath)
    {
        if (!Directory.Exists(directoryPath))
            throw new DirectoryNotFoundException($"Directory not found: {directoryPath}");

        var result = new Dictionary<string, string>();

        foreach (var filePath in Directory.GetFiles(directoryPath))
        {
            string fileName = Path.GetFileName(filePath);
            string content = File.ReadAllText(filePath);

            result[fileName] = content;
        }

        return result;
    }

    public static void AssertFileName(string fileName, string typeName, string prefix, string suffix, string ext) =>
        Assert.Equal(fileName, $"{prefix}{typeName}{suffix}.{ext}");
}
