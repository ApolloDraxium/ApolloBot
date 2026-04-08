// ApolloBot Persistence Fix Version
// Adds proper Railway volume detection + logging + uptime persistence fixes

using System;
using System.IO;
using System.Linq;

public static class DataPathHelper
{
    public static string GetDataPath()
    {
        return Environment.GetEnvironmentVariable("APP_DATA_PATH")
            ?? Environment.GetEnvironmentVariable("RAILWAY_VOLUME_MOUNT_PATH")
            ?? "/app/data";
    }

    public static void EnsureAndLog()
    {
        var path = GetDataPath();
        Directory.CreateDirectory(path);

        Console.WriteLine($"[DATA] APP_DATA_PATH = {Environment.GetEnvironmentVariable("APP_DATA_PATH") ?? "(null)"}");
        Console.WriteLine($"[DATA] RAILWAY_VOLUME_MOUNT_PATH = {Environment.GetEnvironmentVariable("RAILWAY_VOLUME_MOUNT_PATH") ?? "(null)"}");
        Console.WriteLine($"[DATA] Using path: {path}");
        Console.WriteLine($"[DATA] Exists: {Directory.Exists(path)}");

        try
        {
            var testFile = Path.Combine(path, "volume_test.txt");
            File.WriteAllText(testFile, $"Test write at {DateTime.UtcNow:O}");
            Console.WriteLine($"[DATA] Test write success: {testFile}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DATA] Test write FAILED: {ex}");
        }

        var files = Directory.GetFiles(path);
        Console.WriteLine($"[DATA] Files: {string.Join(", ", files.Select(Path.GetFileName))}");
    }
}
