using System.Globalization;
using System.Text.Json;

public sealed class LocalizationManager
{
    public const string DefaultLanguage = "en-GB";

    private readonly string _directory;
    private readonly Dictionary<string, Dictionary<string, string>> _languages =
        new(StringComparer.OrdinalIgnoreCase);

    public LocalizationManager(string directory)
    {
        _directory = directory;
    }

    public IReadOnlyCollection<string> AvailableLanguages => _languages.Keys;

    public void Load()
    {
        _languages.Clear();
        Directory.CreateDirectory(_directory);

        foreach (string file in Directory.EnumerateFiles(_directory, "*.json", SearchOption.AllDirectories))
        {
            string language = Path.GetFileNameWithoutExtension(file);

            try
            {
                using JsonDocument document = JsonDocument.Parse(File.ReadAllText(file));
                var strings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                Flatten(document.RootElement, "", strings);
                _languages[language] = strings;
                Console.WriteLine($"[LOCALIZATION] Loaded {language}: {strings.Count} strings");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[LOCALIZATION] Failed to load {file}: {ex.Message}");
            }
        }

        if (!_languages.ContainsKey(DefaultLanguage))
            throw new InvalidOperationException($"Required fallback language '{DefaultLanguage}' was not found in '{_directory}'.");

        ValidateAgainstDefault();
    }

    public string Get(string key, string? language = null, params object?[] args)
    {
        string requestedLanguage = NormalizeLanguage(language);

        if (!TryGetRaw(requestedLanguage, key, out string? value) &&
            !TryGetRaw(DefaultLanguage, key, out value))
        {
            Console.WriteLine($"[LOCALIZATION] Missing key in fallback language: {key}");
            return key;
        }

        if (args.Length == 0)
            return value!;

        try
        {
            return string.Format(GetCulture(requestedLanguage), value!, args);
        }
        catch (FormatException ex)
        {
            Console.WriteLine($"[LOCALIZATION] Invalid format string '{key}' ({requestedLanguage}): {ex.Message}");
            return value!;
        }
    }

    public bool IsSupported(string? language) =>
        !string.IsNullOrWhiteSpace(language) && _languages.ContainsKey(language.Trim());

    public string NormalizeLanguage(string? language)
    {
        if (string.IsNullOrWhiteSpace(language))
            return DefaultLanguage;

        string requested = language.Trim();
        if (_languages.ContainsKey(requested))
            return _languages.Keys.First(x => x.Equals(requested, StringComparison.OrdinalIgnoreCase));

        string neutral = requested.Split('-', '_')[0];
        string? regionalMatch = _languages.Keys.FirstOrDefault(x =>
            x.Equals(neutral, StringComparison.OrdinalIgnoreCase) ||
            x.StartsWith(neutral + "-", StringComparison.OrdinalIgnoreCase));

        return regionalMatch ?? DefaultLanguage;
    }

    private bool TryGetRaw(string language, string key, out string? value)
    {
        value = null;
        return _languages.TryGetValue(language, out Dictionary<string, string>? strings) &&
               strings.TryGetValue(key, out value);
    }

    private static CultureInfo GetCulture(string language)
    {
        try { return CultureInfo.GetCultureInfo(language); }
        catch (CultureNotFoundException) { return CultureInfo.InvariantCulture; }
    }

    private static void Flatten(JsonElement element, string prefix, Dictionary<string, string> destination)
    {
        if (element.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException("Localization root must be a JSON object.");

        foreach (JsonProperty property in element.EnumerateObject())
        {
            string key = string.IsNullOrEmpty(prefix) ? property.Name : $"{prefix}.{property.Name}";

            if (property.Value.ValueKind == JsonValueKind.Object)
            {
                Flatten(property.Value, key, destination);
                continue;
            }

            if (property.Value.ValueKind != JsonValueKind.String)
                throw new InvalidDataException($"Localization value '{key}' must be a string.");

            destination[key] = property.Value.GetString() ?? "";
        }
    }

    private void ValidateAgainstDefault()
    {
        Dictionary<string, string> fallback = _languages[DefaultLanguage];

        foreach ((string language, Dictionary<string, string> strings) in _languages)
        {
            if (language.Equals(DefaultLanguage, StringComparison.OrdinalIgnoreCase))
                continue;

            string[] missing = fallback.Keys.Where(key => !strings.ContainsKey(key)).OrderBy(x => x).ToArray();
            if (missing.Length == 0)
            {
                Console.WriteLine($"[LOCALIZATION] {language}: complete");
                continue;
            }

            Console.WriteLine($"[LOCALIZATION] {language} missing {missing.Length} key(s); falling back to {DefaultLanguage}.");
            foreach (string key in missing.Take(20))
                Console.WriteLine($"[LOCALIZATION]   missing: {key}");
            if (missing.Length > 20)
                Console.WriteLine($"[LOCALIZATION]   ...and {missing.Length - 20} more");
        }
    }
}
