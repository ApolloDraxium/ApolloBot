using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

public sealed class LocalizationManager
{
    public const string DefaultLanguage = "en-GB";
    private readonly string _directory;
    private readonly Dictionary<string, Dictionary<string, string>> _languages = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _completeLanguages = new(StringComparer.OrdinalIgnoreCase);
    public LocalizationManager(string directory) { _directory = directory; }
    public IReadOnlyCollection<string> AvailableLanguages => _completeLanguages;

    public void Load()
    {
        _languages.Clear(); _completeLanguages.Clear(); Directory.CreateDirectory(_directory);
        foreach (string file in Directory.EnumerateFiles(_directory, "*.json", SearchOption.AllDirectories))
        {
            string language = Path.GetFileNameWithoutExtension(file);
            try
            {
                using JsonDocument document = JsonDocument.Parse(File.ReadAllText(file));
                var strings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                Flatten(document.RootElement, "", strings); _languages[language] = strings;
            }
            catch (Exception ex) { Console.WriteLine($"[LOCALIZATION] Failed to load {file}: {ex.Message}"); }
        }
        if (!_languages.ContainsKey(DefaultLanguage))
            throw new InvalidOperationException($"Required fallback language '{DefaultLanguage}' was not found in '{_directory}'.");
        ValidateAgainstDefault();
    }

    public string Get(string key, string? language = null, params object?[] args)
    {
        string requestedLanguage = NormalizeLanguage(language);
        if (!TryGetRaw(requestedLanguage, key, out string? value) && !TryGetRaw(DefaultLanguage, key, out value))
        { Console.WriteLine($"[LOCALIZATION] Missing key in fallback language: {key}"); return key; }
        if (args.Length == 0) return value!;
        try { return string.Format(GetCulture(requestedLanguage), value!, args); }
        catch (FormatException ex) { Console.WriteLine($"[LOCALIZATION] Invalid format string '{key}' ({requestedLanguage}): {ex.Message}"); return value!; }
    }

    public bool IsSupported(string? language) => !string.IsNullOrWhiteSpace(language) && _completeLanguages.Contains(language.Trim());
    public string NormalizeLanguage(string? language)
    {
        if (string.IsNullOrWhiteSpace(language)) return DefaultLanguage;
        string requested = language.Trim();
        if (_completeLanguages.Contains(requested)) return _completeLanguages.First(x => x.Equals(requested, StringComparison.OrdinalIgnoreCase));
        string neutral = requested.Split('-', '_')[0];
        string? regionalMatch = _completeLanguages.FirstOrDefault(x => x.Equals(neutral, StringComparison.OrdinalIgnoreCase) || x.StartsWith(neutral + "-", StringComparison.OrdinalIgnoreCase));
        return regionalMatch ?? DefaultLanguage;
    }
    private bool TryGetRaw(string language, string key, out string? value) { value=null; return _languages.TryGetValue(language,out var strings)&&strings.TryGetValue(key,out value); }
    private static CultureInfo GetCulture(string language) { try{return CultureInfo.GetCultureInfo(language);}catch(CultureNotFoundException){return CultureInfo.InvariantCulture;} }
    private static void Flatten(JsonElement element,string prefix,Dictionary<string,string> destination)
    {
        if(element.ValueKind!=JsonValueKind.Object) throw new InvalidDataException("Localization root must be a JSON object.");
        foreach(JsonProperty property in element.EnumerateObject())
        {
            string key=string.IsNullOrEmpty(prefix)?property.Name:$"{prefix}.{property.Name}";
            if(property.Value.ValueKind==JsonValueKind.Object){Flatten(property.Value,key,destination);continue;}
            if(property.Value.ValueKind!=JsonValueKind.String) throw new InvalidDataException($"Localization value '{key}' must be a string.");
            destination[key]=property.Value.GetString()??"";
        }
    }
    private static HashSet<string> GetFormatTokens(string value) =>
        Regex.Matches(value, @"\{\d+(?:,[^}:]+)?(?:[^}]*)\}")
            .Select(match => Regex.Match(match.Value, @"\{\d+").Value)
            .ToHashSet(StringComparer.Ordinal);

    private void ValidateAgainstDefault()
    {
        Dictionary<string,string> fallback=_languages[DefaultLanguage]; _completeLanguages.Add(DefaultLanguage);
        Console.WriteLine($"[LOCALIZATION] {DefaultLanguage}: {fallback.Count}/{fallback.Count} ✓");
        foreach((string language,Dictionary<string,string> strings) in _languages.OrderBy(x=>x.Key))
        {
            if(language.Equals(DefaultLanguage,StringComparison.OrdinalIgnoreCase)) continue;
            string[] missing=fallback.Keys.Where(key=>!strings.ContainsKey(key)).OrderBy(x=>x).ToArray();
            string[] extra=strings.Keys.Where(key=>!fallback.ContainsKey(key)).OrderBy(x=>x).ToArray();
            string[] badFormats=fallback.Keys.Where(strings.ContainsKey)
                .Where(key => !GetFormatTokens(fallback[key]).SetEquals(GetFormatTokens(strings[key])))
                .OrderBy(x=>x).ToArray();
            if(missing.Length==0 && badFormats.Length==0){_completeLanguages.Add(language);Console.WriteLine($"[LOCALIZATION] {language}: {fallback.Count}/{fallback.Count} ✓");}
            else
            {
                Console.WriteLine($"[LOCALIZATION] {language}: {fallback.Count-missing.Length}/{fallback.Count} - INCOMPLETE (not advertised)");
                foreach(string key in missing.Take(50)) Console.WriteLine($"[LOCALIZATION]   missing: {key}");
                if(missing.Length>50) Console.WriteLine($"[LOCALIZATION]   ...and {missing.Length-50} more missing keys");
                foreach(string key in badFormats.Take(50)) Console.WriteLine($"[LOCALIZATION]   placeholder mismatch: {key}");
                if(badFormats.Length>50) Console.WriteLine($"[LOCALIZATION]   ...and {badFormats.Length-50} more placeholder mismatches");
            }
            foreach(string key in extra.Take(20)) Console.WriteLine($"[LOCALIZATION]   extra: {key}");
        }
    }
}
