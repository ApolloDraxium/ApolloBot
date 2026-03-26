using Google.Cloud.Translation.V2;

namespace ApolloBot.Services
{
    public class TranslationResultData
    {
        public string OriginalText { get; set; } = "";
        public string TranslatedText { get; set; } = "";
        public string DetectedSourceLanguage { get; set; } = "unknown";
        public string TargetLanguage { get; set; } = "en";
    }

    public class TranslationService
    {
        private TranslationClient? _client;

        public TranslationService()
        {
            try
            {
                var apiKey = Environment.GetEnvironmentVariable("GOOGLE_TRANSLATE_API_KEY");

                if (string.IsNullOrWhiteSpace(apiKey))
                {
                    Console.WriteLine("⚠️ GOOGLE_TRANSLATE_API_KEY is missing.");
                    return;
                }

                _client = new TranslationClientBuilder
                {
                    ApiKey = apiKey
                }.Build();

                Console.WriteLine("✅ Google Translate client initialized with API key.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️ Google Translate not configured yet: {ex}");
            }
        }

        public async Task<TranslationResultData?> TranslateAsync(string text, string targetLanguage)
        {
            if (_client == null || string.IsNullOrWhiteSpace(text))
                return null;

            try
            {
                return await Task.Run(() =>
                {
                    var detection = _client.DetectLanguage(text);
                    var detectedLanguage = detection?.Language ?? "unknown";

                    if (string.Equals(detectedLanguage, targetLanguage, StringComparison.OrdinalIgnoreCase))
                    {
                        return new TranslationResultData
                        {
                            OriginalText = text,
                            TranslatedText = text,
                            DetectedSourceLanguage = detectedLanguage,
                            TargetLanguage = targetLanguage
                        };
                    }

                    var result = _client.TranslateText(text, targetLanguage);

                    return new TranslationResultData
                    {
                        OriginalText = text,
                        TranslatedText = result.TranslatedText,
                        DetectedSourceLanguage = detectedLanguage,
                        TargetLanguage = targetLanguage
                    };
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Translation failed: {ex}");
                return null;
            }
        }

        public string NormalizeLanguageCode(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return "en";

            input = input.Trim().ToLowerInvariant();

            return input switch
            {
                "english" => "en",
                "eng" => "en",
                "spanish" => "es",
                "español" => "es",
                "french" => "fr",
                "français" => "fr",
                "german" => "de",
                "deutsch" => "de",
                "italian" => "it",
                "italiano" => "it",
                "japanese" => "ja",
                "jp" => "ja",
                "korean" => "ko",
                "portuguese" => "pt",
                "brazilian portuguese" => "pt",
                "russian" => "ru",
                "ukrainian" => "uk",
                "polish" => "pl",
                "turkish" => "tr",
                "arabic" => "ar",
                "chinese" => "zh-CN",
                "simplified chinese" => "zh-CN",
                "traditional chinese" => "zh-TW",
                "dutch" => "nl",
                "swedish" => "sv",
                "norwegian" => "no",
                "danish" => "da",
                "finnish" => "fi",
                "czech" => "cs",
                "greek" => "el",
                "romanian" => "ro",
                "hungarian" => "hu",
                "thai" => "th",
                "vietnamese" => "vi",
                "indonesian" => "id",
                _ => input
            };
        }
    }
}