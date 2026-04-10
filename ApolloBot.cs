
// ApolloBot - Update: Silent Mode, Experimental Tags, Clean URLs

// --- ADDITIONS SUMMARY ---
// 1. Silent mode per guild
// 2. Experimental label for Threads
// 3. URL cleaning (utm, si, etc.)

// NOTE: This is a PATCH-style file. Merge into your existing ApolloBot.cs

// =======================
// ADD TO GuildSettings
// =======================
public bool SilentMode { get; set; } = false;


// =======================
// CLEAN URL FUNCTION
// =======================
private string CleanUrl(string url)
{
    try
    {
        var uri = new Uri(url);
        var clean = uri.GetLeftPart(UriPartial.Path);
        return clean;
    }
    catch
    {
        return url;
    }
}


// =======================
// APPLY CLEANING
// =======================
private string CleanAllUrls(string text)
{
    return Regex.Replace(text, @"https?://[^\s]+", match =>
    {
        return CleanUrl(match.Value);
    });
}


// =======================
// MODIFY ApplyAllReplacements
// =======================
// Add this at the top of the method:
result = CleanAllUrls(result);


// =======================
// SILENT MODE COMMAND
// =======================
if (sub == "silent")
{
    if (parts.Length < 2)
    {
        await textChannel.SendMessageAsync("Usage: !ab silent on/off");
        return;
    }

    string mode = parts[1].ToLower();

    if (mode == "on")
    {
        settings.SilentMode = true;
        SaveGuildSettings();
        await textChannel.SendMessageAsync("Silent mode enabled.");
    }
    else if (mode == "off")
    {
        settings.SilentMode = false;
        SaveGuildSettings();
        await textChannel.SendMessageAsync("Silent mode disabled.");
    }
    return;
}


// =======================
// MODIFY BUTTON BUILDING
// =======================
private MessageComponent BuildButtons(List<string> platforms, bool silentMode)
{
    if (silentMode)
        return new ComponentBuilder().Build();

    var builder = new ComponentBuilder();

    foreach (string platform in platforms.Distinct())
    {
        string label = GetButtonLabel(platform);

        if (platform == "threads")
            label += " (Experimental)";

        builder.WithButton(label, $"cycle_{platform}", ButtonStyle.Danger);
    }

    builder.WithButton("Delete", "delete_embed", ButtonStyle.Secondary);

    return builder.Build();
}


// =======================
// MODIFY CALL SITE
// =======================
// Replace:
// BuildButtons(detectedPlatforms)
// With:
BuildButtons(detectedPlatforms, settings.SilentMode)
