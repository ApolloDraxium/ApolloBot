using System.Net;
using System.Net.Sockets;
using System.Text;
using Discord;
using Discord.Net;
using Discord.Rest;
using Discord.Webhook;
using Discord.WebSocket;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

public static class DataPathHelper
{
    public static string GetDataPath()
    {
        return Environment.GetEnvironmentVariable("RAILWAY_VOLUME_MOUNT_PATH")
            ?? Environment.GetEnvironmentVariable("APP_DATA_PATH")
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

class Program
{
    private DiscordSocketClient? _client;
    private readonly Random _random = new();
    private bool _slashCommandsRegistered = false;
    private long _embedsFixedCount = 0;
    private long _accumulatedUptimeSeconds = 0;
    private DateTime _sessionStartedAtUtc = DateTime.UtcNow;
    private DateTime _lastHeartbeatUtc = DateTime.UtcNow;
    private List<UptimeSession> _uptimeHistory = new();
    private readonly object _statsLock = new();

    // Use your test server ID for fast slash command registration.
    // Set to 0 to register globally instead.
    private static readonly ulong TestGuildId = 1486178596765565009;

    private Dictionary<string, List<string>> _providers = CreateDefaultProviders();
    private readonly HashSet<ulong> _specialTwitterUsers = new();
    private readonly SortedDictionary<int, string> _plannedUpdates = new();

    private readonly Dictionary<ulong, RelayMessageState> _relayStates = new();
    private readonly Dictionary<(ulong MessageId, ulong UserId), DateTime> _cooldowns = new();
    private readonly Dictionary<ulong, GuildSettings> _guildSettings = new();
    private readonly Dictionary<ulong, UserIgnoreSettings> _userIgnoreSettings = new();

    private const string WebhookName = "Apollo Bot Relay";

    private static readonly string DataDirectory = DataPathHelper.GetDataPath();

    private static readonly string BotStatsStateFilePath =
        Path.Combine(DataDirectory, "bot_stats_state.json");

    private static readonly string StateFilePath =
        Path.Combine(DataDirectory, "relay_states.json");

    private static readonly string GuildSettingsFilePath =
        Path.Combine(DataDirectory, "guild_settings.json");

    private static readonly string UserIgnoreSettingsFilePath =
        Path.Combine(DataDirectory, "user_ignore_settings.json");

    private static readonly string PresenceFilePath =
        Path.Combine(DataDirectory, "bot_presence.json");

    private static readonly string ProvidersFilePath =
        Path.Combine(DataDirectory, "providers.json");

    private static readonly string SpecialTwitterUsersFilePath =
        Path.Combine(DataDirectory, "special_twitter_users.json");

    private static readonly string PlannedUpdatesFilePath =
        Path.Combine(DataDirectory, "planned_updates.json");

    private static readonly string GuildActivityStateFilePath =
        Path.Combine(DataDirectory, "guild_activity_state.json");

    private static readonly string GuildUsageStatsFilePath =
        Path.Combine(DataDirectory, "guild_usage_stats.json");

    private readonly Dictionary<ulong, GuildActivityState> _guildActivity = new();
    private readonly Dictionary<ulong, GuildUsageStats> _guildUsageStats = new();

    private readonly ulong _ownerLogChannelId =
        ulong.TryParse(Environment.GetEnvironmentVariable("OWNER_LOG_CHANNEL_ID"), out ulong parsedOwnerLogChannelId)
            ? parsedOwnerLogChannelId
            : 0;

    private readonly string _supportUrl =
        Environment.GetEnvironmentVariable("APOLLOBOT_SUPPORT_URL")?.Trim() ?? "";

    private const int DefaultPageSize = 10;

    private BotPresenceSettings _presenceSettings = new();

    private static readonly HashSet<ulong> BotOwnerIds = new()
    {
        127877921464385537,
        846147700700610600
    };

    private static readonly TimeSpan ButtonCooldown = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan CooldownRetention = TimeSpan.FromMinutes(10);

    static Task Main(string[] args)
    {
        DataPathHelper.EnsureAndLog();
        return new Program().MainAsync();
    }

    public async Task MainAsync()
    {
        Directory.CreateDirectory(DataDirectory);

        LoadRelayStates();
        LoadGuildSettings();
        LoadUserIgnoreSettings();
        LoadBotStatsState();
        LoadPresenceSettings();
        LoadProviders();
        LoadSpecialTwitterUsers();
        LoadPlannedUpdates();
        LoadGuildActivityState();
        LoadGuildUsageStats();
        RegisterShutdownHandlers();

        _client = new DiscordSocketClient(new DiscordSocketConfig
        {
            GatewayIntents =
                GatewayIntents.Guilds |
                GatewayIntents.GuildMessages |
                GatewayIntents.MessageContent
        });

        _client.Log += Log;
        _client.Ready += OnReady;
        _client.JoinedGuild += OnJoinedGuild;
        _client.LeftGuild += OnLeftGuild;
        _client.MessageReceived += MessageReceived;
        _client.ButtonExecuted += ButtonExecuted;
        _client.SlashCommandExecuted += SlashCommandExecuted;

        string? token = Environment.GetEnvironmentVariable("DISCORD_TOKEN");

        if (string.IsNullOrWhiteSpace(token))
        {
            Console.WriteLine("DISCORD_TOKEN is missing. Set it as an environment variable in your host dashboard.");
            return;
        }

        await _client.LoginAsync(TokenType.Bot, token);
        await _client.StartAsync();

        _ = Task.Run(StartStatsHttpServerAsync);

        _ = Task.Run(async () =>
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(60));
                SaveBotStatsHeartbeat();
            }
        });

        Console.WriteLine("Bot is running.");
        await Task.Delay(-1);
    }

    private Task Log(LogMessage msg)
    {
        Console.WriteLine(msg.ToString());
        return Task.CompletedTask;
    }

    private async Task OnReady()
    {
        Console.WriteLine($"Connected as {_client?.CurrentUser}");
        Console.WriteLine($"Loaded {_relayStates.Count} persisted relay state(s).");
        Console.WriteLine($"Loaded {_guildSettings.Count} guild setting profile(s).");
        Console.WriteLine($"Loaded {_userIgnoreSettings.Count} user ignore profile(s).");

        if (_client != null)
            await ApplyPresenceAsync();

        await SyncGuildTrackingStateAsync();

        if (!_slashCommandsRegistered)
        {
            await RegisterSlashCommandsAsync();
            _slashCommandsRegistered = true;
        }
    }


    private async Task OnJoinedGuild(SocketGuild guild)
    {
        try
        {
            Console.WriteLine($"[JOIN] Joined guild: {guild.Name} ({guild.Id})");

            GuildActivityState activity = GetOrCreateGuildActivityState(guild);
            activity.ServerName = guild.Name;
            activity.LastKnownMemberCount = guild.MemberCount;
            activity.OwnerId = guild.OwnerId;
            activity.OwnerName = guild.Owner != null
                ? $"{guild.Owner.Username}#{guild.Owner.Discriminator}"
                : "Unknown";
            activity.LastJoinedAtUtc = DateTime.UtcNow;
            activity.LastUpdatedAtUtc = DateTime.UtcNow;
            SaveGuildActivityState();

            EnsureGuildUsageStatsEntry(guild);
            SaveGuildUsageStats();

            await SendGuildLifecycleLogAsync(guild, joined: true, activity);

            await Task.Delay(TimeSpan.FromSeconds(2));

            if (_client?.CurrentUser == null)
            {
                Console.WriteLine("[JOIN] CurrentUser is null, skipping welcome message.");
                return;
            }

            SocketGuildUser? botUser = guild.GetUser(_client.CurrentUser.Id);
            if (botUser == null)
            {
                Console.WriteLine($"[JOIN] Could not resolve bot user in guild '{guild.Name}'.");
                return;
            }

            SocketTextChannel? channel = null;

            if (guild.SystemChannel != null)
            {
                ChannelPermissions systemPerms = botUser.GetPermissions(guild.SystemChannel);
                if (systemPerms.ViewChannel && systemPerms.SendMessages && systemPerms.EmbedLinks)
                    channel = guild.SystemChannel;
            }

            if (channel == null)
            {
                channel = guild.TextChannels
                    .OrderBy(c => c.Position)
                    .FirstOrDefault(c =>
                    {
                        ChannelPermissions perms = botUser.GetPermissions(c);
                        return perms.ViewChannel && perms.SendMessages && perms.EmbedLinks;
                    });
            }

            if (channel == null)
            {
                Console.WriteLine($"[JOIN] No usable text channel found for welcome message in guild '{guild.Name}' ({guild.Id}).");
                return;
            }

            var embed = new EmbedBuilder()
                .WithTitle("Hey! I'm ApolloBot 👋")
                .WithDescription(
                    "I fix embeds for:
" +
                    "Twitter / Reddit / TikTok / Instagram

" +
                    "**Use:**
" +
                    "`!embedfix on` *(To ensure I am fixing embeds)*
" +
                    "`!ab perms` *(To ensure I have the right permissions per channel)*
" +
                    "`!support` *(For bug reports, help, and feedback)*

" +
                    "**Optional:**
" +
                    "`!ab help` *(For additional commands)*")
                .WithColor(Color.Red)
                .Build();

            await channel.SendMessageAsync(embed: embed);
            Console.WriteLine($"[JOIN] Welcome message sent in #{channel.Name} ({channel.Id}) for guild '{guild.Name}'.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to handle joined guild flow: {ex}");
        }
    }

    private async Task OnLeftGuild(SocketGuild guild)
    {
        try
        {
            GuildActivityState activity = GetOrCreateGuildActivityState(guild);
            activity.ServerName = string.IsNullOrWhiteSpace(guild.Name) ? activity.ServerName : guild.Name;
            activity.LastKnownMemberCount = guild.MemberCount > 0 ? guild.MemberCount : activity.LastKnownMemberCount;
            activity.OwnerId = guild.OwnerId != 0 ? guild.OwnerId : activity.OwnerId;
            activity.LastRemovedAtUtc = DateTime.UtcNow;
            activity.LastUpdatedAtUtc = DateTime.UtcNow;

            SaveGuildActivityState();
            await SendGuildLifecycleLogAsync(guild, joined: false, activity);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to handle left guild flow: {ex}");
        }
    }

    private async Task RegisterSlashCommandsAsync()
    {
        if (_client == null)
            return;

        var rollCommand = new SlashCommandBuilder()
            .WithName("roll")
            .WithDescription("Roll dice for D&D or other tabletop chaos.")
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("dice")
                .WithDescription("Dice formula, e.g. 1d20, 1d20+6, 2d6+3")
                .WithType(ApplicationCommandOptionType.String)
                .WithRequired(false))
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("mode")
                .WithDescription("Normal, advantage, or disadvantage")
                .WithType(ApplicationCommandOptionType.String)
                .WithRequired(false)
                .AddChoice("normal", "normal")
                .AddChoice("advantage", "advantage")
                .AddChoice("disadvantage", "disadvantage"));

        try
        {
            if (TestGuildId != 0)
            {
                SocketGuild? guild = _client.GetGuild(TestGuildId);
                if (guild == null)
                {
                    Console.WriteLine($"Test guild {TestGuildId} not found. Slash command was not registered.");
                    return;
                }

                await guild.BulkOverwriteApplicationCommandAsync(new ApplicationCommandProperties[]
                {
                    rollCommand.Build()
                });

                Console.WriteLine($"Registered slash commands in test guild: {guild.Name} ({guild.Id})");
            }
            else
            {
                await _client.BulkOverwriteGlobalApplicationCommandsAsync(new ApplicationCommandProperties[]
                {
                    rollCommand.Build()
                });

                Console.WriteLine("Registered global slash commands.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to register slash commands: {ex}");
        }
    }

    private async Task MessageReceived(SocketMessage message)
    {
        if (message.Author.IsBot)
            return;

        if (message.Channel is not SocketTextChannel textChannel)
            return;

        if (message is not SocketUserMessage userMessage)
            return;

        if (string.IsNullOrWhiteSpace(userMessage.Content))
        {
            await NotifyOriginalAuthorOfReplyAsync(userMessage, textChannel);
            return;
        }

        string content = userMessage.Content.Trim();

        if (content.Equals("!vote", StringComparison.OrdinalIgnoreCase))
        {
            await SendVoteMessage(textChannel, userMessage.Author);
            return;
        }

        if (content.Equals("!updates", StringComparison.OrdinalIgnoreCase) ||
            content.Equals("!ab updates", StringComparison.OrdinalIgnoreCase))
        {
            await SendPlannedUpdates(textChannel);
            return;
        }

        if (content.Equals("!support", StringComparison.OrdinalIgnoreCase) ||
            content.Equals("!ab support", StringComparison.OrdinalIgnoreCase))
        {
            await SendSupportMessage(textChannel);
            return;
        }

        if (content.StartsWith("!bot", StringComparison.OrdinalIgnoreCase))
        {
            if (!IsBotOwner(userMessage.Author))
                return;

            await HandleBotCommand(userMessage, textChannel);
            return;
        }

        if (content.Equals("!embedfix on", StringComparison.OrdinalIgnoreCase) ||
            content.Equals("!embedfix off", StringComparison.OrdinalIgnoreCase))
        {
            await HandleApolloBotCommand(userMessage, textChannel);
            return;
        }

        if (content.StartsWith("!ab", StringComparison.OrdinalIgnoreCase))
        {
            await HandleApolloBotCommand(userMessage, textChannel);
            return;
        }

        await NotifyOriginalAuthorOfReplyAsync(userMessage, textChannel);

        if (!ShouldProcessMessageInChannel(textChannel))
            return;

        if (ShouldIgnoreUser(textChannel.Guild.Id, userMessage.Author.Id))
            return;

        string originalContent = message.Content;
        List<string> detectedPlatforms = GetPlatformsInText(originalContent);

        if (detectedPlatforms.Count == 0)
            return;

        Dictionary<string, int> providerIndexes = CreateDefaultProviderIndexes(detectedPlatforms);
        string newContent = ApplyAllReplacements(originalContent, providerIndexes, message.Author.Id);

        if (newContent == originalContent)
            return;

        try
        {
            List<string> missing = GetLikelyMissingPermissions(textChannel);
            if (missing.Count > 0)
            {
                Console.WriteLine(
                    $"[PRECHECK] Bot may be missing permissions in guild '{textChannel.Guild.Name}' " +
                    $"channel '#{textChannel.Name}': {string.Join(", ", missing)}");
            }

            RestWebhook? webhook = await GetOrCreateWebhookAsync(textChannel);
            if (webhook == null)
                return;

            if (string.IsNullOrWhiteSpace(webhook.Token))
            {
                Console.WriteLine("Webhook token is missing.");
                return;
            }

            string displayName = message.Author.GlobalName ?? message.Author.Username;
            string avatarUrl = message.Author.GetAvatarUrl(ImageFormat.Auto, 128)
                               ?? message.Author.GetDefaultAvatarUrl();

            var webhookClient = new DiscordWebhookClient(webhook.Id, webhook.Token);

            ulong relayedMessageId = await webhookClient.SendMessageAsync(
                text: newContent,
                username: displayName,
                avatarUrl: avatarUrl,
                components: BuildButtons(detectedPlatforms)
            );

            _relayStates[relayedMessageId] = new RelayMessageState
            {
                OriginalContent = originalContent,
                WebhookId = webhook.Id,
                WebhookToken = webhook.Token,
                OriginalAuthorId = message.Author.Id,
                Platforms = detectedPlatforms,
                ProviderIndexes = providerIndexes
            };

            SaveRelayStates();
            IncrementEmbedsFixedCount();
            RecordGuildEmbedFix(textChannel.Guild);

            await message.DeleteAsync();
        }
        catch (Exception ex)
        {
            LogPermissionFailure(textChannel, "Relaying message", ex);
        }
    }

    private async Task<RestWebhook?> GetOrCreateWebhookAsync(SocketTextChannel textChannel)
    {
        IReadOnlyCollection<RestWebhook> webhooks = await textChannel.GetWebhooksAsync();
        RestWebhook? webhook = webhooks.FirstOrDefault(w => w.Name == WebhookName);

        if (webhook == null)
            webhook = await textChannel.CreateWebhookAsync(WebhookName);

        return webhook;
    }

    private async Task HandleBotCommand(SocketUserMessage message, SocketTextChannel textChannel)
    {
        string[] parts = message.Content
            .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length < 2)
        {
            await SendBotHelp(textChannel);
            return;
        }

        string sub = parts[1].ToLowerInvariant();

        if (sub == "help")
        {
            await SendBotHelp(textChannel);
            return;
        }

        if (sub == "provider")
        {
            await HandleProviderCommand(textChannel, parts);
            return;
        }

        if (sub == "special")
        {
            await HandleSpecialTwitterUserCommand(textChannel, parts);
            return;
        }

        if (sub == "update")
        {
            await HandlePlannedUpdateOwnerCommand(textChannel, parts);
            return;
        }

        if (sub == "status")
        {
            if (parts.Length < 5)
            {
                await textChannel.SendMessageAsync(
                    "Usage:\n" +
                    "`!bot status <type> <status> <text>`\n" +
                    "Types: playing, watching, listening, streaming\n" +
                    "Status: online, idle, dnd, invisible\n\n" +
                    "Example:\n" +
                    "`!bot status watching online Fixing embeds`");
                return;
            }

            string type = parts[2].ToLowerInvariant();
            string status = parts[3].ToLowerInvariant();
            string textValue;
            string? streamUrl = null;

            if (type is not ("playing" or "watching" or "listening" or "streaming"))
            {
                await textChannel.SendMessageAsync("Invalid type. Use: playing, watching, listening, or streaming.");
                return;
            }

            if (status is not ("online" or "idle" or "dnd" or "invisible"))
            {
                await textChannel.SendMessageAsync("Invalid status. Use: online, idle, dnd, or invisible.");
                return;
            }

            if (type == "streaming")
            {
                if (parts.Length < 6)
                {
                    await textChannel.SendMessageAsync(
                        "Usage for streaming:\n`!bot status streaming <status> <url> <text>`");
                    return;
                }

                streamUrl = parts[4];
                textValue = string.Join(" ", parts.Skip(5));
            }
            else
            {
                textValue = string.Join(" ", parts.Skip(4));
            }

            if (string.IsNullOrWhiteSpace(textValue))
            {
                await textChannel.SendMessageAsync("Status text cannot be empty.");
                return;
            }

            _presenceSettings.Type = type;
            _presenceSettings.Status = status;
            _presenceSettings.Text = textValue;
            _presenceSettings.StreamUrl = streamUrl;

            SavePresenceSettings();
            await ApplyPresenceAsync();

            await textChannel.SendMessageAsync($"✅ Status updated to **{type} {textValue}**");
            return;
        }

        if (sub == "servercount")
        {
            int count = _client?.Guilds.Count ?? 0;
            await textChannel.SendMessageAsync($"🌐 Connected to **{count}** server(s).");
            return;
        }

        if (sub == "setembeds")
        {
            if (parts.Length < 3 || !long.TryParse(parts[2], out long newCount) || newCount < 0)
            {
                await textChannel.SendMessageAsync("Usage: `!bot setembeds <number>`");
                return;
            }

            lock (_statsLock)
            {
                _embedsFixedCount = newCount;
            }

            SaveBotStatsHeartbeat();

            await textChannel.SendMessageAsync($"✅ Embeds fixed count set to **{newCount}**.");
            return;
        }

        if (sub == "setservicetime")
        {
            if (parts.Length < 3)
            {
                await textChannel.SendMessageAsync(
                    "Usage: `!bot setservicetime <duration>`\n" +
                    "Examples: `!bot setservicetime 11d`, `!bot setservicetime 11d12h`, `!bot setservicetime 3h30m`, `!bot setservicetime 90m`");
                return;
            }

            string durationText = string.Concat(parts.Skip(2));

            if (!TryParseDurationInput(durationText, out long totalSeconds) || totalSeconds < 0)
            {
                await textChannel.SendMessageAsync(
                    "Invalid duration. Examples: `11d`, `11d12h`, `3h30m`, `90m`, `3600s`");
                return;
            }

            lock (_statsLock)
            {
                _accumulatedUptimeSeconds = totalSeconds;
                _uptimeHistory = new List<UptimeSession>();
                _sessionStartedAtUtc = DateTime.UtcNow;
                _lastHeartbeatUtc = _sessionStartedAtUtc;
            }

            SaveBotStatsHeartbeat();

            await textChannel.SendMessageAsync($"✅ Service time set to **{FormatDuration(TimeSpan.FromSeconds(totalSeconds))}**.");
            return;
        }

        if (sub == "servers")
        {
            if (_client == null)
            {
                await textChannel.SendMessageAsync("Client not ready.");
                return;
            }

            var guilds = _client.Guilds
                .OrderByDescending(g => g.MemberCount)
                .ThenBy(g => g.Name)
                .Select((g, i) => $"**{i + 1}.** {g.Name}\nID: `{g.Id}` | Members: **{g.MemberCount}**")
                .ToList();

            if (guilds.Count == 0)
            {
                await textChannel.SendMessageAsync("I'm not in any servers.");
                return;
            }

            await SendPaginatedEmbedAsync(
                textChannel,
                "ApolloBot Connected Servers",
                guilds,
                "botservers",
                page: 0,
                pageSize: DefaultPageSize,
                color: Color.Gold,
                headerText: $"**Total Servers:** {_client.Guilds.Count}\n**Total Users:** {_client.Guilds.Sum(g => g.MemberCount)}");

            return;
        }

        if (sub == "topservers")
        {
            await SendTopServersByUsageAsync(textChannel);
            return;
        }

        if (sub == "serverstats")
        {
            await SendSingleServerStatsAsync(textChannel, parts);
            return;
        }

        if (sub == "stats")
        {
            int serverCount = _client?.Guilds.Count ?? 0;
            int relayCount = _relayStates.Count;
            int guildSettingsCount = _guildSettings.Count;
            int ignoredUsersCount = _userIgnoreSettings.Count;
            long currentSessionSeconds = GetCurrentSessionSeconds();
            long totalUptimeSeconds = GetTotalUptimeSeconds();
            TimeSpan uptime = TimeSpan.FromSeconds(totalUptimeSeconds);

            var embed = new EmbedBuilder()
                .WithTitle("Bot Stats")
                .AddField("Servers", serverCount, true)
                .AddField("Relay States", relayCount, true)
                .AddField("Guild Settings", guildSettingsCount, true)
                .AddField("Ignored User Profiles", ignoredUsersCount, true)
                .AddField("Platforms Supported", _providers.Count, true)
                .AddField("Embeds Fixed", _embedsFixedCount, true)
                .AddField("Tracked Servers", _guildUsageStats.Count, true)
                .AddField("Uptime", FormatDuration(uptime), true)
                .WithColor(Color.DarkBlue)
                .WithCurrentTimestamp()
                .Build();

            await textChannel.SendMessageAsync(embed: embed);
            return;
        }

        await SendBotHelp(textChannel);
    }

    private async Task SendBotHelp(SocketTextChannel channel)
    {
        await SendPaginatedEmbedAsync(
            channel,
            "👑 Bot Owner Commands",
            BuildBotOwnerHelpLines(),
            "bothelp",
            page: 0,
            pageSize: DefaultPageSize,
            color: Color.DarkPurple,
            headerText: "Owner-only controls and maintenance commands.");
    }

    private async Task HandleApolloBotCommand(SocketUserMessage message, SocketTextChannel textChannel)
    {
        string raw = message.Content.Trim();
        string[] parts;

        if (raw.StartsWith("!ab", StringComparison.OrdinalIgnoreCase))
        {
            string remainder = raw.Length > 3 ? raw.Substring(3).Trim() : "";
            parts = string.IsNullOrWhiteSpace(remainder)
                ? Array.Empty<string>()
                : remainder.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
        else if (raw.StartsWith("!embedfix", StringComparison.OrdinalIgnoreCase))
        {
            string remainder = raw.Length > 10 ? raw.Substring(10).Trim() : "";
            parts = string.IsNullOrWhiteSpace(remainder)
                ? Array.Empty<string>()
                : remainder.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
        else
        {
            parts = Array.Empty<string>();
        }

        ulong guildId = textChannel.Guild.Id;
        GuildSettings settings = GetOrCreateGuildSettings(guildId);

        if (parts.Length == 0)
        {
            await SendApolloBotHelp(textChannel, message.Author);
            return;
        }

        string sub = parts[0].ToLowerInvariant();

        if (sub == "help")
        {
            await SendApolloBotHelp(textChannel, message.Author);
            return;
        }

        if (sub == "updates")
        {
            await SendPlannedUpdates(textChannel);
            return;
        }

        if (sub == "about")
        {
            await SendAbout(textChannel);
            return;
        }

        if (sub == "support")
        {
            await SendSupportMessage(textChannel);
            return;
        }

        if (sub == "ping")
        {
            int latency = _client?.Latency ?? 0;
            await textChannel.SendMessageAsync($"🏓 Pong! Gateway latency: **{latency}ms**");
            return;
        }

        if (sub == "providers")
        {
            await SendProviders(textChannel);
            return;
        }

        if (sub == "perms")
        {
            await SendPermissionReport(textChannel);
            return;
        }

        if (sub == "status")
        {
            await SendGuildStatus(textChannel, settings);
            return;
        }

        if (sub == "ignore")
        {
            await HandleIgnoreCommand(message, textChannel, parts);
            return;
        }

        if (sub == "update" && IsBotOwner(message.Author))
        {
            await HandlePlannedUpdateOwnerCommand(textChannel, new[] { "bot", "update" }.Concat(parts.Skip(1)).ToArray());
            return;
        }

        if (message.Author is not SocketGuildUser guildUser || !guildUser.GuildPermissions.ManageGuild)
        {
            await textChannel.SendMessageAsync("You need **Manage Server** to use that command.");
            return;
        }

        if (raw.Equals("!embedfix on", StringComparison.OrdinalIgnoreCase) || sub == "on")
        {
            settings.Enabled = true;
            SaveGuildSettings();
            await textChannel.SendMessageAsync("Embed fixer is now **enabled** for this server.");
            return;
        }

        if (raw.Equals("!embedfix off", StringComparison.OrdinalIgnoreCase) || sub == "off")
        {
            settings.Enabled = false;
            SaveGuildSettings();
            await textChannel.SendMessageAsync("Embed fixer is now **disabled** for this server.");
            return;
        }

        if (sub == "whitelist")
        {
            await HandleWhitelistCommand(message, textChannel, settings, parts);
            return;
        }

        await SendApolloBotHelp(textChannel, message.Author);
    }

    private async Task HandleIgnoreCommand(SocketUserMessage message, SocketTextChannel textChannel, string[] parts)
    {
        UserIgnoreSettings ignoreSettings = GetOrCreateUserIgnoreSettings(message.Author.Id);
        ulong guildId = textChannel.Guild.Id;

        if (parts.Length < 2)
        {
            bool ignoredHere = IsIgnoredInGuild(ignoreSettings, guildId);
            string globalText = ignoreSettings.IgnoreAllServers ? "ON" : "OFF";
            string thisServerText = ignoredHere ? "ON" : "OFF";

            await textChannel.SendMessageAsync(
                $"**Your ignore settings**\n" +
                $"This server: **{thisServerText}**\n" +
                $"All servers: **{globalText}**\n\n" +
                "Commands:\n" +
                "`!ab ignore on`\n" +
                "`!ab ignore off`\n" +
                "`!ab ignore all`\n" +
                "`!ab ignore all on`\n" +
                "`!ab ignore all off`");
            return;
        }

        string action = parts[1].ToLowerInvariant();

        if (action == "on")
        {
            if (!ignoreSettings.IgnoredGuildIds.Contains(guildId))
                ignoreSettings.IgnoredGuildIds.Add(guildId);

            SaveUserIgnoreSettings();
            await textChannel.SendMessageAsync("✅ ApolloBot will now **ignore your embeds in this server**.");
            return;
        }

        if (action == "off")
        {
            bool removed = ignoreSettings.IgnoredGuildIds.Remove(guildId);
            SaveUserIgnoreSettings();

            if (ignoreSettings.IgnoreAllServers)
            {
                await textChannel.SendMessageAsync(
                    "⚠️ You turned off ignore for this server, but your **global ignore is still ON**, so ApolloBot will still ignore you everywhere.\n" +
                    "Use `!ab ignore all off` if you want the bot to process your embeds again.");
                return;
            }

            await textChannel.SendMessageAsync(
                removed
                    ? "✅ ApolloBot will no longer ignore your embeds in this server."
                    : "ApolloBot was already **not** ignoring your embeds in this server.");
            return;
        }

        if (action == "all")
        {
            if (parts.Length >= 3)
            {
                string mode = parts[2].ToLowerInvariant();

                if (mode == "on")
                {
                    ignoreSettings.IgnoreAllServers = true;
                    SaveUserIgnoreSettings();
                    await textChannel.SendMessageAsync("✅ ApolloBot will now **ignore your embeds in all servers**.");
                    return;
                }

                if (mode == "off")
                {
                    ignoreSettings.IgnoreAllServers = false;
                    SaveUserIgnoreSettings();
                    await textChannel.SendMessageAsync("✅ ApolloBot will no longer ignore your embeds globally.");
                    return;
                }

                await textChannel.SendMessageAsync("Usage: `!ab ignore all`, `!ab ignore all on`, or `!ab ignore all off`");
                return;
            }

            ignoreSettings.IgnoreAllServers = !ignoreSettings.IgnoreAllServers;
            SaveUserIgnoreSettings();

            await textChannel.SendMessageAsync(
                ignoreSettings.IgnoreAllServers
                    ? "✅ ApolloBot will now **ignore your embeds in all servers**."
                    : "✅ ApolloBot will no longer ignore your embeds globally.");
            return;
        }

        await textChannel.SendMessageAsync(
            "Usage:\n" +
            "`!ab ignore on`\n" +
            "`!ab ignore off`\n" +
            "`!ab ignore all`\n" +
            "`!ab ignore all on`\n" +
            "`!ab ignore all off`");
    }

    private async Task SendApolloBotHelp(SocketTextChannel channel, SocketUser user)
    {
        bool isAdmin = user is SocketGuildUser guildUser && guildUser.GuildPermissions.ManageGuild;
        List<string> lines = BuildApolloBotHelpLines(isAdmin);

        await SendPaginatedEmbedAsync(
            channel,
            "📦 ApolloBot Commands",
            lines,
            "abhelp",
            page: 0,
            pageSize: DefaultPageSize,
            color: Color.Teal,
            headerText: "Embed fixing, support, and utility commands.");
    }

    private async Task SendAbout(SocketTextChannel channel)
    {
        var embed = new EmbedBuilder()
            .WithTitle("About ApolloBot")
            .WithDescription("Replaces supported social links with embed-friendly providers and reposts them through a webhook relay.")
            .AddField("Supported Platforms", "Twitter/X, Reddit, TikTok, Instagram", false)
            .AddField("Features",
                "• Provider cycling buttons\n" +
                "• Original poster-only controls\n" +
                "• Delete button\n" +
                "• Cooldowns\n" +
                "• Persistence\n" +
                "• User ignore system\n" +
                "• Reply ping for original poster\n" +
                "• Server enable/disable and whitelist settings\n" +
                "• Slash roll command\n" +
                "• Public vote command\n" +
                "• Public planned updates list\n" +
                "• Support command for bug reports and feedback\n" +
                "• Dynamic provider management for bot owners", false)
            .WithColor(Color.Gold)
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }

    private async Task SendProviders(SocketTextChannel channel)
    {
        var lines = _providers
            .OrderBy(p => p.Key)
            .Select(p => $"**{FormatPlatformName(p.Key)}**: {string.Join(", ", p.Value)}");

        var embed = new EmbedBuilder()
            .WithTitle("Configured Providers")
            .WithDescription(string.Join("\n", lines))
            .WithColor(Color.LightGrey)
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }


    private async Task SendVoteMessage(SocketTextChannel channel, SocketUser user)
    {
        var embed = new EmbedBuilder()
            .WithTitle("Support ApolloBot")
            .WithDescription($"{user.Mention} you can vote for ApolloBot here:\nhttps://top.gg/bot/1486174544531034212/vote")
            .WithColor(Color.Gold)
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }


    private async Task SendSupportMessage(SocketTextChannel channel)
    {
        if (string.IsNullOrWhiteSpace(_supportUrl))
        {
            await channel.SendMessageAsync(
                "Support is not configured yet. Set the `APOLLOBOT_SUPPORT_URL` environment variable to enable `!support`.");
            return;
        }

        var embed = new EmbedBuilder()
            .WithTitle("ApolloBot Support & Feedback")
            .WithDescription(
                "Found a bug, have a suggestion, or want to send feedback?\n\n" +
                $"Use the support page here:\n{_supportUrl}")
            .WithColor(Color.Gold)
            .WithFooter("Your feedback helps improve ApolloBot.")
            .Build();

        var components = new ComponentBuilder()
            .WithButton("Open Support Page", url: _supportUrl, style: ButtonStyle.Link)
            .Build();

        await channel.SendMessageAsync(embed: embed, components: components);
    }

    private async Task SendPlannedUpdates(SocketTextChannel channel)
    {
        if (_plannedUpdates.Count == 0)
        {
            await channel.SendMessageAsync("There are no planned updates listed right now.");
            return;
        }

        string lines = string.Join("\n", _plannedUpdates.Select(kvp => $"`{kvp.Key}.` {kvp.Value}"));

        var embed = new EmbedBuilder()
            .WithTitle("ApolloBot Planned Updates")
            .WithDescription(lines)
            .WithColor(Color.Blue)
            .WithFooter("Subject to change.")
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }

    private async Task HandlePlannedUpdateOwnerCommand(SocketTextChannel channel, string[] parts)
    {
        if (parts.Length < 3)
        {
            await channel.SendMessageAsync(
                "Usage:\n" +
                "`!bot update add <id> <text>`\n" +
                "`!bot update edit <id> <text>`\n" +
                "`!bot update remove <id>`\n" +
                "`!bot update list`\n" +
                "`!bot update clear`");
            return;
        }

        string action = parts[2].ToLowerInvariant();

        if (action == "list")
        {
            await SendPlannedUpdates(channel);
            return;
        }

        if (action == "clear")
        {
            _plannedUpdates.Clear();
            SavePlannedUpdates();
            await channel.SendMessageAsync("Cleared all planned updates.");
            return;
        }

        if (action == "remove")
        {
            if (parts.Length < 4 || !int.TryParse(parts[3], out int removeId) || removeId <= 0)
            {
                await channel.SendMessageAsync("Please provide a valid positive update ID.");
                return;
            }

            bool removed = _plannedUpdates.Remove(removeId);
            SavePlannedUpdates();
            await channel.SendMessageAsync(removed
                ? $"Removed update `{removeId}`."
                : $"Update `{removeId}` was not found.");
            return;
        }

        if (action is not ("add" or "edit"))
        {
            await channel.SendMessageAsync("Unknown update action. Use add, edit, remove, list, or clear.");
            return;
        }

        if (parts.Length < 5 || !int.TryParse(parts[3], out int id) || id <= 0)
        {
            await channel.SendMessageAsync("Please provide a valid positive update ID.");
            return;
        }

        string textValue = string.Join(" ", parts.Skip(4)).Trim();

        if (string.IsNullOrWhiteSpace(textValue))
        {
            await channel.SendMessageAsync("Please provide update text.");
            return;
        }

        _plannedUpdates[id] = textValue;
        SavePlannedUpdates();

        await channel.SendMessageAsync(action == "add"
            ? $"Added update `{id}`."
            : $"Updated update `{id}`.");
    }

    private async Task HandleSpecialTwitterUserCommand(SocketTextChannel channel, string[] parts)
    {
        if (parts.Length < 3)
        {
            await channel.SendMessageAsync(
                "Usage:\n" +
                "`!bot special add <userId>`\n" +
                "`!bot special remove <userId>`\n" +
                "`!bot special list`\n" +
                "`!bot special clear`");
            return;
        }

        string action = parts[2].ToLowerInvariant();

        if (action == "list")
        {
            if (_specialTwitterUsers.Count == 0)
            {
                await channel.SendMessageAsync("No users are approved for the silly Twitter/X providers.");
                return;
            }

            string users = string.Join("\n", _specialTwitterUsers.Select(x => $"• `{x}`"));
            await channel.SendMessageAsync($"**Approved silly Twitter/X users:**\n{users}");
            return;
        }

        if (action == "clear")
        {
            _specialTwitterUsers.Clear();
            SaveSpecialTwitterUsers();
            await channel.SendMessageAsync("Cleared the silly Twitter/X user list.");
            return;
        }

        if (parts.Length < 4 || !ulong.TryParse(parts[3], out ulong userId))
        {
            await channel.SendMessageAsync("Please provide a valid user ID.");
            return;
        }

        if (action == "add")
        {
            bool added = _specialTwitterUsers.Add(userId);
            SaveSpecialTwitterUsers();
            await channel.SendMessageAsync(added
                ? $"Added `{userId}` to the silly Twitter/X list."
                : $"`{userId}` is already on the silly Twitter/X list.");
            return;
        }

        if (action == "remove")
        {
            bool removed = _specialTwitterUsers.Remove(userId);
            SaveSpecialTwitterUsers();
            await channel.SendMessageAsync(removed
                ? $"Removed `{userId}` from the silly Twitter/X list."
                : $"`{userId}` was not on the silly Twitter/X list.");
            return;
        }

        await channel.SendMessageAsync("Unknown special action. Use add, remove, list, or clear.");
    }

    private async Task HandleProviderCommand(SocketTextChannel channel, string[] parts)
    {
        if (parts.Length < 3)
        {
            await channel.SendMessageAsync(
                "Usage:\n" +
                "`!bot provider list`\n" +
                "`!bot provider list <platform>`\n" +
                "`!bot provider add <platform> <domain>`\n" +
                "`!bot provider remove <platform> <domain>`\n" +
                "`!bot provider clear <platform>`");
            return;
        }

        string action = parts[2].ToLowerInvariant();

        if (action == "list")
        {
            if (parts.Length == 3)
            {
                await SendProviders(channel);
                return;
            }

            string platform = parts[3].ToLowerInvariant();

            if (!_providers.TryGetValue(platform, out List<string>? listedProviders))
            {
                await channel.SendMessageAsync("Invalid platform. Use twitter, reddit, tiktok, or instagram.");
                return;
            }

            await channel.SendMessageAsync(
                $"**{FormatPlatformName(platform)} providers:** {string.Join(", ", listedProviders)}");
            return;
        }

        if (parts.Length < 4)
        {
            await channel.SendMessageAsync("Please provide a platform.");
            return;
        }

        string targetPlatform = parts[3].ToLowerInvariant();

        if (!_providers.ContainsKey(targetPlatform))
        {
            await channel.SendMessageAsync("Invalid platform. Use twitter, reddit, tiktok, or instagram.");
            return;
        }

        if (action == "clear")
        {
            _providers[targetPlatform].Clear();
            SaveProviders();
            await channel.SendMessageAsync($"Cleared all providers for {FormatPlatformName(targetPlatform)}.");
            return;
        }

        if (parts.Length < 5)
        {
            await channel.SendMessageAsync("Please provide a provider domain.");
            return;
        }

        string domain = parts[4].Trim().ToLowerInvariant();

        if (domain.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            domain = domain["http://".Length..];

        if (domain.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            domain = domain["https://".Length..];

        domain = domain.Trim('/');

        if (string.IsNullOrWhiteSpace(domain))
        {
            await channel.SendMessageAsync("Please provide a valid provider domain.");
            return;
        }

        if (action == "add")
        {
            if (_providers[targetPlatform].Any(x => string.Equals(x, domain, StringComparison.OrdinalIgnoreCase)))
            {
                await channel.SendMessageAsync($"{domain} is already configured for {FormatPlatformName(targetPlatform)}.");
                return;
            }

            _providers[targetPlatform].Add(domain);
            SaveProviders();
            await channel.SendMessageAsync($"Added `{domain}` to {FormatPlatformName(targetPlatform)} providers.");
            return;
        }

        if (action == "remove")
        {
            string? existing = _providers[targetPlatform]
                .FirstOrDefault(x => string.Equals(x, domain, StringComparison.OrdinalIgnoreCase));

            if (existing == null)
            {
                await channel.SendMessageAsync($"{domain} was not found for {FormatPlatformName(targetPlatform)}.");
                return;
            }

            _providers[targetPlatform].Remove(existing);
            SaveProviders();
            await channel.SendMessageAsync($"Removed `{existing}` from {FormatPlatformName(targetPlatform)} providers.");
            return;
        }

        await channel.SendMessageAsync("Unknown provider action. Use add, remove, list, or clear.");
    }

    private async Task HandleWhitelistCommand(SocketUserMessage message, SocketTextChannel textChannel, GuildSettings settings, string[] parts)
    {
        if (parts.Length < 2)
        {
            await textChannel.SendMessageAsync(
                "**Whitelist commands:**\n" +
                "`!ab whitelist add`\n" +
                "`!ab whitelist add #channel`\n" +
                "`!ab whitelist remove`\n" +
                "`!ab whitelist remove #channel`\n" +
                "`!ab whitelist list`\n" +
                "`!ab whitelist clear`");
            return;
        }

        string action = parts[1].ToLowerInvariant();

        if (action == "list")
        {
            if (settings.WhitelistedChannelIds.Count == 0)
            {
                await textChannel.SendMessageAsync("Whitelist is empty, so the bot currently works in **all channels**.");
                return;
            }

            string listedChannels = string.Join(
                "\n",
                settings.WhitelistedChannelIds.Select(id => $"• <#{id}>"));

            await textChannel.SendMessageAsync($"**Whitelisted channels:**\n{listedChannels}");
            return;
        }

        if (action == "clear")
        {
            settings.WhitelistedChannelIds.Clear();
            SaveGuildSettings();

            await textChannel.SendMessageAsync("Whitelist cleared. Embed fixer now works in **all channels**.");
            return;
        }

        ulong targetChannelId = textChannel.Id;

        if (message.MentionedChannels.Count > 0)
            targetChannelId = message.MentionedChannels.First().Id;

        if (action == "add")
        {
            if (settings.WhitelistedChannelIds.Contains(targetChannelId))
            {
                await textChannel.SendMessageAsync($"<#{targetChannelId}> is already whitelisted.");
                return;
            }

            settings.WhitelistedChannelIds.Add(targetChannelId);
            SaveGuildSettings();

            await textChannel.SendMessageAsync($"Added <#{targetChannelId}> to the whitelist.");
            return;
        }

        if (action == "remove")
        {
            bool removed = settings.WhitelistedChannelIds.Remove(targetChannelId);
            SaveGuildSettings();

            if (removed)
                await textChannel.SendMessageAsync($"Removed <#{targetChannelId}> from the whitelist.");
            else
                await textChannel.SendMessageAsync($"<#{targetChannelId}> was not in the whitelist.");

            return;
        }

        await textChannel.SendMessageAsync("Unknown whitelist action. Use `add`, `remove`, `list`, or `clear`.");
    }

    private async Task SendGuildStatus(SocketTextChannel channel, GuildSettings settings)
    {
        string enabledText = settings.Enabled ? "Enabled" : "Disabled";
        string whitelistText = settings.WhitelistedChannelIds.Count == 0
            ? "All channels"
            : string.Join("\n", settings.WhitelistedChannelIds.Select(id => $"• <#{id}>"));

        var embed = new EmbedBuilder()
            .WithTitle("ApolloBot Settings")
            .WithDescription($"Settings for **{channel.Guild.Name}**")
            .AddField("Status", enabledText, true)
            .AddField("Allowed Channels", whitelistText, false)
            .WithColor(settings.Enabled ? Color.Green : Color.Red)
            .WithCurrentTimestamp()
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }

    private async Task SendPermissionReport(SocketTextChannel channel)
    {
        if (_client?.CurrentUser == null)
        {
            await channel.SendMessageAsync("Couldn't inspect bot permissions because CurrentUser is null.");
            return;
        }

        SocketGuildUser? botUser = channel.Guild.GetUser(_client.CurrentUser.Id);
        if (botUser == null)
        {
            await channel.SendMessageAsync("Couldn't resolve the bot user in this server.");
            return;
        }

        ChannelPermissions perms = botUser.GetPermissions(channel);
        GuildPermissions guildPerms = botUser.GuildPermissions;
        List<string> missing = GetLikelyMissingPermissions(channel);

        string likelyMissingText = missing.Count == 0
            ? "None detected from the current permission snapshot."
            : string.Join(", ", missing);

        var embed = new EmbedBuilder()
            .WithTitle("ApolloBot Permission Report")
            .WithDescription($"Permission check for **{channel.Guild.Name}** in <#{channel.Id}>")
            .AddField("View Channel", perms.ViewChannel, true)
            .AddField("Send Messages", perms.SendMessages, true)
            .AddField("Embed Links", perms.EmbedLinks, true)
            .AddField("Read Message History", perms.ReadMessageHistory, true)
            .AddField("Manage Messages", perms.ManageMessages, true)
            .AddField("Manage Webhooks", perms.ManageWebhooks, true)
            .AddField("Use Application Commands", guildPerms.UseApplicationCommands, true)
            .AddField("Likely Missing", likelyMissingText, false)
            .WithColor(missing.Count == 0 ? Color.Green : Color.Orange)
            .WithCurrentTimestamp()
            .Build();

        await channel.SendMessageAsync(embed: embed);

        Console.WriteLine("========== LIVE PERMISSION REPORT ==========");
        Console.WriteLine($"Guild:                   {channel.Guild.Name} ({channel.Guild.Id})");
        Console.WriteLine($"Channel:                 #{channel.Name} ({channel.Id})");
        Console.WriteLine($"Bot User:                {botUser.Username} ({botUser.Id})");
        Console.WriteLine($"ViewChannel:             {perms.ViewChannel}");
        Console.WriteLine($"SendMessages:            {perms.SendMessages}");
        Console.WriteLine($"EmbedLinks:              {perms.EmbedLinks}");
        Console.WriteLine($"ReadHistory:             {perms.ReadMessageHistory}");
        Console.WriteLine($"ManageMessages:          {perms.ManageMessages}");
        Console.WriteLine($"ManageWebhooks:          {perms.ManageWebhooks}");
        Console.WriteLine($"UseApplicationCommands:  {guildPerms.UseApplicationCommands}");
        Console.WriteLine($"LikelyMissing:           {(missing.Count == 0 ? "None" : string.Join(", ", missing))}");
        Console.WriteLine("===========================================");
    }

    private async Task SlashCommandExecuted(SocketSlashCommand command)
    {
        try
        {
            if (command.Data.Name == "roll")
            {
                await HandleRollSlashCommand(command);
                return;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error handling slash command '{command.Data.Name}': {ex}");

            try
            {
                if (!command.HasResponded)
                    await command.RespondAsync("Something went wrong while running that slash command.", ephemeral: true);
                else
                    await command.FollowupAsync("Something went wrong while running that slash command.", ephemeral: true);
            }
            catch
            {
            }
        }
    }

    private async Task HandleRollSlashCommand(SocketSlashCommand command)
    {
        string diceText = "1d20";
        string modeText = "normal";

        foreach (SocketSlashCommandDataOption option in command.Data.Options)
        {
            if (option.Name == "dice" && option.Value is string diceValue && !string.IsNullOrWhiteSpace(diceValue))
                diceText = diceValue.Trim();

            if (option.Name == "mode" && option.Value is string modeValue && !string.IsNullOrWhiteSpace(modeValue))
                modeText = modeValue.Trim().ToLowerInvariant();
        }

        RollParseResult parseResult = ParseRollCommand(diceText);

        if (!parseResult.Success || parseResult.Request == null)
        {
            await command.RespondAsync(
                "Invalid dice format.\nExamples: `1d20`, `1d20+6`, `2d6+3`",
                ephemeral: true);
            return;
        }

        RollRequest request = parseResult.Request;

        request.Advantage = modeText == "advantage";
        request.Disadvantage = modeText == "disadvantage";

        if ((request.Advantage || request.Disadvantage) &&
            !(request.DiceCount == 1 && request.DieSize == 20))
        {
            await command.RespondAsync("Advantage/disadvantage is only supported for **1d20** rolls.", ephemeral: true);
            return;
        }

        RollResult result = ExecuteRoll(request);

        var embed = new EmbedBuilder()
            .WithTitle("🎲 Roll Result")
            .WithDescription($"Requested by {command.User.Mention}")
            .AddField("Roll", result.RollLabel, true)
            .AddField("Mode", result.ModeLabel, true)
            .AddField("Total", result.Total, true)
            .WithColor(Color.DarkGreen)
            .WithCurrentTimestamp();

        if (result.AdvantageRolls.Count > 0)
        {
            embed.AddField(
                "Dice",
                $"{result.AdvantageRolls[0]} and {result.AdvantageRolls[1]} → kept **{result.BaseRollTotal}**",
                false);
        }
        else
        {
            embed.AddField("Dice", string.Join(", ", result.IndividualRolls), false);
        }

        if (request.Modifier != 0)
        {
            string modText = request.Modifier > 0
                ? $"+{request.Modifier}"
                : request.Modifier.ToString(CultureInfo.InvariantCulture);

            embed.AddField("Modifier", modText, true);
        }

        await command.RespondAsync(embed: embed.Build());
    }

    private RollParseResult ParseRollCommand(string argsText)
    {
        if (string.IsNullOrWhiteSpace(argsText))
        {
            return new RollParseResult
            {
                Success = true,
                Request = new RollRequest
                {
                    DiceCount = 1,
                    DieSize = 20,
                    Modifier = 0,
                    Advantage = false,
                    Disadvantage = false
                }
            };
        }

        string diceToken = argsText.Trim().ToLowerInvariant();

        if (diceToken.StartsWith("d"))
            diceToken = "1" + diceToken;

        Match match = Regex.Match(diceToken, @"^(\d+)d(\d+)([+-]\d+)?$", RegexOptions.IgnoreCase);
        if (!match.Success)
            return new RollParseResult { Success = false };

        if (!int.TryParse(match.Groups[1].Value, out int diceCount) || diceCount <= 0)
            return new RollParseResult { Success = false };

        if (!int.TryParse(match.Groups[2].Value, out int dieSize) || dieSize <= 0)
            return new RollParseResult { Success = false };

        int modifier = 0;
        if (match.Groups[3].Success && !int.TryParse(match.Groups[3].Value, out modifier))
            return new RollParseResult { Success = false };

        return new RollParseResult
        {
            Success = true,
            Request = new RollRequest
            {
                DiceCount = diceCount,
                DieSize = dieSize,
                Modifier = modifier,
                Advantage = false,
                Disadvantage = false
            }
        };
    }


    private bool TryParseDurationInput(string input, out long totalSeconds)
    {
        totalSeconds = 0;

        if (string.IsNullOrWhiteSpace(input))
            return false;

        string normalized = input.Trim().ToLowerInvariant().Replace(" ", "");

        MatchCollection matches = Regex.Matches(normalized, @"(\d+)([dhms])", RegexOptions.IgnoreCase);

        if (matches.Count == 0)
            return false;

        string rebuilt = string.Concat(matches.Select(m => m.Value));
        if (!string.Equals(rebuilt, normalized, StringComparison.OrdinalIgnoreCase))
            return false;

        long seconds = 0;

        foreach (Match match in matches)
        {
            if (!long.TryParse(match.Groups[1].Value, out long value))
                return false;

            switch (match.Groups[2].Value.ToLowerInvariant())
            {
                case "d":
                    seconds += value * 86400;
                    break;
                case "h":
                    seconds += value * 3600;
                    break;
                case "m":
                    seconds += value * 60;
                    break;
                case "s":
                    seconds += value;
                    break;
                default:
                    return false;
            }
        }

        totalSeconds = seconds;
        return true;
    }

    private RollResult ExecuteRoll(RollRequest request)
    {
        var result = new RollResult
        {
            RollLabel = $"{request.DiceCount}d{request.DieSize}" +
                        (request.Modifier == 0 ? "" : request.Modifier > 0 ? $"+{request.Modifier}" : $"{request.Modifier}"),
            ModeLabel = request.Advantage ? "Advantage" : request.Disadvantage ? "Disadvantage" : "Normal"
        };

        if (request.Advantage || request.Disadvantage)
        {
            int rollA = _random.Next(1, request.DieSize + 1);
            int rollB = _random.Next(1, request.DieSize + 1);

            result.AdvantageRolls.Add(rollA);
            result.AdvantageRolls.Add(rollB);

            int kept = request.Advantage
                ? Math.Max(rollA, rollB)
                : Math.Min(rollA, rollB);

            result.BaseRollTotal = kept;
            result.Total = kept + request.Modifier;
            return result;
        }

        int total = 0;

        for (int i = 0; i < request.DiceCount; i++)
        {
            int roll = _random.Next(1, request.DieSize + 1);
            result.IndividualRolls.Add(roll);
            total += roll;
        }

        result.BaseRollTotal = total;
        result.Total = total + request.Modifier;
        return result;
    }

    private async Task ButtonExecuted(SocketMessageComponent component)
    {
        string customId = component.Data.CustomId;

        if (customId.StartsWith("page:", StringComparison.Ordinal))
        {
            await HandlePaginatorButtonAsync(component);
            return;
        }

        if (customId != "delete_embed" && !customId.StartsWith("cycle_", StringComparison.Ordinal))
            return;

        if (!_relayStates.TryGetValue(component.Message.Id, out RelayMessageState? state))
        {
            await component.RespondAsync(
                "I no longer have state for this message. It may have been created before a restart or the state file is missing.",
                ephemeral: true);
            return;
        }

        if (component.User.Id != state.OriginalAuthorId)
        {
            await component.RespondAsync(
                "Only the original poster can use these buttons.",
                ephemeral: true);
            return;
        }

        (ulong MessageId, ulong UserId) cooldownKey = (component.Message.Id, component.User.Id);

        if (_cooldowns.TryGetValue(cooldownKey, out DateTime lastUsed))
        {
            TimeSpan elapsed = DateTime.UtcNow - lastUsed;

            if (elapsed < ButtonCooldown)
            {
                double remaining = (ButtonCooldown - elapsed).TotalSeconds;
                await component.RespondAsync(
                    $"Slow down a bit 😅 Try again in {remaining:F1}s.",
                    ephemeral: true);
                return;
            }
        }

        _cooldowns[cooldownKey] = DateTime.UtcNow;
        CleanupOldCooldowns();

        try
        {
            if (component.Channel is SocketTextChannel buttonChannel)
            {
                List<string> missing = GetLikelyMissingPermissions(buttonChannel);
                if (missing.Count > 0)
                {
                    Console.WriteLine(
                        $"[PRECHECK] Button action in guild '{buttonChannel.Guild.Name}' " +
                        $"channel '#{buttonChannel.Name}' may be missing: {string.Join(", ", missing)}");
                }
            }

            await component.DeferAsync(ephemeral: true);

            if (customId == "delete_embed")
            {
                var deleteClient = new DiscordWebhookClient(state.WebhookId, state.WebhookToken);
                await deleteClient.DeleteMessageAsync(component.Message.Id);

                _relayStates.Remove(component.Message.Id);
                SaveRelayStates();

                await component.FollowupAsync("Deleted your relayed message.", ephemeral: true);
                return;
            }

            string platform = customId.Replace("cycle_", "", StringComparison.Ordinal);

            if (!state.Platforms.Contains(platform))
            {
                await component.FollowupAsync(
                    "That platform is not present in this message.",
                    ephemeral: true);
                return;
            }

            if (!_providers.TryGetValue(platform, out List<string>? providers) || providers.Count == 0)
            {
                await component.FollowupAsync(
                    "No providers are configured for that platform.",
                    ephemeral: true);
                return;
            }

            int currentIndex = state.ProviderIndexes.TryGetValue(platform, out int idx) ? idx : 0;
            int nextIndex = currentIndex + 1;
            bool loopedBack = false;

            if (nextIndex >= providers.Count)
            {
                nextIndex = 0;
                loopedBack = true;
            }

            state.ProviderIndexes[platform] = nextIndex;

            string newContent = ApplyAllReplacements(state.OriginalContent, state.ProviderIndexes, state.OriginalAuthorId);

            var editClient = new DiscordWebhookClient(state.WebhookId, state.WebhookToken);

            await editClient.ModifyMessageAsync(component.Message.Id, props =>
            {
                props.Content = Optional.Create(newContent);
                props.Components = Optional.Create(BuildButtons(state.Platforms));
            });

            _relayStates[component.Message.Id] = state;
            SaveRelayStates();

            string responseText = loopedBack
                ? $"Tried all configured {FormatPlatformName(platform)} embed providers and looped back to the first."
                : $"Switched {FormatPlatformName(platform)} embed provider to {providers[nextIndex]}.";

            await component.FollowupAsync(responseText, ephemeral: true);
        }
        catch (Exception ex)
        {
            if (component.Channel is SocketTextChannel buttonChannel)
                LogPermissionFailure(buttonChannel, $"Button '{customId}'", ex);
            else
                Console.WriteLine($"Error handling button click: {ex}");

            try
            {
                await component.FollowupAsync(
                    "Something went wrong while handling that button.",
                    ephemeral: true);
            }
            catch
            {
            }
        }
    }


    private async Task SendPaginatedEmbedAsync(
        ISocketMessageChannel channel,
        string title,
        List<string> lines,
        string paginatorType,
        int page,
        int pageSize,
        Color color,
        string? headerText = null)
    {
        if (lines.Count == 0)
        {
            var emptyEmbed = new EmbedBuilder()
                .WithTitle(title)
                .WithDescription("Nothing to display.")
                .WithColor(color)
                .Build();

            await channel.SendMessageAsync(embed: emptyEmbed);
            return;
        }

        int totalPages = (int)Math.Ceiling(lines.Count / (double)pageSize);
        page = Math.Clamp(page, 0, Math.Max(0, totalPages - 1));

        List<string> pageLines = lines
            .Skip(page * pageSize)
            .Take(pageSize)
            .ToList();

        string description = string.Join("\n", pageLines);

        if (!string.IsNullOrWhiteSpace(headerText))
            description = $"{headerText}\n\n{description}";

        var embed = new EmbedBuilder()
            .WithTitle(title)
            .WithDescription(description)
            .WithColor(color)
            .WithFooter($"Page {page + 1}/{totalPages}")
            .Build();

        await channel.SendMessageAsync(
            embed: embed,
            components: BuildPaginatorComponents(paginatorType, page, totalPages));
    }

    private MessageComponent BuildPaginatorComponents(string paginatorType, int page, int totalPages)
    {
        bool hasPrevious = page > 0;
        bool hasNext = page < totalPages - 1;

        var builder = new ComponentBuilder()
            .WithButton("Previous", $"page:{paginatorType}:{page - 1}", ButtonStyle.Secondary, disabled: !hasPrevious)
            .WithButton("Next", $"page:{paginatorType}:{page + 1}", ButtonStyle.Primary, disabled: !hasNext);

        return builder.Build();
    }

    private async Task HandlePaginatorButtonAsync(SocketMessageComponent component)
    {
        try
        {
            string[] parts = component.Data.CustomId.Split(':', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 3)
            {
                await component.RespondAsync("Invalid paginator button.", ephemeral: true);
                return;
            }

            string paginatorType = parts[1];

            if (!int.TryParse(parts[2], out int page))
            {
                await component.RespondAsync("Invalid page value.", ephemeral: true);
                return;
            }

            List<string> lines;
            string title;
            string? headerText = null;
            Color color;

            switch (paginatorType)
            {
                case "botservers":
                    if (_client == null)
                    {
                        await component.RespondAsync("Client not ready.", ephemeral: true);
                        return;
                    }

                    lines = _client.Guilds
                        .OrderByDescending(g => g.MemberCount)
                        .ThenBy(g => g.Name)
                        .Select((g, i) => $"**{i + 1}.** {g.Name}\nID: `{g.Id}` | Members: **{g.MemberCount}**")
                        .ToList();
                    title = "ApolloBot Connected Servers";
                    headerText = $"**Total Servers:** {_client.Guilds.Count}\n**Total Users:** {_client.Guilds.Sum(g => g.MemberCount)}";
                    color = Color.Gold;
                    break;

                case "abhelp":
                    bool isAdmin = component.User is SocketGuildUser guildUser && guildUser.GuildPermissions.ManageGuild;
                    lines = BuildApolloBotHelpLines(isAdmin);
                    title = "📦 ApolloBot Commands";
                    headerText = "Embed fixing, support, and utility commands.";
                    color = Color.Teal;
                    break;

                case "bothelp":
                    lines = BuildBotOwnerHelpLines();
                    title = "👑 Bot Owner Commands";
                    headerText = "Owner-only controls and maintenance commands.";
                    color = Color.DarkPurple;
                    break;

                default:
                    await component.RespondAsync("Unknown paginator.", ephemeral: true);
                    return;
            }

            if (lines.Count == 0)
            {
                await component.RespondAsync("Nothing to display.", ephemeral: true);
                return;
            }

            int totalPages = (int)Math.Ceiling(lines.Count / (double)DefaultPageSize);
            page = Math.Clamp(page, 0, Math.Max(0, totalPages - 1));

            List<string> pageLines = lines
                .Skip(page * DefaultPageSize)
                .Take(DefaultPageSize)
                .ToList();

            string description = string.Join("\n", pageLines);

            if (!string.IsNullOrWhiteSpace(headerText))
                description = $"{headerText}\n\n{description}";

            var embed = new EmbedBuilder()
                .WithTitle(title)
                .WithDescription(description)
                .WithColor(color)
                .WithFooter($"Page {page + 1}/{totalPages}")
                .Build();

            await component.UpdateAsync(msg =>
            {
                msg.Embed = Optional.Create(embed);
                msg.Components = Optional.Create(BuildPaginatorComponents(paginatorType, page, totalPages));
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to handle paginator button: {ex}");

            try
            {
                await component.RespondAsync("Something went wrong while changing pages.", ephemeral: true);
            }
            catch
            {
            }
        }
    }

    private List<string> BuildBotOwnerHelpLines()
    {
        return new List<string>
        {
            "**Core Owner Commands**",
            "`!bot help` – Show this menu",
            "`!bot stats` – Show bot stats and uptime",
            "`!bot servercount` – Show total connected servers",
            "`!bot servers` – List connected servers with pagination",
            "`!bot topservers` – Show most-used servers by embed fixes",
            "`!bot serverstats <serverId>` – Show detailed stats for one server",
            "`!bot setembeds <number>` – Manually set the embeds fixed count",
            "`!bot setservicetime <duration>` – Manually set total service time",
            "",
            "**Bot Management**",
            "`!bot status <type> <status> <text>` – Change bot presence",
            "`!bot provider ...` – Manage platform providers",
            "`!bot special ...` – Manage silly Twitter/X users",
            "`!bot update ...` – Manage public planned updates"
        };
    }

    private List<string> BuildApolloBotHelpLines(bool isAdmin)
    {
        var lines = new List<string>
        {
            "**Public Commands**",
            "`!ab help` – Show this menu",
            "`!ab about` – What ApolloBot does",
            "`!ab ping` – Check if the bot is alive",
            "`!ab providers` – Show configured providers",
            "`!ab perms` – Check channel permissions",
            "`!ab status` – Show server settings",
            "`!support` – Open support / bug report page",
            "`!ab ignore on` – Ignore your embeds in this server",
            "`!ab ignore off` – Stop ignoring your embeds in this server",
            "`!ab ignore all` – Toggle ignore in all servers",
            "`/roll` – Roll dice"
        };

        if (isAdmin)
        {
            lines.Add("");
            lines.Add("**Admin Commands**");
            lines.Add("`!embedfix on` – Enable embed fixing");
            lines.Add("`!embedfix off` – Disable embed fixing");
            lines.Add("`!ab whitelist add #channel`");
            lines.Add("`!ab whitelist remove #channel`");
            lines.Add("`!ab whitelist list`");
            lines.Add("`!ab whitelist clear`");
        }

        lines.Add("");
        lines.Add("**Roll Examples**");
        lines.Add("`/roll`");
        lines.Add("`/roll dice:1d20`");
        lines.Add("`/roll dice:1d20+6`");
        lines.Add("`/roll dice:1d20 mode:advantage`");
        lines.Add("`/roll dice:1d20+4 mode:disadvantage`");
        lines.Add("`/roll dice:2d6+3`");

        return lines;
    }

    private GuildActivityState GetOrCreateGuildActivityState(SocketGuild guild)
    {
        if (_guildActivity.TryGetValue(guild.Id, out GuildActivityState? existing))
            return existing;

        var created = new GuildActivityState
        {
            GuildId = guild.Id,
            ServerName = guild.Name,
            LastKnownMemberCount = guild.MemberCount,
            OwnerId = guild.OwnerId,
            OwnerName = guild.Owner != null ? $"{guild.Owner.Username}#{guild.Owner.Discriminator}" : "Unknown",
            FirstSeenAtUtc = DateTime.UtcNow,
            LastJoinedAtUtc = DateTime.UtcNow,
            LastUpdatedAtUtc = DateTime.UtcNow
        };

        _guildActivity[guild.Id] = created;
        return created;
    }

    private void EnsureGuildUsageStatsEntry(SocketGuild guild)
    {
        if (_guildUsageStats.TryGetValue(guild.Id, out GuildUsageStats? existing))
        {
            existing.ServerName = guild.Name;
            existing.LastKnownMemberCount = guild.MemberCount;
            existing.LastUpdatedAtUtc = DateTime.UtcNow;
            return;
        }

        _guildUsageStats[guild.Id] = new GuildUsageStats
        {
            GuildId = guild.Id,
            ServerName = guild.Name,
            LastKnownMemberCount = guild.MemberCount,
            FirstSeenAtUtc = DateTime.UtcNow,
            LastUpdatedAtUtc = DateTime.UtcNow
        };
    }

    private void RecordGuildEmbedFix(SocketGuild guild)
    {
        EnsureGuildUsageStatsEntry(guild);

        GuildUsageStats stats = _guildUsageStats[guild.Id];
        stats.EmbedFixCount++;
        stats.ServerName = guild.Name;
        stats.LastKnownMemberCount = guild.MemberCount;
        stats.LastUsedAtUtc = DateTime.UtcNow;
        stats.LastUpdatedAtUtc = DateTime.UtcNow;

        SaveGuildUsageStats();
    }

    private async Task SendTopServersByUsageAsync(SocketTextChannel channel)
    {
        var top = _guildUsageStats.Values
            .OrderByDescending(x => x.EmbedFixCount)
            .ThenByDescending(x => x.LastUsedAtUtc)
            .Take(10)
            .ToList();

        if (top.Count == 0)
        {
            await channel.SendMessageAsync("No per-server embed usage has been recorded yet.");
            return;
        }

        string lines = string.Join("\n", top.Select((x, i) =>
            $"**{i + 1}.** {x.ServerName} (`{x.GuildId}`) — **{x.EmbedFixCount}** fixes"));

        var embed = new EmbedBuilder()
            .WithTitle("Top Servers by ApolloBot Usage")
            .WithDescription(lines)
            .WithColor(Color.DarkBlue)
            .WithCurrentTimestamp()
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }

    private async Task SendSingleServerStatsAsync(SocketTextChannel channel, string[] parts)
    {
        if (parts.Length < 3 || !ulong.TryParse(parts[2], out ulong guildId))
        {
            await channel.SendMessageAsync("Usage: `!bot serverstats <serverId>`");
            return;
        }

        _guildUsageStats.TryGetValue(guildId, out GuildUsageStats? usageStats);
        _guildActivity.TryGetValue(guildId, out GuildActivityState? activity);

        SocketGuild? liveGuild = _client?.GetGuild(guildId);

        if (usageStats == null && activity == null && liveGuild == null)
        {
            await channel.SendMessageAsync("I don't have any tracked data for that server ID.");
            return;
        }

        string serverName = liveGuild?.Name
            ?? usageStats?.ServerName
            ?? activity?.ServerName
            ?? "Unknown";

        int members = liveGuild?.MemberCount
            ?? usageStats?.LastKnownMemberCount
            ?? activity?.LastKnownMemberCount
            ?? 0;

        DateTime? joinedAt = activity?.LastJoinedAtUtc == default ? null : activity?.LastJoinedAtUtc;
        DateTime? removedAt = activity?.LastRemovedAtUtc == default ? null : activity?.LastRemovedAtUtc;
        DateTime? lastUsedAt = usageStats?.LastUsedAtUtc == default ? null : usageStats?.LastUsedAtUtc;

        string retentionText = "Still in server / unknown";
        if (joinedAt.HasValue && removedAt.HasValue && removedAt.Value >= joinedAt.Value)
            retentionText = FormatDuration(removedAt.Value - joinedAt.Value);

        var embed = new EmbedBuilder()
            .WithTitle("ApolloBot Server Stats")
            .AddField("Server", serverName, true)
            .AddField("Server ID", guildId.ToString(), true)
            .AddField("Members", members, true)
            .AddField("Embed Fixes", usageStats?.EmbedFixCount ?? 0, true)
            .AddField("Last Used", lastUsedAt.HasValue ? lastUsedAt.Value.ToString("yyyy-MM-dd HH:mm:ss 'UTC'") : "Never", true)
            .AddField("Joined At", joinedAt.HasValue ? joinedAt.Value.ToString("yyyy-MM-dd HH:mm:ss 'UTC'") : "Unknown", true)
            .AddField("Removed At", removedAt.HasValue ? removedAt.Value.ToString("yyyy-MM-dd HH:mm:ss 'UTC'") : "Still present / unknown", true)
            .AddField("Time In Server", retentionText, true)
            .WithColor(Color.DarkTeal)
            .WithCurrentTimestamp()
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }

    private async Task SyncGuildTrackingStateAsync()
    {
        if (_client == null)
            return;

        foreach (SocketGuild guild in _client.Guilds)
        {
            GuildActivityState activity = GetOrCreateGuildActivityState(guild);

            activity.ServerName = guild.Name;
            activity.LastKnownMemberCount = guild.MemberCount;
            activity.OwnerId = guild.OwnerId;
            activity.OwnerName = guild.Owner != null
                ? $"{guild.Owner.Username}#{guild.Owner.Discriminator}"
                : activity.OwnerName;

            if (activity.FirstSeenAtUtc == default)
                activity.FirstSeenAtUtc = DateTime.UtcNow;

            if (activity.LastJoinedAtUtc == default)
                activity.LastJoinedAtUtc = DateTime.UtcNow;

            activity.LastUpdatedAtUtc = DateTime.UtcNow;

            EnsureGuildUsageStatsEntry(guild);
        }

        SaveGuildActivityState();
        SaveGuildUsageStats();
    }

    private async Task SendGuildLifecycleLogAsync(SocketGuild guild, bool joined, GuildActivityState activity)
    {
        if (_ownerLogChannelId == 0 || _client == null)
            return;

        if (_client.GetChannel(_ownerLogChannelId) is not IMessageChannel channel)
            return;

        int liveServerCount = _client.Guilds.Count;
        int liveUserCount = _client.Guilds.Sum(x => x.MemberCount);

        string ownerText = activity.OwnerId == 0
            ? activity.OwnerName
            : $"{activity.OwnerName} (`{activity.OwnerId}`)";

        string retentionText = "Unknown";

        if (!joined && activity.LastJoinedAtUtc != default && activity.LastRemovedAtUtc != default && activity.LastRemovedAtUtc >= activity.LastJoinedAtUtc)
            retentionText = FormatDuration(activity.LastRemovedAtUtc - activity.LastJoinedAtUtc);

        var embed = new EmbedBuilder()
            .WithTitle(joined ? "ApolloBot Joined a Server" : "ApolloBot Left a Server")
            .WithColor(joined ? Color.Green : Color.Red)
            .AddField("Server", string.IsNullOrWhiteSpace(guild.Name) ? activity.ServerName : guild.Name, true)
            .AddField("Server ID", guild.Id.ToString(), true)
            .AddField("Members", guild.MemberCount > 0 ? guild.MemberCount : activity.LastKnownMemberCount, true)
            .AddField("Owner", ownerText, false)
            .AddField(joined ? "Joined At" : "Removed At", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss 'UTC'"), true)
            .AddField("Total Servers", liveServerCount, true)
            .AddField("Total Users", liveUserCount, true)
            .WithCurrentTimestamp();

        if (!joined)
            embed.AddField("Time In Server", retentionText, true);

        await channel.SendMessageAsync(embed: embed.Build());
    }

    private void SaveGuildActivityState()
    {
        try
        {
            string json = JsonSerializer.Serialize(_guildActivity, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(GuildActivityStateFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save guild activity state: {ex}");
        }
    }

    private void LoadGuildActivityState()
    {
        try
        {
            if (!File.Exists(GuildActivityStateFilePath))
                return;

            string json = File.ReadAllText(GuildActivityStateFilePath);
            Dictionary<ulong, GuildActivityState>? loaded =
                JsonSerializer.Deserialize<Dictionary<ulong, GuildActivityState>>(json);

            _guildActivity.Clear();

            if (loaded != null)
            {
                foreach ((ulong guildId, GuildActivityState state) in loaded)
                    _guildActivity[guildId] = state;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load guild activity state: {ex}");
        }
    }

    private void SaveGuildUsageStats()
    {
        try
        {
            string json = JsonSerializer.Serialize(_guildUsageStats, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(GuildUsageStatsFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save guild usage stats: {ex}");
        }
    }

    private void LoadGuildUsageStats()
    {
        try
        {
            if (!File.Exists(GuildUsageStatsFilePath))
                return;

            string json = File.ReadAllText(GuildUsageStatsFilePath);
            Dictionary<ulong, GuildUsageStats>? loaded =
                JsonSerializer.Deserialize<Dictionary<ulong, GuildUsageStats>>(json);

            _guildUsageStats.Clear();

            if (loaded != null)
            {
                foreach ((ulong guildId, GuildUsageStats stats) in loaded)
                    _guildUsageStats[guildId] = stats;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load guild usage stats: {ex}");
        }
    }

    private bool ShouldProcessMessageInChannel(SocketTextChannel channel)
    {
        GuildSettings settings = GetOrCreateGuildSettings(channel.Guild.Id);

        if (!settings.Enabled)
            return false;

        if (settings.WhitelistedChannelIds.Count == 0)
            return true;

        return settings.WhitelistedChannelIds.Contains(channel.Id);
    }

    private bool IsBotOwner(SocketUser user)
    {
        return BotOwnerIds.Contains(user.Id);
    }

    private GuildSettings GetOrCreateGuildSettings(ulong guildId)
    {
        if (_guildSettings.TryGetValue(guildId, out GuildSettings? existing))
            return existing;

        var created = new GuildSettings
        {
            GuildId = guildId,
            Enabled = true,
            WhitelistedChannelIds = new List<ulong>()
        };

        _guildSettings[guildId] = created;
        SaveGuildSettings();
        return created;
    }

    private UserIgnoreSettings GetOrCreateUserIgnoreSettings(ulong userId)
    {
        if (_userIgnoreSettings.TryGetValue(userId, out UserIgnoreSettings? existing))
            return existing;

        var created = new UserIgnoreSettings
        {
            UserId = userId,
            IgnoreAllServers = false,
            IgnoredGuildIds = new List<ulong>()
        };

        _userIgnoreSettings[userId] = created;
        SaveUserIgnoreSettings();
        return created;
    }

    private bool ShouldIgnoreUser(ulong guildId, ulong userId)
    {
        if (!_userIgnoreSettings.TryGetValue(userId, out UserIgnoreSettings? settings))
            return false;

        if (settings.IgnoreAllServers)
            return true;

        return settings.IgnoredGuildIds.Contains(guildId);
    }

    private bool IsIgnoredInGuild(UserIgnoreSettings settings, ulong guildId)
    {
        return settings.IgnoreAllServers || settings.IgnoredGuildIds.Contains(guildId);
    }

    private bool IsMissingPermissionsError(Exception ex)
    {
        return ex is HttpException httpEx &&
               httpEx.DiscordCode == DiscordErrorCode.MissingPermissions;
    }

    private List<string> GetLikelyMissingPermissions(SocketTextChannel channel)
    {
        var missing = new List<string>();

        if (_client?.CurrentUser == null)
            return missing;

        SocketGuildUser? botUser = channel.Guild.GetUser(_client.CurrentUser.Id);
        if (botUser == null)
            return missing;

        ChannelPermissions perms = botUser.GetPermissions(channel);
        GuildPermissions guildPerms = botUser.GuildPermissions;

        if (!perms.ViewChannel)
            missing.Add("View Channel");

        if (!perms.SendMessages)
            missing.Add("Send Messages");

        if (!perms.EmbedLinks)
            missing.Add("Embed Links");

        if (!perms.ReadMessageHistory)
            missing.Add("Read Message History");

        if (!perms.ManageMessages)
            missing.Add("Manage Messages");

        if (!perms.ManageWebhooks)
            missing.Add("Manage Webhooks");

        if (!guildPerms.UseApplicationCommands)
            missing.Add("Use Application Commands");

        return missing;
    }

    private void LogBotChannelPermissions(SocketTextChannel channel, string actionName)
    {
        if (_client?.CurrentUser == null)
        {
            Console.WriteLine($"[PERM CHECK] Cannot inspect permissions for action '{actionName}' because CurrentUser is null.");
            return;
        }

        SocketGuildUser? botUser = channel.Guild.GetUser(_client.CurrentUser.Id);

        if (botUser == null)
        {
            Console.WriteLine($"[PERM CHECK] Could not resolve bot user in guild '{channel.Guild.Name}' for action '{actionName}'.");
            return;
        }

        ChannelPermissions perms = botUser.GetPermissions(channel);
        GuildPermissions guildPerms = botUser.GuildPermissions;

        Console.WriteLine("========== BOT PERMISSION REPORT ==========");
        Console.WriteLine($"Action:                  {actionName}");
        Console.WriteLine($"Guild:                   {channel.Guild.Name} ({channel.Guild.Id})");
        Console.WriteLine($"Channel:                 #{channel.Name} ({channel.Id})");
        Console.WriteLine($"Bot User:                {botUser.Username} ({botUser.Id})");
        Console.WriteLine($"ViewChannel:             {perms.ViewChannel}");
        Console.WriteLine($"SendMessages:            {perms.SendMessages}");
        Console.WriteLine($"EmbedLinks:              {perms.EmbedLinks}");
        Console.WriteLine($"ManageMessages:          {perms.ManageMessages}");
        Console.WriteLine($"ManageWebhooks:          {perms.ManageWebhooks}");
        Console.WriteLine($"ReadHistory:             {perms.ReadMessageHistory}");
        Console.WriteLine($"UseApplicationCommands:  {guildPerms.UseApplicationCommands}");
        Console.WriteLine("===========================================");
    }

    private void LogPermissionFailure(SocketTextChannel channel, string actionName, Exception ex)
    {
        Console.WriteLine("********** PERMISSION FAILURE **********");
        Console.WriteLine($"Action:  {actionName}");
        Console.WriteLine($"Guild:   {channel.Guild.Name} ({channel.Guild.Id})");
        Console.WriteLine($"Channel: #{channel.Name} ({channel.Id})");
        Console.WriteLine($"Error:   {ex.GetType().Name}: {ex.Message}");

        if (IsMissingPermissionsError(ex))
            Console.WriteLine("Discord error code 50013 confirmed: Missing Permissions.");

        List<string> missing = GetLikelyMissingPermissions(channel);

        if (missing.Count == 0)
            Console.WriteLine("Likely missing permissions: None detected from current permission snapshot. Could be role hierarchy, denied overwrite, webhook-specific issue, or missing OAuth scope.");
        else
            Console.WriteLine($"Likely missing permissions: {string.Join(", ", missing)}");

        LogBotChannelPermissions(channel, actionName);
        Console.WriteLine("***************************************");
    }

    private MessageComponent BuildButtons(List<string> platforms)
    {
        var builder = new ComponentBuilder();

        foreach (string platform in platforms.Distinct())
        {
            builder.WithButton(
                label: GetButtonLabel(platform),
                customId: $"cycle_{platform}",
                style: ButtonStyle.Danger);
        }

        builder.WithButton(
            label: "Delete",
            customId: "delete_embed",
            style: ButtonStyle.Secondary);

        return builder.Build();
    }

    private string GetButtonLabel(string platform)
    {
        return platform switch
        {
            "twitter" => "Fix Twitter/X",
            "reddit" => "Fix Reddit",
            "tiktok" => "Fix TikTok",
            "instagram" => "Fix Instagram",
            _ => "Fix Embed"
        };
    }

    private string FormatPlatformName(string platform)
    {
        return platform switch
        {
            "twitter" => "Twitter/X",
            "reddit" => "Reddit",
            "tiktok" => "TikTok",
            "instagram" => "Instagram",
            _ => platform
        };
    }

    private string FormatDuration(TimeSpan span)
    {
        if (span.TotalDays >= 1)
            return $"{(int)span.TotalDays}d {span.Hours}h";

        if (span.TotalHours >= 1)
            return $"{(int)span.TotalHours}h {span.Minutes}m";

        if (span.TotalMinutes >= 1)
            return $"{span.Minutes}m";

        return $"{Math.Max(1, span.Seconds)}s";
    }

    private List<string> GetPlatformsInText(string text)
    {
        var platforms = new List<string>();

        if (ContainsTwitterLink(text))
            platforms.Add("twitter");

        if (ContainsRedditLink(text))
            platforms.Add("reddit");

        if (ContainsTikTokLink(text))
            platforms.Add("tiktok");

        if (ContainsInstagramLink(text))
            platforms.Add("instagram");

        return platforms;
    }

    private Dictionary<string, int> CreateDefaultProviderIndexes(List<string> platforms)
    {
        var indexes = new Dictionary<string, int>();

        foreach (string platform in platforms)
            indexes[platform] = 0;

        return indexes;
    }

    private string ApplyAllReplacements(string text, Dictionary<string, int> providerIndexes, ulong originalAuthorId)
    {
        string result = text;

        if (providerIndexes.ContainsKey("twitter"))
            result = ReplaceTwitterLinks(result, providerIndexes["twitter"], originalAuthorId);

        if (providerIndexes.ContainsKey("reddit"))
            result = ReplaceRedditLinks(result, providerIndexes["reddit"], originalAuthorId);

        if (providerIndexes.ContainsKey("tiktok"))
            result = ReplaceTikTokLinks(result, providerIndexes["tiktok"], originalAuthorId);

        if (providerIndexes.ContainsKey("instagram"))
            result = ReplaceInstagramLinks(result, providerIndexes["instagram"], originalAuthorId);

        return result;
    }

    private bool ContainsTwitterLink(string text)
    {
        return Regex.IsMatch(
            text,
            @"https?://(www\.)?(x\.com|twitter\.com)(/|$)",
            RegexOptions.IgnoreCase);
    }

    private bool ContainsRedditLink(string text)
    {
        return Regex.IsMatch(
            text,
            @"https?://(www\.)?reddit\.com(/|$)",
            RegexOptions.IgnoreCase);
    }

    private bool ContainsTikTokLink(string text)
    {
        return Regex.IsMatch(
            text,
            @"https?://((www|vm)\.)?tiktok\.com(/|$)",
            RegexOptions.IgnoreCase);
    }

    private bool ContainsInstagramLink(string text)
    {
        return Regex.IsMatch(
            text,
            @"https?://(www\.)?instagram\.com(/|$)",
            RegexOptions.IgnoreCase);
    }

    private string ReplaceTwitterLinks(string text, int providerIndex, ulong originalAuthorId)
    {
        List<string> twitterProviders = GetProvidersForPlatform("twitter", originalAuthorId);

        if (twitterProviders.Count == 0)
            return text;

        if (providerIndex < 0 || providerIndex >= twitterProviders.Count)
            providerIndex = 0;

        string replacementDomain = twitterProviders[providerIndex];

        text = Regex.Replace(
            text,
            @"https?://(www\.)?x\.com",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        text = Regex.Replace(
            text,
            @"https?://(www\.)?twitter\.com",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        return text;
    }

    private string ReplaceRedditLinks(string text, int providerIndex, ulong originalAuthorId)
    {
        List<string> redditProviders = GetProvidersForPlatform("reddit", originalAuthorId);

        if (redditProviders.Count == 0)
            return text;

        if (providerIndex < 0 || providerIndex >= redditProviders.Count)
            providerIndex = 0;

        string replacementDomain = redditProviders[providerIndex];

        text = Regex.Replace(
            text,
            @"https?://(www\.)?reddit\.com",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        return text;
    }

    private string ReplaceTikTokLinks(string text, int providerIndex, ulong originalAuthorId)
    {
        List<string> tikTokProviders = GetProvidersForPlatform("tiktok", originalAuthorId);

        if (tikTokProviders.Count == 0)
            return text;

        if (providerIndex < 0 || providerIndex >= tikTokProviders.Count)
            providerIndex = 0;

        string replacementDomain = tikTokProviders[providerIndex];

        text = Regex.Replace(
            text,
            @"https?://(www\.)?tiktok\.com",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        text = Regex.Replace(
            text,
            @"https?://vm\.tiktok\.com",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        return text;
    }

    private string ReplaceInstagramLinks(string text, int providerIndex, ulong originalAuthorId)
    {
        List<string> instagramProviders = GetProvidersForPlatform("instagram", originalAuthorId);

        if (instagramProviders.Count == 0)
            return text;

        if (providerIndex < 0 || providerIndex >= instagramProviders.Count)
            providerIndex = 0;

        string replacementDomain = instagramProviders[providerIndex];

        text = Regex.Replace(
            text,
            @"https?://(www\.)?instagram\.com",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        return text;
    }

    private List<string> GetProvidersForPlatform(string platform, ulong originalAuthorId)
    {
        if (platform == "twitter" && _specialTwitterUsers.Contains(originalAuthorId))
        {
            var special = new List<string>
            {
                "stupidpenisx.com",
                "girlcockx.com"
            };

            if (_providers.TryGetValue("twitter", out List<string>? normalTwitterProviders))
            {
                foreach (string provider in normalTwitterProviders)
                {
                    if (!special.Any(x => string.Equals(x, provider, StringComparison.OrdinalIgnoreCase)))
                        special.Add(provider);
                }
            }

            return special;
        }

        if (_providers.TryGetValue(platform, out List<string>? providers))
            return providers;

        return new List<string>();
    }

    private void SaveRelayStates()
    {
        try
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };

            string json = JsonSerializer.Serialize(_relayStates, options);
            File.WriteAllText(StateFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save relay states: {ex}");
        }
    }

    private void LoadRelayStates()
    {
        try
        {
            if (!File.Exists(StateFilePath))
            {
                Console.WriteLine("No relay state file found. Starting fresh.");
                return;
            }

            string json = File.ReadAllText(StateFilePath);

            Dictionary<ulong, RelayMessageState>? loadedStates =
                JsonSerializer.Deserialize<Dictionary<ulong, RelayMessageState>>(json);

            if (loadedStates == null)
            {
                Console.WriteLine("Relay state file was empty or invalid. Starting fresh.");
                return;
            }

            _relayStates.Clear();

            foreach ((ulong messageId, RelayMessageState state) in loadedStates)
                _relayStates[messageId] = state;

            Console.WriteLine($"Loaded {_relayStates.Count} relay state(s) from disk.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load relay states: {ex}");
        }
    }

    private void IncrementEmbedsFixedCount()
    {
        lock (_statsLock)
        {
            _embedsFixedCount++;
        }

        SaveBotStatsHeartbeat();
    }

    private void RegisterShutdownHandlers()
    {
        AppDomain.CurrentDomain.ProcessExit += (_, _) => FinalizeCurrentSession("ProcessExit");
        Console.CancelKeyPress += (_, _) => FinalizeCurrentSession("CancelKeyPress");
    }

    private long GetCurrentSessionSeconds()
    {
        lock (_statsLock)
        {
            return Math.Max(0, (long)(DateTime.UtcNow - _sessionStartedAtUtc).TotalSeconds);
        }
    }

    private long GetTotalUptimeSeconds()
    {
        lock (_statsLock)
        {
            long currentSessionSeconds = Math.Max(0, (long)(DateTime.UtcNow - _sessionStartedAtUtc).TotalSeconds);
            return _accumulatedUptimeSeconds + currentSessionSeconds;
        }
    }

    private void SaveBotStatsHeartbeat()
    {
        try
        {
            BotStatsState state;

            lock (_statsLock)
            {
                _lastHeartbeatUtc = DateTime.UtcNow;

                state = new BotStatsState
                {
                    EmbedsFixedCount = _embedsFixedCount,
                    AccumulatedUptimeSeconds = _accumulatedUptimeSeconds,
                    CurrentSessionStartedAtUtc = _sessionStartedAtUtc,
                    LastHeartbeatUtc = _lastHeartbeatUtc,
                    UptimeHistory = _uptimeHistory
                        .OrderByDescending(x => x.EndedAtUtc)
                        .Take(100)
                        .ToList()
                };
            }

            string json = JsonSerializer.Serialize(state, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(BotStatsStateFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save bot stats state: {ex}");
        }
    }

    private void FinalizeCurrentSession(string reason)
    {
        try
        {
            lock (_statsLock)
            {
                DateTime endUtc = _lastHeartbeatUtc > _sessionStartedAtUtc
                    ? _lastHeartbeatUtc
                    : DateTime.UtcNow;

                long durationSeconds = Math.Max(0, (long)(endUtc - _sessionStartedAtUtc).TotalSeconds);

                if (durationSeconds > 0)
                {
                    _accumulatedUptimeSeconds += durationSeconds;
                    _uptimeHistory.Insert(0, new UptimeSession
                    {
                        StartedAtUtc = _sessionStartedAtUtc,
                        EndedAtUtc = endUtc,
                        DurationSeconds = durationSeconds
                    });

                    _uptimeHistory = _uptimeHistory
                        .OrderByDescending(x => x.EndedAtUtc)
                        .Take(100)
                        .ToList();
                }

                _sessionStartedAtUtc = DateTime.UtcNow;
                _lastHeartbeatUtc = _sessionStartedAtUtc;
            }

            SaveBotStatsHeartbeat();
            Console.WriteLine($"Finalized uptime session due to {reason}.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to finalize current session during {reason}: {ex}");
        }
    }

    private void LoadBotStatsState()
    {
        try
        {
            DateTime now = DateTime.UtcNow;

            if (!File.Exists(BotStatsStateFilePath))
            {
                Console.WriteLine("No bot stats state file found. Starting fresh.");
                lock (_statsLock)
                {
                    _sessionStartedAtUtc = now;
                    _lastHeartbeatUtc = now;
                }

                SaveBotStatsHeartbeat();
                return;
            }

            string json = File.ReadAllText(BotStatsStateFilePath);

            BotStatsState? loaded =
                JsonSerializer.Deserialize<BotStatsState>(json);

            if (loaded == null)
            {
                Console.WriteLine("Bot stats state file invalid. Starting fresh.");
                lock (_statsLock)
                {
                    _sessionStartedAtUtc = now;
                    _lastHeartbeatUtc = now;
                }

                SaveBotStatsHeartbeat();
                return;
            }

            lock (_statsLock)
            {
                _embedsFixedCount = loaded.EmbedsFixedCount;
                _accumulatedUptimeSeconds = loaded.AccumulatedUptimeSeconds;
                _uptimeHistory = loaded.UptimeHistory ?? new List<UptimeSession>();

                DateTime previousStartUtc = loaded.CurrentSessionStartedAtUtc;
                DateTime previousHeartbeatUtc = loaded.LastHeartbeatUtc;

                if (previousStartUtc == default && loaded.LegacyLastStartedAtUtc != default)
                {
                    previousStartUtc = loaded.LegacyLastStartedAtUtc;
                }

                if (previousHeartbeatUtc == default)
                {
                    DateTime fileWriteUtc = File.GetLastWriteTimeUtc(BotStatsStateFilePath);
                    if (fileWriteUtc != default)
                        previousHeartbeatUtc = fileWriteUtc;
                }

                if (previousStartUtc != default &&
                    previousHeartbeatUtc != default &&
                    previousHeartbeatUtc >= previousStartUtc)
                {
                    long previousSessionSeconds = Math.Max(0, (long)(previousHeartbeatUtc - previousStartUtc).TotalSeconds);

                    if (previousSessionSeconds > 0)
                    {
                        bool alreadyTracked = _uptimeHistory.Any(x =>
                            x.StartedAtUtc == previousStartUtc &&
                            x.EndedAtUtc == previousHeartbeatUtc &&
                            x.DurationSeconds == previousSessionSeconds);

                        if (!alreadyTracked)
                        {
                            _accumulatedUptimeSeconds += previousSessionSeconds;
                            _uptimeHistory.Insert(0, new UptimeSession
                            {
                                StartedAtUtc = previousStartUtc,
                                EndedAtUtc = previousHeartbeatUtc,
                                DurationSeconds = previousSessionSeconds
                            });
                        }
                    }
                }

                _uptimeHistory = _uptimeHistory
                    .OrderByDescending(x => x.EndedAtUtc)
                    .Take(100)
                    .ToList();

                _sessionStartedAtUtc = now;
                _lastHeartbeatUtc = now;

                Console.WriteLine($"Loaded embeds fixed count: {_embedsFixedCount}");
                Console.WriteLine($"Loaded accumulated uptime: {_accumulatedUptimeSeconds}s");
                Console.WriteLine($"Loaded uptime history entries: {_uptimeHistory.Count}");
            }

            SaveBotStatsHeartbeat();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load bot stats state: {ex}");

            lock (_statsLock)
            {
                _sessionStartedAtUtc = DateTime.UtcNow;
                _lastHeartbeatUtc = _sessionStartedAtUtc;
            }
        }
    }

    private async Task NotifyOriginalAuthorOfReplyAsync(SocketUserMessage replyMessage, SocketTextChannel textChannel)
    {
        try
        {
            if (replyMessage.Reference?.MessageId.IsSpecified != true)
                return;

            ulong referencedMessageId = replyMessage.Reference.MessageId.Value;

            if (!_relayStates.TryGetValue(referencedMessageId, out RelayMessageState? relayState))
                return;

            if (replyMessage.Author.Id == relayState.OriginalAuthorId)
                return;

            string jumpUrl = $"https://discord.com/channels/{textChannel.Guild.Id}/{textChannel.Id}/{replyMessage.Id}";
            string replierName = replyMessage.Author.GlobalName ?? replyMessage.Author.Username;
            string originalMention = $"<@{relayState.OriginalAuthorId}>";

            var allowedMentions = AllowedMentions.None;
            allowedMentions.UserIds = new List<ulong> { relayState.OriginalAuthorId };

            IUserMessage pingMessage = await textChannel.SendMessageAsync(
                text: $"{originalMention} **{replierName}** replied to your relayed message: {jumpUrl}",
                allowedMentions: allowedMentions);

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(12));
                    await pingMessage.DeleteAsync();
                }
                catch
                {
                }
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to notify original author about a reply: {ex}");
        }
    }

    private async Task StartStatsHttpServerAsync()
    {
        try
        {
            int port = 8080;

            string? portValue = Environment.GetEnvironmentVariable("PORT");
            if (!string.IsNullOrWhiteSpace(portValue) && int.TryParse(portValue, out int parsedPort))
                port = parsedPort;

            var listener = new TcpListener(IPAddress.Any, port);
            listener.Start();

            Console.WriteLine($"Stats HTTP server listening on port {port}");

            while (true)
            {
                TcpClient client = await listener.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleStatsHttpClientAsync(client));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Stats HTTP server crashed: {ex}");
        }
    }

    private async Task HandleStatsHttpClientAsync(TcpClient client)
    {
        using (client)
        using (NetworkStream stream = client.GetStream())
        using (var reader = new StreamReader(stream, Encoding.ASCII, leaveOpen: true))
        {
            try
            {
                string? requestLine = await reader.ReadLineAsync();
                if (string.IsNullOrWhiteSpace(requestLine))
                    return;

                string[] parts = requestLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                string method = parts.Length > 0 ? parts[0] : "GET";
                string path = parts.Length > 1 ? parts[1] : "/";

                string? line;
                do
                {
                    line = await reader.ReadLineAsync();
                }
                while (!string.IsNullOrEmpty(line));

                if (method.Equals("OPTIONS", StringComparison.OrdinalIgnoreCase))
                {
                    await WriteHttpResponseAsync(
                        stream,
                        "204 No Content",
                        "text/plain; charset=utf-8",
                        "");
                    return;
                }

                if (path.StartsWith("/stats", StringComparison.OrdinalIgnoreCase))
                {
                string json = BuildPublicStatsJson();
                await WriteHttpResponseAsync(
                        stream,
                        "200 OK",
                        "application/json; charset=utf-8",
                        json);

                    return;
                }

                if (path == "/")
                {
                    await WriteHttpResponseAsync(
                        stream,
                        "200 OK",
                        "text/plain; charset=utf-8",
                        "ApolloBot stats endpoint is live. Use /stats");
                    return;
                }

                await WriteHttpResponseAsync(
                    stream,
                    "404 Not Found",
                    "application/json; charset=utf-8",
                    "{\"error\":\"Not found\"}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to handle stats HTTP request: {ex}");
            }
        }
    }

    private async Task WriteHttpResponseAsync(NetworkStream stream, string status, string contentType, string body)
    {
        byte[] bodyBytes = Encoding.UTF8.GetBytes(body);

        string headers =
            $"HTTP/1.1 {status}\r\n" +
            $"Content-Type: {contentType}\r\n" +
            $"Content-Length: {bodyBytes.Length}\r\n" +
            $"Access-Control-Allow-Origin: *\r\n" +
            $"Access-Control-Allow-Methods: GET, OPTIONS\r\n" +
            $"Access-Control-Allow-Headers: Content-Type\r\n" +
            $"Connection: close\r\n" +
            $"\r\n";

        byte[] headerBytes = Encoding.UTF8.GetBytes(headers);

        await stream.WriteAsync(headerBytes, 0, headerBytes.Length);
        await stream.WriteAsync(bodyBytes, 0, bodyBytes.Length);
        await stream.FlushAsync();
    }

    private string BuildPublicStatsJson()
    {
        int serverCount = _client?.Guilds.Count ?? 0;
        int totalUsers = _client?.Guilds.Sum(g => g.MemberCount) ?? 0;

        long embedsFixed;
        long currentSessionSeconds;
        long totalUptimeSeconds;
        long longestSessionSeconds;
        int restartCount;
        DateTime activeSessionStartedAtUtc;
        DateTime lastHeartbeatUtc;
        List<UptimeSession> history;

        lock (_statsLock)
        {
            embedsFixed = _embedsFixedCount;
            currentSessionSeconds = Math.Max(0, (long)(DateTime.UtcNow - _sessionStartedAtUtc).TotalSeconds);
            totalUptimeSeconds = _accumulatedUptimeSeconds + currentSessionSeconds;
            longestSessionSeconds = _uptimeHistory.Count == 0
                ? currentSessionSeconds
                : Math.Max(_uptimeHistory.Max(x => x.DurationSeconds), currentSessionSeconds);
            restartCount = _uptimeHistory.Count;
            activeSessionStartedAtUtc = _sessionStartedAtUtc;
            lastHeartbeatUtc = _lastHeartbeatUtc;
            history = _uptimeHistory.Take(10).ToList();
        }

        var payload = new PublicStatsPayload
        {
            EmbedsFixed = embedsFixed,
            ServerCount = serverCount,
            TotalUsers = totalUsers,
            TrackedUsageServers = _guildUsageStats.Count,
            Uptime = FormatDuration(TimeSpan.FromSeconds(totalUptimeSeconds)),
            PlatformCount = _providers.Count,
            TotalUptimeSeconds = totalUptimeSeconds,
            CurrentSessionSeconds = currentSessionSeconds,
            LongestSessionSeconds = longestSessionSeconds,
            RestartCount = restartCount,
            ActiveSessionStartedAtUtc = activeSessionStartedAtUtc,
            LastHeartbeatUtc = lastHeartbeatUtc,
            UptimeHistory = history
        };

        return JsonSerializer.Serialize(payload, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        });
    }

    private void LoadPresenceSettings()
    {
        try
        {
            if (!File.Exists(PresenceFilePath))
            {
                SavePresenceSettings();
                return;
            }

            string json = File.ReadAllText(PresenceFilePath);
            BotPresenceSettings? loaded = JsonSerializer.Deserialize<BotPresenceSettings>(json);

            if (loaded != null)
                _presenceSettings = loaded;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load presence settings: {ex}");
        }
    }

    private void SavePresenceSettings()
    {
        try
        {
            string json = JsonSerializer.Serialize(_presenceSettings, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(PresenceFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save presence settings: {ex}");
        }
    }

    private async Task ApplyPresenceAsync()
    {
        if (_client == null)
            return;

        UserStatus status = _presenceSettings.Status switch
        {
            "idle" => UserStatus.Idle,
            "dnd" => UserStatus.DoNotDisturb,
            "invisible" => UserStatus.Invisible,
            _ => UserStatus.Online
        };

        await _client.SetStatusAsync(status);

        ActivityType type = _presenceSettings.Type switch
        {
            "playing" => ActivityType.Playing,
            "listening" => ActivityType.Listening,
            "streaming" => ActivityType.Streaming,
            _ => ActivityType.Watching
        };

        if (type == ActivityType.Streaming)
            await _client.SetGameAsync(_presenceSettings.Text, _presenceSettings.StreamUrl, ActivityType.Streaming);
        else
            await _client.SetGameAsync(_presenceSettings.Text, null, type);
    }


    private static Dictionary<string, List<string>> CreateDefaultProviders()
    {
        return new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase)
        {
            {
                "twitter",
                new List<string>
                {
                    "vxtwitter.com",
                    "fxtwitter.com",
                    "fixvx.com",
                    "fixupx.com"
                }
            },
            {
                "reddit",
                new List<string>
                {
                    "vxreddit.com",
                    "rxddit.com"
                }
            },
            {
                "tiktok",
                new List<string>
                {
                    "tnktok.com",
                    "tiktxk.com",
                    "fixtiktok.com"
                }
            },
            {
                "instagram",
                new List<string>
                {
                    "eeinstagram.com",
                    "kkinstagram.com"
                }
            }
        };
    }

    private void LoadProviders()
    {
        try
        {
            if (!File.Exists(ProvidersFilePath))
            {
                _providers = CreateDefaultProviders();
                SaveProviders();
                return;
            }

            string json = File.ReadAllText(ProvidersFilePath);
            Dictionary<string, List<string>>? loaded =
                JsonSerializer.Deserialize<Dictionary<string, List<string>>>(json);

            _providers = loaded ?? CreateDefaultProviders();

            foreach ((string key, List<string> defaults) in CreateDefaultProviders())
            {
                if (!_providers.ContainsKey(key))
                    _providers[key] = new List<string>(defaults);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load providers: {ex}");
            _providers = CreateDefaultProviders();
        }
    }

    private void SaveProviders()
    {
        try
        {
            string json = JsonSerializer.Serialize(_providers, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(ProvidersFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save providers: {ex}");
        }
    }

    private void LoadSpecialTwitterUsers()
    {
        try
        {
            if (!File.Exists(SpecialTwitterUsersFilePath))
                return;

            string json = File.ReadAllText(SpecialTwitterUsersFilePath);
            HashSet<ulong>? loaded = JsonSerializer.Deserialize<HashSet<ulong>>(json);

            _specialTwitterUsers.Clear();

            if (loaded != null)
            {
                foreach (ulong userId in loaded)
                    _specialTwitterUsers.Add(userId);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load special Twitter users: {ex}");
        }
    }

    private void SaveSpecialTwitterUsers()
    {
        try
        {
            string json = JsonSerializer.Serialize(_specialTwitterUsers, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(SpecialTwitterUsersFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save special Twitter users: {ex}");
        }
    }

    private void LoadPlannedUpdates()
    {
        try
        {
            if (!File.Exists(PlannedUpdatesFilePath))
                return;

            string json = File.ReadAllText(PlannedUpdatesFilePath);
            SortedDictionary<int, string>? loaded =
                JsonSerializer.Deserialize<SortedDictionary<int, string>>(json);

            _plannedUpdates.Clear();

            if (loaded != null)
            {
                foreach ((int id, string value) in loaded)
                    _plannedUpdates[id] = value;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load planned updates: {ex}");
        }
    }

    private void SavePlannedUpdates()
    {
        try
        {
            string json = JsonSerializer.Serialize(_plannedUpdates, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(PlannedUpdatesFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save planned updates: {ex}");
        }
    }

    private void SaveGuildSettings()
    {
        try
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };

            string json = JsonSerializer.Serialize(_guildSettings, options);
            File.WriteAllText(GuildSettingsFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save guild settings: {ex}");
        }
    }

    private void LoadGuildSettings()
    {
        try
        {
            if (!File.Exists(GuildSettingsFilePath))
            {
                Console.WriteLine("No guild settings file found. Starting fresh.");
                return;
            }

            string json = File.ReadAllText(GuildSettingsFilePath);

            Dictionary<ulong, GuildSettings>? loaded =
                JsonSerializer.Deserialize<Dictionary<ulong, GuildSettings>>(json);

            if (loaded == null)
            {
                Console.WriteLine("Guild settings file was empty or invalid. Starting fresh.");
                return;
            }

            _guildSettings.Clear();

            foreach ((ulong guildId, GuildSettings settings) in loaded)
                _guildSettings[guildId] = settings;

            Console.WriteLine($"Loaded {_guildSettings.Count} guild setting profile(s) from disk.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load guild settings: {ex}");
        }
    }

    private void SaveUserIgnoreSettings()
    {
        try
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };

            string json = JsonSerializer.Serialize(_userIgnoreSettings, options);
            File.WriteAllText(UserIgnoreSettingsFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save user ignore settings: {ex}");
        }
    }

    private void LoadUserIgnoreSettings()
    {
        try
        {
            if (!File.Exists(UserIgnoreSettingsFilePath))
            {
                Console.WriteLine("No user ignore settings file found. Starting fresh.");
                return;
            }

            string json = File.ReadAllText(UserIgnoreSettingsFilePath);

            Dictionary<ulong, UserIgnoreSettings>? loaded =
                JsonSerializer.Deserialize<Dictionary<ulong, UserIgnoreSettings>>(json);

            if (loaded == null)
            {
                Console.WriteLine("User ignore settings file was empty or invalid. Starting fresh.");
                return;
            }

            _userIgnoreSettings.Clear();

            foreach ((ulong userId, UserIgnoreSettings settings) in loaded)
                _userIgnoreSettings[userId] = settings;

            Console.WriteLine($"Loaded {_userIgnoreSettings.Count} user ignore profile(s) from disk.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load user ignore settings: {ex}");
        }
    }

    private void CleanupOldCooldowns()
    {
        DateTime cutoff = DateTime.UtcNow - CooldownRetention;

        List<(ulong MessageId, ulong UserId)> staleKeys = _cooldowns
            .Where(pair => pair.Value < cutoff)
            .Select(pair => pair.Key)
            .ToList();

        foreach ((ulong MessageId, ulong UserId) key in staleKeys)
            _cooldowns.Remove(key);
    }
}

class GuildActivityState
{
    public ulong GuildId { get; set; }
    public string ServerName { get; set; } = "Unknown";
    public int LastKnownMemberCount { get; set; }
    public ulong OwnerId { get; set; }
    public string OwnerName { get; set; } = "Unknown";
    public DateTime FirstSeenAtUtc { get; set; }
    public DateTime LastJoinedAtUtc { get; set; }
    public DateTime LastRemovedAtUtc { get; set; }
    public DateTime LastUpdatedAtUtc { get; set; }
}

class GuildUsageStats
{
    public ulong GuildId { get; set; }
    public string ServerName { get; set; } = "Unknown";
    public long EmbedFixCount { get; set; }
    public int LastKnownMemberCount { get; set; }
    public DateTime FirstSeenAtUtc { get; set; }
    public DateTime LastUsedAtUtc { get; set; }
    public DateTime LastUpdatedAtUtc { get; set; }
}

class BotPresenceSettings
{
    public string Text { get; set; } = "Running 24/7";
    public string Type { get; set; } = "watching";
    public string? StreamUrl { get; set; }
    public string Status { get; set; } = "online";
}

class RelayMessageState
{
    public string OriginalContent { get; set; } = "";
    public ulong WebhookId { get; set; }
    public string WebhookToken { get; set; } = "";
    public ulong OriginalAuthorId { get; set; }
    public List<string> Platforms { get; set; } = new();
    public Dictionary<string, int> ProviderIndexes { get; set; } = new();
}

class RollRequest
{
    public int DiceCount { get; set; }
    public int DieSize { get; set; }
    public int Modifier { get; set; }
    public bool Advantage { get; set; }
    public bool Disadvantage { get; set; }
}

class RollResult
{
    public string RollLabel { get; set; } = "";
    public string ModeLabel { get; set; } = "Normal";
    public int BaseRollTotal { get; set; }
    public int Total { get; set; }
    public List<int> IndividualRolls { get; set; } = new();
    public List<int> AdvantageRolls { get; set; } = new();
}

class RollParseResult
{
    public bool Success { get; set; }
    public RollRequest? Request { get; set; }
}

class BotStatsState
{
    public long EmbedsFixedCount { get; set; }
    public long AccumulatedUptimeSeconds { get; set; }
    public DateTime CurrentSessionStartedAtUtc { get; set; }
    public DateTime LastHeartbeatUtc { get; set; }
    public DateTime LegacyLastStartedAtUtc { get; set; }
    public List<UptimeSession> UptimeHistory { get; set; } = new();
}

class UptimeSession
{
    public DateTime StartedAtUtc { get; set; }
    public DateTime EndedAtUtc { get; set; }
    public long DurationSeconds { get; set; }
}

class PublicStatsPayload
{
    public long EmbedsFixed { get; set; }
    public int ServerCount { get; set; }
    public int TotalUsers { get; set; }
    public int TrackedUsageServers { get; set; }
    public string Uptime { get; set; } = "";
    public int PlatformCount { get; set; }
    public long TotalUptimeSeconds { get; set; }
    public long CurrentSessionSeconds { get; set; }
    public long LongestSessionSeconds { get; set; }
    public int RestartCount { get; set; }
    public DateTime ActiveSessionStartedAtUtc { get; set; }
    public DateTime LastHeartbeatUtc { get; set; }
    public List<UptimeSession> UptimeHistory { get; set; } = new();
}

class GuildSettings
{
    public ulong GuildId { get; set; }
    public bool Enabled { get; set; } = true;
    public List<ulong> WhitelistedChannelIds { get; set; } = new();
}

class UserIgnoreSettings
{
    public ulong UserId { get; set; }
    public bool IgnoreAllServers { get; set; } = false;
    public List<ulong> IgnoredGuildIds { get; set; } = new();
}
