using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using Discord;
using Discord.Audio;
using Discord.Net;
using Discord.Rest;
using Discord.Webhook;
using Discord.WebSocket;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
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
    private readonly LocalizationManager _localization = new(Path.Combine(AppContext.BaseDirectory, "Localization"));
    private DiscordSocketClient? _client;
    private readonly Random _random = Random.Shared;
    private static readonly HttpClient AnimalHttpClient = new() { Timeout = TimeSpan.FromSeconds(10) };
    private static readonly HttpClient TopGgHttpClient = new() { Timeout = TimeSpan.FromSeconds(15) };
    private readonly ConcurrentDictionary<ulong, DateTime> _animalCooldowns = new();
    private readonly ConcurrentDictionary<string, string> _lastAnimalImageUrls = new(StringComparer.OrdinalIgnoreCase);
    private bool _slashCommandsRegistered = false;
    private int _topGgStatsLoopStarted = 0;
    private long _embedsFixedCount = 0;
    private long _accumulatedUptimeSeconds = 0;
    private DateTime _sessionStartedAtUtc = DateTime.UtcNow;
    private DateTime _lastHeartbeatUtc = DateTime.UtcNow;
    private List<UptimeSession> _uptimeHistory = new();
    private readonly object _statsLock = new();

    // Set TEST_GUILD_ID for fast guild-only slash command registration during development.
    // Leave it unset or 0 in production so commands register globally.
    private static readonly ulong TestGuildId =
        ulong.TryParse(Environment.GetEnvironmentVariable("TEST_GUILD_ID"), out ulong parsedTestGuildId)
            ? parsedTestGuildId
            : 0;

    private Dictionary<string, List<string>> _providers = CreateDefaultProviders();
    private readonly HashSet<ulong> _specialTwitterUsers = new();
    private readonly SortedDictionary<int, string> _plannedUpdates = new();

    private readonly ConcurrentDictionary<ulong, RelayMessageState> _relayStates = new();
    private readonly ConcurrentDictionary<ulong, RestWebhook> _webhookCache = new();
    private readonly ConcurrentDictionary<ulong, SemaphoreSlim> _webhookLocks = new();
    private readonly SemaphoreSlim _relayConcurrency = new(8, 8);
    private readonly object _relayStateFileLock = new();
    private readonly object _guildUsageFileLock = new();
    private int _relayStateSaveScheduled;
    private int _guildUsageSaveScheduled;
    private readonly object _cooldownsLock = new();
    private readonly Dictionary<(ulong MessageId, ulong UserId), DateTime> _cooldowns = new();
    private readonly ConcurrentDictionary<ulong, GuildSettings> _guildSettings = new();
    private readonly ConcurrentDictionary<ulong, UserIgnoreSettings> _userIgnoreSettings = new();

    // Servers in this list are excluded from public/displayed server and user counts.
    // Useful for bot-listing/advertising servers where regular members cannot use ApolloBot.
    private readonly HashSet<ulong> _statsExcludedGuildIds = new()
    {
        110373943822540800
    };

    // Guilds the owner has explicitly blocked. ApolloBot immediately leaves if added again.
    private readonly HashSet<ulong> _blockedGuildIds = new();
    private readonly object _blockedGuildsLock = new();

    // Keeps voice connections alive so Discord.Net does not drop the bot after a few seconds.
    private readonly Dictionary<ulong, IAudioClient> _voiceConnections = new();
    private readonly Dictionary<ulong, CancellationTokenSource> _voiceKeepAliveTokens = new();

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

    private static readonly string UserUsageStatsFilePath =
        Path.Combine(DataDirectory, "user_usage_stats.json");

    private static readonly string StatsExcludedGuildsFilePath =
        Path.Combine(DataDirectory, "stats_excluded_guilds.json");

    private static readonly string BlockedGuildsFilePath =
        Path.Combine(DataDirectory, "blocked_guilds.json");

    private readonly ConcurrentDictionary<ulong, GuildActivityState> _guildActivity = new();
    private readonly ConcurrentDictionary<ulong, GuildUsageStats> _guildUsageStats = new();
    private readonly ConcurrentDictionary<ulong, UserUsageStats> _userUsageStats = new();

    private readonly ulong _ownerLogChannelId =
        ulong.TryParse(Environment.GetEnvironmentVariable("OWNER_LOG_CHANNEL_ID"), out ulong parsedOwnerLogChannelId)
            ? parsedOwnerLogChannelId
            : 0;

    private readonly string _supportUrl =
        Environment.GetEnvironmentVariable("APOLLOBOT_SUPPORT_URL")?.Trim() ?? "";

    private const int DefaultPageSize = 10;

    private BotPresenceSettings _presenceSettings = new();

    private const ulong ApolloBotCreatorUserId = 846147700700610600;

    private static readonly HashSet<ulong> BotOwnerIds = new()
    {
        127877921464385537,
        ApolloBotCreatorUserId
    };

    private static readonly TimeSpan CooldownRetention = TimeSpan.FromMinutes(10);

    static Task Main(string[] args)
    {
        DataPathHelper.EnsureAndLog();
        return new Program().MainAsync();
    }

    public async Task MainAsync()
    {
        Directory.CreateDirectory(DataDirectory);
        _localization.Load();

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
        LoadUserUsageStats();
        LoadStatsExcludedGuilds();
        LoadBlockedGuilds();
        RegisterShutdownHandlers();

        _client = new DiscordSocketClient(new DiscordSocketConfig
        {
            GatewayIntents =
                GatewayIntents.Guilds |
                GatewayIntents.GuildMessages |
                GatewayIntents.MessageContent |
                GatewayIntents.GuildVoiceStates
        });

        _client.Log += Log;
        _client.Ready += OnReady;
        _client.JoinedGuild += OnJoinedGuild;
        _client.LeftGuild += OnLeftGuild;
        _client.MessageReceived += MessageReceived;
        _client.ButtonExecuted += ButtonExecuted;
        _client.SelectMenuExecuted += SelectMenuExecuted;
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
                try
                {
                    SaveBotStatsHeartbeat();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[HEARTBEAT] Stats heartbeat failed: {ex}");
                }
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

        // Report the current server count to Top.gg as soon as Discord is ready.
        await ReportTopGgStatsAsync();

        // Ready can fire again after a reconnect, so only start one background reporter.
        if (Interlocked.Exchange(ref _topGgStatsLoopStarted, 1) == 0)
            _ = Task.Run(TopGgStatsLoopAsync);
    }

    private async Task TopGgStatsLoopAsync()
    {
        while (true)
        {
            await Task.Delay(TimeSpan.FromMinutes(30));
            await ReportTopGgStatsAsync();
        }
    }

    private async Task ReportTopGgStatsAsync()
    {
        if (_client?.CurrentUser == null)
            return;

        string? topGgToken = Environment.GetEnvironmentVariable("TOPGG_TOKEN")?.Trim();
        if (string.IsNullOrWhiteSpace(topGgToken))
        {
            Console.WriteLine("[TOP.GG] TOPGG_TOKEN is not set; skipping server-count update.");
            return;
        }

        try
        {
            int serverCount = _client.Guilds.Count;
            string endpoint = $"https://top.gg/api/bots/{_client.CurrentUser.Id}/stats";
            string json = JsonSerializer.Serialize(new { server_count = serverCount });

            using var request = new HttpRequestMessage(HttpMethod.Post, endpoint);
            request.Headers.TryAddWithoutValidation("Authorization", topGgToken);
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");

            using HttpResponseMessage response = await TopGgHttpClient.SendAsync(request);
            if (response.IsSuccessStatusCode)
            {
                Console.WriteLine($"[TOP.GG] Server count updated: {serverCount}");
                return;
            }

            string responseBody = await response.Content.ReadAsStringAsync();
            if (responseBody.Length > 300)
                responseBody = responseBody[..300];

            Console.WriteLine($"[TOP.GG] Update failed: {(int)response.StatusCode} {response.ReasonPhrase}. {responseBody}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TOP.GG] Server-count update failed: {ex.Message}");
        }
    }


    private async Task OnJoinedGuild(SocketGuild guild)
    {
        try
        {
            Console.WriteLine($"[JOIN] Joined guild: {guild.Name} ({guild.Id})");

            if (IsGuildBlocked(guild.Id))
            {
                Console.WriteLine($"[BLOCKLIST] Rejoined blocked guild '{guild.Name}' ({guild.Id}); leaving immediately.");
                await guild.LeaveAsync();
                return;
            }

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

            GuildSettings welcomeSettings = GetOrCreateGuildSettings(guild.Id);

            var embed = new EmbedBuilder()
                .WithTitle(L(welcomeSettings, "welcome.title"))
                .WithDescription(L(welcomeSettings, "welcome.description"))
                .WithColor(Color.Red)
                .Build();

            var welcomeComponents = new ComponentBuilder()
                .WithButton(L(welcomeSettings, "language.button"), $"serversetup_language:{guild.Id}", ButtonStyle.Secondary, emote: new Emoji("🌐"))
                .Build();

            await channel.SendMessageAsync(embed: embed, components: welcomeComponents);
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

    private Dictionary<string, string> SlashDescriptionLocalizations(string key)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (string language in _localization.AvailableLanguages)
        {
            if (language.Equals(LocalizationManager.DefaultLanguage, StringComparison.OrdinalIgnoreCase))
                continue;

            string discordLocale = language.ToLowerInvariant() switch
            {
                "de-de" => "de",
                "fr-fr" => "fr",
                "en-us" => "en-US",
                "es-es" => "es-ES",
                "pt-br" => "pt-BR",
                _ => language
            };

            result[discordLocale] = _localization.Get(key, language);
        }
        return result;
    }

    private Dictionary<string, string> SlashChoiceLocalizations(string key) => SlashDescriptionLocalizations(key);

    private async Task RegisterSlashCommandsAsync()
    {
        if (_client == null)
            return;

        var sharedCommandContexts = new[]
        {
            InteractionContextType.Guild,
            InteractionContextType.BotDm,
            InteractionContextType.PrivateChannel
        };

        var sharedIntegrationTypes = new[]
        {
            ApplicationIntegrationType.GuildInstall,
            ApplicationIntegrationType.UserInstall
        };

        var rollCommand = new SlashCommandBuilder()
            .WithName("roll")
            .WithDescription(LT("en-GB", "slash.roll")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.roll"))
            .WithContextTypes(sharedCommandContexts)
            .WithIntegrationTypes(sharedIntegrationTypes)
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("dice")
                .WithDescription(LT("en-GB", "slash.option_dice")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_dice"))
                .WithType(ApplicationCommandOptionType.String)
                .WithRequired(false))
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("mode")
                .WithDescription(LT("en-GB", "slash.option_mode")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_mode"))
                .WithType(ApplicationCommandOptionType.String)
                .WithRequired(false)
                .AddChoice("normal", "normal", SlashChoiceLocalizations("slash.choice_normal"))
                .AddChoice("advantage", "advantage", SlashChoiceLocalizations("slash.choice_advantage"))
                .AddChoice("disadvantage", "disadvantage", SlashChoiceLocalizations("slash.choice_disadvantage")))
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("exhaustion")
                .WithDescription(LT("en-GB", "slash.option_exhaustion")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_exhaustion"))
                .WithType(ApplicationCommandOptionType.Integer)
                .WithRequired(false)
                .WithMinValue(0)
                .WithMaxValue(6))
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("resistant")
                .WithDescription(LT("en-GB", "slash.option_resistant")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_resistant"))
                .WithType(ApplicationCommandOptionType.Boolean)
                .WithRequired(false))
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("vulnerable")
                .WithDescription(LT("en-GB", "slash.option_vulnerable")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_vulnerable"))
                .WithType(ApplicationCommandOptionType.Boolean)
                .WithRequired(false));

        var fixCommand = new SlashCommandBuilder()
            .WithName("fix")
            .WithDescription(LT("en-GB", "slash.fix")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.fix"))
            .WithContextTypes(sharedCommandContexts)
            .WithIntegrationTypes(sharedIntegrationTypes)
            .AddOption(new SlashCommandOptionBuilder()
                .WithName("url")
                .WithDescription(LT("en-GB", "slash.option_url")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_url"))
                .WithType(ApplicationCommandOptionType.String)
                .WithRequired(true));

        var guildContexts = new[] { InteractionContextType.Guild };
        var guildInstall = new[] { ApplicationIntegrationType.GuildInstall };

        SlashCommandBuilder SimpleGuildCommand(string name, string key) =>
            new SlashCommandBuilder().WithName(name)
                .WithDescription(LT("en-GB", key))
                .WithDescriptionLocalizations(SlashDescriptionLocalizations(key))
                .WithContextTypes(guildContexts).WithIntegrationTypes(guildInstall);

        // Admin commands are hidden from members who do not have Manage Server.
        // Discord applies this in the slash-command picker before the interaction reaches ApolloBot.
        SlashCommandBuilder AdminGuildCommand(string name, string description) =>
            SimpleGuildCommand(name, description).WithDefaultMemberPermissions(GuildPermission.ManageGuild);

        var infoCommand = SimpleGuildCommand("info", "slash.info")
            .AddOption("message", ApplicationCommandOptionType.String, LT("en-GB", "slash.option_message"), isRequired: true, descriptionLocalizations: SlashDescriptionLocalizations("slash.option_message"));
        var userStatsCommand = SimpleGuildCommand("userstats", "slash.userstats")
            .AddOption("user", ApplicationCommandOptionType.User, LT("en-GB", "slash.option_user"), isRequired: false, descriptionLocalizations: SlashDescriptionLocalizations("slash.option_user"));
        var embedFixCommand = AdminGuildCommand("embedfix", "slash.embedfix")
            .AddOption("enabled", ApplicationCommandOptionType.Boolean, LT("en-GB", "slash.option_embed_enabled"), isRequired: true, descriptionLocalizations: SlashDescriptionLocalizations("slash.option_embed_enabled"));
        var silentCommand = AdminGuildCommand("silent", "slash.silent")
            .AddOption("enabled", ApplicationCommandOptionType.Boolean, LT("en-GB", "slash.option_silent_enabled"), isRequired: true, descriptionLocalizations: SlashDescriptionLocalizations("slash.option_silent_enabled"));
        var toggleButtonsCommand = AdminGuildCommand("togglebuttons", "slash.togglebuttons")
            .AddOption("enabled", ApplicationCommandOptionType.Boolean, LT("en-GB", "slash.option_buttons_enabled"), isRequired: true, descriptionLocalizations: SlashDescriptionLocalizations("slash.option_buttons_enabled"));
        var cooldownCommand = AdminGuildCommand("cooldown", "slash.cooldown")
            .AddOption(new SlashCommandOptionBuilder().WithName("seconds").WithDescription(LT("en-GB", "slash.option_cooldown")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_cooldown")).WithType(ApplicationCommandOptionType.Integer).WithRequired(true).WithMinValue(1).WithMaxValue(30));
        var whitelistCommand = AdminGuildCommand("whitelist", "slash.whitelist")
            .AddOption(new SlashCommandOptionBuilder().WithName("action").WithDescription(LT("en-GB", "slash.option_action")).WithDescriptionLocalizations(SlashDescriptionLocalizations("slash.option_action")).WithType(ApplicationCommandOptionType.String).WithRequired(true).AddChoice("add", "add", SlashChoiceLocalizations("slash.choice_add")).AddChoice("remove", "remove", SlashChoiceLocalizations("slash.choice_remove")).AddChoice("list", "list", SlashChoiceLocalizations("slash.choice_list")).AddChoice("clear", "clear", SlashChoiceLocalizations("slash.choice_clear")))
            .AddOption("channel", ApplicationCommandOptionType.Channel, LT("en-GB", "slash.option_channel"), isRequired: false, descriptionLocalizations: SlashDescriptionLocalizations("slash.option_channel"));

        ApplicationCommandProperties[] commands = new ApplicationCommandProperties[]
        {
            rollCommand.Build(), fixCommand.Build(),
            SimpleGuildCommand("help", "slash.help").Build(),
            SimpleGuildCommand("about", "slash.about").Build(),
            SimpleGuildCommand("updates", "slash.updates").Build(),
            SimpleGuildCommand("support", "slash.support").Build(),
            SimpleGuildCommand("vote", "slash.vote").Build(),
            SimpleGuildCommand("ping", "slash.ping").Build(),
            SimpleGuildCommand("providers", "slash.providers").Build(),
            SimpleGuildCommand("fox", "slash.fox").Build(),
            SimpleGuildCommand("cat", "slash.cat").Build(),
            SimpleGuildCommand("dog", "slash.dog").Build(),
            SimpleGuildCommand("perms", "slash.perms").Build(),
            SimpleGuildCommand("status", "slash.status").Build(),
            infoCommand.Build(), userStatsCommand.Build(),
            SimpleGuildCommand("serverstats", "slash.serverstats").Build(),
            SimpleGuildCommand("usersettings", "slash.usersettings").Build(),
            AdminGuildCommand("setup", "slash.setup").Build(),
            embedFixCommand.Build(), silentCommand.Build(), toggleButtonsCommand.Build(), cooldownCommand.Build(), whitelistCommand.Build(),
            AdminGuildCommand("reset", "slash.reset").Build()
        };

        try
        {
            Console.WriteLine("[SLASH] Forcing global slash command registration for DM compatibility.");

            if (TestGuildId != 0)
            {
                SocketGuild? guild = _client.GetGuild(TestGuildId);
                if (guild != null)
                {
                    Console.WriteLine($"[SLASH] Clearing stale guild slash commands from test guild: {guild.Name} ({guild.Id})");
                    await guild.BulkOverwriteApplicationCommandAsync(Array.Empty<ApplicationCommandProperties>());
                }
                else
                {
                    Console.WriteLine($"[SLASH] TEST_GUILD_ID was set to {TestGuildId}, but that guild was not found. Continuing with global registration anyway.");
                }
            }

            Console.WriteLine("[SLASH] Clearing existing global slash commands so Discord refreshes command metadata and contexts.");
            await _client.BulkOverwriteGlobalApplicationCommandsAsync(commands);

            Console.WriteLine("[SLASH] Registered global slash commands: /fix, /roll, public commands, user settings, and server admin commands.");
            Console.WriteLine("[SLASH] Commands were registered with Guild + User install support and Guild/Bot DM/Private Channel contexts.");
            Console.WriteLine("[SLASH] Global commands should now be eligible for DM visibility, subject to Discord install/context propagation.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to register slash commands: {ex}");
        }
    }

    private Task MessageReceived(SocketMessage message)
    {
        // Keep Discord.Net's gateway event loop free. REST/webhook work happens independently.
        _ = Task.Run(() => ProcessMessageReceivedAsync(message));
        return Task.CompletedTask;
    }

    private async Task ProcessMessageReceivedAsync(SocketMessage message)
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
            if (await StopIfOptionalCommandDisabledAsync(textChannel, "vote")) return;
            await SendVoteMessage(textChannel, userMessage.Author);
            return;
        }

        if (content.Equals("!updates", StringComparison.OrdinalIgnoreCase) ||
            content.Equals("!ab updates", StringComparison.OrdinalIgnoreCase))
        {
            if (await StopIfOptionalCommandDisabledAsync(textChannel, "updates")) return;
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

        GuildSettings settings = GetOrCreateGuildSettings(textChannel.Guild.Id);

        string originalContent = message.Content;
        List<string> detectedPlatforms = GetPlatformsInText(originalContent);

        if (detectedPlatforms.Count == 0)
            return;

        Dictionary<string, int> providerIndexes = CreateDefaultProviderIndexes(detectedPlatforms, message.Author.Id);
        string newContent = ApplyAllReplacements(originalContent, providerIndexes, message.Author.Id);

        if (newContent == originalContent)
            return;

        bool relaySlotTaken = false;
        try
        {
            relaySlotTaken = await _relayConcurrency.WaitAsync(TimeSpan.FromSeconds(15));
            if (!relaySlotTaken)
            {
                Console.WriteLine($"[RELAY] Dropping delayed relay in #{textChannel.Name} ({textChannel.Id}); Discord REST work is saturated.");
                return;
            }

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

            (string displayName, string avatarUrl) = GetRelayIdentity(message.Author, textChannel.Guild);

            var webhookClient = new DiscordWebhookClient(webhook.Id, webhook.Token);

            MessageComponent buttons = BuildButtonsForGuild(textChannel.Guild.Id, detectedPlatforms, settings.SilentMode);

            ulong relayedMessageId = await webhookClient.SendMessageAsync(
                text: newContent,
                username: displayName,
                avatarUrl: avatarUrl,
                components: buttons
            );

            _relayStates[relayedMessageId] = new RelayMessageState
            {
                OriginalContent = originalContent,
                WebhookId = webhook.Id,
                WebhookToken = webhook.Token,
                OriginalAuthorId = message.Author.Id,
                SilentMode = settings.SilentMode,
                GuildId = textChannel.Guild.Id,
                Platforms = detectedPlatforms,
                ProviderIndexes = providerIndexes
            };

            SaveRelayStates();
            IncrementEmbedsFixedCount();
            RecordGuildEmbedFix(textChannel.Guild, detectedPlatforms);
            RecordUserEmbedFix(message.Author, textChannel.Guild.Id, detectedPlatforms);

            await message.DeleteAsync();
        }
        catch (Exception ex)
        {
            // If the webhook was deleted or became invalid, force a fresh lookup next time.
            InvalidateWebhookCache(textChannel.Id);
            LogPermissionFailure(textChannel, "Relaying message", ex);
        }
        finally
        {
            if (relaySlotTaken)
                _relayConcurrency.Release();
        }
    }

    private async Task<RestWebhook?> GetOrCreateWebhookAsync(SocketTextChannel textChannel)
    {
        if (_webhookCache.TryGetValue(textChannel.Id, out RestWebhook? cached) &&
            !string.IsNullOrWhiteSpace(cached.Token))
        {
            return cached;
        }

        SemaphoreSlim channelLock = _webhookLocks.GetOrAdd(textChannel.Id, _ => new SemaphoreSlim(1, 1));
        await channelLock.WaitAsync();

        try
        {
            // Another relay may have populated the cache while this one was waiting.
            if (_webhookCache.TryGetValue(textChannel.Id, out cached) &&
                !string.IsNullOrWhiteSpace(cached.Token))
            {
                return cached;
            }

            IReadOnlyCollection<RestWebhook> webhooks = await textChannel.GetWebhooksAsync();
            RestWebhook? webhook = webhooks.FirstOrDefault(w => w.Name == WebhookName);

            if (webhook == null)
                webhook = await textChannel.CreateWebhookAsync(WebhookName);

            if (webhook != null && !string.IsNullOrWhiteSpace(webhook.Token))
            {
                _webhookCache[textChannel.Id] = webhook;
                Console.WriteLine($"[WEBHOOK] Cached relay webhook for #{textChannel.Name} ({textChannel.Id}).");
            }

            return webhook;
        }
        finally
        {
            channelLock.Release();
        }
    }

    private void InvalidateWebhookCache(ulong channelId)
    {
        _webhookCache.TryRemove(channelId, out _);
    }

    private async Task HandleBotCommand(SocketUserMessage message, SocketTextChannel textChannel)
    {
        string[] parts = message.Content
            .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length < 2)
        {
            await SendBotHelp(textChannel, message.Author.Id);
            return;
        }

        string sub = parts[1].ToLowerInvariant();

        if (sub == "help")
        {
            await SendBotHelp(textChannel, message.Author.Id);
            return;
        }

        if (sub == "provider")
        {
            await HandleProviderCommand(textChannel, message.Author.Id, parts);
            return;
        }

        if (sub == "special")
        {
            await HandleSpecialTwitterUserCommand(textChannel, message.Author.Id, parts);
            return;
        }

        if (sub == "update")
        {
            await HandlePlannedUpdateOwnerCommand(textChannel, message.Author.Id, parts);
            return;
        }

        if (sub == "status")
        {
            if (parts.Length < 5)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, 
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
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Invalid type. Use: playing, watching, listening, or streaming.");
                return;
            }

            if (status is not ("online" or "idle" or "dnd" or "invisible"))
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Invalid status. Use: online, idle, dnd, or invisible.");
                return;
            }

            if (type == "streaming")
            {
                if (parts.Length < 6)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id, 
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
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Status text cannot be empty.");
                return;
            }

            _presenceSettings.Type = type;
            _presenceSettings.Status = status;
            _presenceSettings.Text = textValue;
            _presenceSettings.StreamUrl = streamUrl;

            SavePresenceSettings();
            await ApplyPresenceAsync();

            await SendBotOwnerMessageAsync(textChannel, message.Author.Id, $"✅ Status updated to **{type} {textValue}**");
            return;
        }

        if (sub == "chat")
        {
            if (_client == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Client not ready.");
                return;
            }

            if (parts.Length < 5 ||
                !ulong.TryParse(parts[2], out ulong targetGuildId) ||
                !ulong.TryParse(parts[3], out ulong targetChannelId))
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    "Usage: `!bot chat <ServerID> <ChannelID> <Message>`");
                return;
            }

            string chatMessage = string.Join(" ", parts.Skip(4)).Trim();
            if (string.IsNullOrWhiteSpace(chatMessage))
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Message cannot be empty.");
                return;
            }

            if (chatMessage.Length > 2000)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Discord messages cannot exceed 2000 characters.");
                return;
            }

            SocketGuild? targetGuild = _client.GetGuild(targetGuildId);
            if (targetGuild == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    $"ApolloBot is not connected to server `{targetGuildId}`.");
                return;
            }

            SocketTextChannel? targetChannel = targetGuild.GetTextChannel(targetChannelId);
            if (targetChannel == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    $"Could not find text channel `{targetChannelId}` in **{targetGuild.Name}**.");
                return;
            }

            try
            {
                SocketGuildUser? botUser = targetGuild.GetUser(_client.CurrentUser.Id);
                if (botUser == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        "Could not resolve ApolloBot's permissions in the target server.");
                    return;
                }

                ChannelPermissions perms = botUser.GetPermissions(targetChannel);
                if (!perms.ViewChannel || !perms.SendMessages)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"ApolloBot cannot send messages in <#{targetChannelId}>. " +
                        $"View Channel: **{perms.ViewChannel}**, Send Messages: **{perms.SendMessages}**.");
                    return;
                }

                await targetChannel.SendMessageAsync(chatMessage);

                // Keep the owner command itself out of the test/control channel when possible.
                try
                {
                    await message.DeleteAsync();
                }
                catch
                {
                }

                Console.WriteLine(
                    $"[OWNER CHAT] {message.Author} sent a message through ApolloBot to " +
                    $"'{targetGuild.Name}' ({targetGuild.Id}) / '#{targetChannel.Name}' ({targetChannel.Id}).");

            }
            catch (Exception ex)
            {
                Console.WriteLine($"[OWNER CHAT] Failed to send remote chat message: {ex}");
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    $"Failed to send the message to **{targetGuild.Name}** → <#{targetChannel.Id}>.");
            }

            return;
        }

        if (sub == "delete")
        {
            if (_client == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Client not ready.");
                return;
            }

            SocketGuild? targetGuild;
            SocketTextChannel? targetChannel;
            ulong targetMessageId;

            if (parts.Length == 3 && ulong.TryParse(parts[2], out targetMessageId))
            {
                targetGuild = textChannel.Guild;
                targetChannel = textChannel;
            }
            else if (parts.Length == 5 &&
                     ulong.TryParse(parts[2], out ulong targetGuildId) &&
                     ulong.TryParse(parts[3], out ulong targetChannelId) &&
                     ulong.TryParse(parts[4], out targetMessageId))
            {
                targetGuild = _client.GetGuild(targetGuildId);
                if (targetGuild == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"ApolloBot is not connected to server `{targetGuildId}`.");
                    return;
                }

                targetChannel = targetGuild.GetTextChannel(targetChannelId);
                if (targetChannel == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"Could not find text channel `{targetChannelId}` in **{targetGuild.Name}**.");
                    return;
                }
            }
            else
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    "Usage:\n`!bot delete <MessageID>`\n`!bot delete <ServerID> <ChannelID> <MessageID>`");
                return;
            }

            try
            {
                SocketGuildUser? botUser = targetGuild.GetUser(_client.CurrentUser.Id);
                if (botUser == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        "Could not resolve ApolloBot's permissions in the target server.");
                    return;
                }

                ChannelPermissions perms = botUser.GetPermissions(targetChannel);
                if (!perms.ViewChannel || !perms.ReadMessageHistory)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"ApolloBot cannot access message history in <#{targetChannel.Id}>. " +
                        $"View Channel: **{perms.ViewChannel}**, Read Message History: **{perms.ReadMessageHistory}**.");
                    return;
                }

                IMessage? targetMessage = await targetChannel.GetMessageAsync(targetMessageId);
                if (targetMessage == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"Could not find message `{targetMessageId}` in <#{targetChannel.Id}>.");
                    return;
                }

                if (targetMessage.Author.Id != _client.CurrentUser.Id)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"Refusing to delete message `{targetMessageId}` because it was not sent by ApolloBot.");
                    return;
                }

                await targetMessage.DeleteAsync();

                try
                {
                    if (message.Id != targetMessageId)
                        await message.DeleteAsync();
                }
                catch
                {
                }

                Console.WriteLine(
                    $"[OWNER DELETE] {message.Author} deleted ApolloBot message {targetMessageId} from " +
                    $"'{targetGuild.Name}' ({targetGuild.Id}) / '#{targetChannel.Name}' ({targetChannel.Id}).");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[OWNER DELETE] Failed to delete message: {ex}");
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    $"Failed to delete ApolloBot message `{targetMessageId}` from **{targetGuild.Name}** → <#{targetChannel.Id}>.");
            }

            return;
        }

        if (sub == "reply")
        {
            if (_client == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Client not ready.");
                return;
            }

            if (parts.Length < 6 ||
                !ulong.TryParse(parts[2], out ulong targetGuildId) ||
                !ulong.TryParse(parts[3], out ulong targetChannelId) ||
                !ulong.TryParse(parts[4], out ulong targetMessageId))
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    "Usage: `!bot reply <ServerID> <ChannelID> <MessageID> <Message>`");
                return;
            }

            string replyText = string.Join(" ", parts.Skip(5)).Trim();
            if (string.IsNullOrWhiteSpace(replyText))
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Reply cannot be empty.");
                return;
            }

            if (replyText.Length > 2000)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Discord messages cannot exceed 2000 characters.");
                return;
            }

            SocketGuild? targetGuild = _client.GetGuild(targetGuildId);
            if (targetGuild == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    $"ApolloBot is not connected to server `{targetGuildId}`.");
                return;
            }

            SocketTextChannel? targetChannel = targetGuild.GetTextChannel(targetChannelId);
            if (targetChannel == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    $"Could not find text channel `{targetChannelId}` in **{targetGuild.Name}**.");
                return;
            }

            try
            {
                SocketGuildUser? botUser = targetGuild.GetUser(_client.CurrentUser.Id);
                if (botUser == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        "Could not resolve ApolloBot's permissions in the target server.");
                    return;
                }

                ChannelPermissions perms = botUser.GetPermissions(targetChannel);
                if (!perms.ViewChannel || !perms.SendMessages || !perms.ReadMessageHistory)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"ApolloBot cannot reply in <#{targetChannelId}>. " +
                        $"View Channel: **{perms.ViewChannel}**, Send Messages: **{perms.SendMessages}**, " +
                        $"Read Message History: **{perms.ReadMessageHistory}**.");
                    return;
                }

                IMessage? targetMessage = await targetChannel.GetMessageAsync(targetMessageId);
                if (targetMessage == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"Could not find message `{targetMessageId}` in <#{targetChannelId}>.");
                    return;
                }

                await targetChannel.SendMessageAsync(
                    text: replyText,
                    messageReference: new MessageReference(targetMessageId, targetChannelId, targetGuildId));

                try
                {
                    await message.DeleteAsync();
                }
                catch
                {
                }

                Console.WriteLine(
                    $"[OWNER REPLY] {message.Author} replied through ApolloBot to message {targetMessageId} in " +
                    $"'{targetGuild.Name}' ({targetGuild.Id}) / '#{targetChannel.Name}' ({targetChannel.Id}).");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[OWNER REPLY] Failed to send remote reply: {ex}");
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                    $"Failed to reply to message `{targetMessageId}` in **{targetGuild.Name}** → <#{targetChannel.Id}>.");
            }

            return;
        }

        if (sub == "unblock")
        {
            if (parts.Length < 3 || !ulong.TryParse(parts[2], out ulong guildIdToUnblock))
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Usage: `!bot unblock <ServerID>`");
                return;
            }

            bool removed = RemoveBlockedGuild(guildIdToUnblock);
            await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                removed ? $"Removed `{guildIdToUnblock}` from ApolloBot's blocklist."
                        : $"`{guildIdToUnblock}` was not on ApolloBot's blocklist.");
            return;
        }

        if (sub == "blocklist")
        {
            List<ulong> blocked;
            lock (_blockedGuildsLock)
                blocked = _blockedGuildIds.OrderBy(id => id).ToList();

            string text = blocked.Count == 0
                ? "ApolloBot's server blocklist is empty."
                : "**Blocked server IDs:**\n" + string.Join("\n", blocked.Select(id => $"• `{id}`"));

            await SendBotOwnerMessageAsync(textChannel, message.Author.Id, text);
            return;
        }

        if (sub == "servercount")
        {
            int visibleCount = GetVisibleServerCount();
            int totalCount = _client?.Guilds.Count ?? 0;
            await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                $"🌐 Public stats show **{visibleCount}** server(s). Actual connected servers: **{totalCount}**.");
            return;
        }

        if (sub == "exclude")
        {
            await HandleStatsExclusionCommandAsync(textChannel, message.Author.Id, parts, exclude: true);
            return;
        }

        if (sub == "include")
        {
            await HandleStatsExclusionCommandAsync(textChannel, message.Author.Id, parts, exclude: false);
            return;
        }

        if (sub == "exclusions")
        {
            await SendStatsExclusionsAsync(textChannel, message.Author.Id);
            return;
        }



        if (sub == "join")
        {
            if (_client == null)
                return;

            SocketVoiceChannel? targetVc = null;

            // !bot join
            // Joins the voice channel the owner is currently connected to.
            if (parts.Length == 2)
            {
                if (message.Author is SocketGuildUser guildUser)
                    targetVc = guildUser.VoiceChannel;
            }
            // !bot join <VoiceChannelID>
            // Joins a specific voice channel by ID if the owner is not in VC.
            else if (parts.Length >= 3)
            {
                if (ulong.TryParse(parts[2], out ulong vcId))
                    targetVc = textChannel.Guild.GetChannel(vcId) as SocketVoiceChannel;
            }

            try
            {
                // Delete first so the owner command does not linger in chat.
                await message.DeleteAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[VOICE DEBUG] Failed to delete join command: {ex}");
            }

            if (targetVc == null)
            {
                Console.WriteLine("[VOICE DEBUG] Join requested, but no valid target voice channel was found.");
                return;
            }

            SocketVoiceChannel voiceChannelToJoin = targetVc;

            // Important: ConnectAsync needs gateway events to complete.
            // Running it directly inside MessageReceived can block the gateway and cause a timeout.
            _ = Task.Run(async () =>
            {
                try
                {
                    ulong guildId = voiceChannelToJoin.Guild.Id;

                    Console.WriteLine(
                        $"[VOICE DEBUG] Background voice join starting. VC='{voiceChannelToJoin.Name}' ({voiceChannelToJoin.Id}) " +
                        $"Guild='{voiceChannelToJoin.Guild.Name}' ({guildId})");

                    // If already connected in this guild, disconnect first so the stored client is fresh.
                    if (_voiceKeepAliveTokens.TryGetValue(guildId, out CancellationTokenSource? existingKeepAlive))
                    {
                        existingKeepAlive.Cancel();
                        existingKeepAlive.Dispose();
                        _voiceKeepAliveTokens.Remove(guildId);
                        Console.WriteLine($"[VOICE DEBUG] Existing keep-alive token cancelled for guild {guildId}.");
                    }

                    if (_voiceConnections.TryGetValue(guildId, out IAudioClient? existingClient))
                    {
                        try
                        {
                            await existingClient.StopAsync();
                            Console.WriteLine($"[VOICE DEBUG] Existing voice client stopped for guild {guildId}.");
                        }
                        catch (Exception stopEx)
                        {
                            Console.WriteLine($"[VOICE DEBUG] Existing voice client StopAsync failed for guild {guildId}: {stopEx}");
                        }

                        _voiceConnections.Remove(guildId);
                    }

                    if (_client?.CurrentUser != null)
                    {
                        SocketGuildUser? botUser = voiceChannelToJoin.Guild.GetUser(_client.CurrentUser.Id);
                        if (botUser != null)
                        {
                            ChannelPermissions vcPerms = botUser.GetPermissions(voiceChannelToJoin);
                            Console.WriteLine(
                                $"[VOICE DEBUG] Target VC='{voiceChannelToJoin.Name}' ({voiceChannelToJoin.Id}) Guild='{voiceChannelToJoin.Guild.Name}' ({guildId}) " +
                                $"Perms: ViewChannel={vcPerms.ViewChannel}, Connect={vcPerms.Connect}, Speak={vcPerms.Speak}, UseVoiceActivation={vcPerms.UseVAD}");
                        }
                        else
                        {
                            Console.WriteLine("[VOICE DEBUG] Could not resolve bot guild user for voice permission check.");
                        }
                    }

                    Console.WriteLine("[VOICE DEBUG] Calling ConnectAsync(selfDeaf: false, selfMute: false) from background task...");

                    // Join and stream silent PCM frames so Discord keeps the voice session alive.
                    // selfMute must stay false or Discord may not accept outgoing audio packets.
                    // selfDeaf is false for debugging. Once stable, you can try true again.
                    IAudioClient audioClient = await voiceChannelToJoin.ConnectAsync(selfDeaf: false, selfMute: false);
                    _voiceConnections[guildId] = audioClient;

                    Console.WriteLine("[VOICE DEBUG] ConnectAsync completed. Starting silent PCM keep-alive task...");

                    var keepAliveToken = new CancellationTokenSource();
                    _voiceKeepAliveTokens[guildId] = keepAliveToken;

                    _ = Task.Run(() => KeepSilentVoiceAliveAsync(guildId, audioClient, keepAliveToken.Token))
                        .ContinueWith(task =>
                        {
                            if (task.Exception != null)
                                Console.WriteLine($"[VOICE DEBUG] Keep-alive task faulted: {task.Exception.Flatten()}");
                            else
                                Console.WriteLine($"[VOICE DEBUG] Keep-alive task ended for guild {guildId}.");
                        });

                    Console.WriteLine(
                        $"[VOICE] ApolloBot joined VC '{voiceChannelToJoin.Name}' in guild '{voiceChannelToJoin.Guild.Name}'.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[VOICE] Background join failed: {ex}");
                }
            });

            return;
        }

        if (sub == "leave")
        {
            // !bot leave <ServerID> removes ApolloBot from that guild and blocklists it.
            // !bot leave with no ID keeps the existing voice-channel leave behaviour.
            if (parts.Length >= 3)
            {
                if (!ulong.TryParse(parts[2], out ulong guildIdToLeave))
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        "Usage: `!bot leave <ServerID>` (or `!bot leave` to leave voice)");
                    return;
                }

                SocketGuild? guildToLeave = _client?.GetGuild(guildIdToLeave);
                if (guildToLeave == null)
                {
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"ApolloBot is not currently connected to server `{guildIdToLeave}`.");
                    return;
                }

                AddBlockedGuild(guildIdToLeave);

                try
                {
                    string guildName = guildToLeave.Name;
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"Leaving **{guildName}** (`{guildIdToLeave}`) and adding it to ApolloBot's blocklist.");
                    await guildToLeave.LeaveAsync();
                    Console.WriteLine($"[OWNER LEAVE] Left and blocklisted '{guildName}' ({guildIdToLeave}).");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[OWNER LEAVE] Failed to leave guild {guildIdToLeave}: {ex}");
                    await SendBotOwnerMessageAsync(textChannel, message.Author.Id,
                        $"I blocklisted `{guildIdToLeave}`, but Discord returned an error while I tried to leave it.");
                }

                return;
            }

            try
            {
                await message.DeleteAsync();

                if (_voiceKeepAliveTokens.TryGetValue(textChannel.Guild.Id, out CancellationTokenSource? keepAliveToken))
                {
                    keepAliveToken.Cancel();
                    keepAliveToken.Dispose();
                    _voiceKeepAliveTokens.Remove(textChannel.Guild.Id);
                }

                if (_voiceConnections.TryGetValue(textChannel.Guild.Id, out IAudioClient? audioClient))
                {
                    await audioClient.StopAsync();
                    _voiceConnections.Remove(textChannel.Guild.Id);

                    Console.WriteLine($"[VOICE] ApolloBot left VC in guild '{textChannel.Guild.Name}'.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[VOICE] Failed to leave VC: {ex}");
            }

            return;
        }

        if (sub == "setembeds")
        {
            if (parts.Length < 3 || !long.TryParse(parts[2], out long newCount) || newCount < 0)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Usage: `!bot setembeds <number>`");
                return;
            }

            lock (_statsLock)
            {
                _embedsFixedCount = newCount;
            }

            SaveBotStatsHeartbeat();

            await SendBotOwnerMessageAsync(textChannel, message.Author.Id, $"✅ Embeds fixed count set to **{newCount}**.");
            return;
        }

        if (sub == "setservicetime")
        {
            if (parts.Length < 3)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, 
                    "Usage: `!bot setservicetime <duration>`\n" +
                    "Examples: `!bot setservicetime 11d`, `!bot setservicetime 11d12h`, `!bot setservicetime 3h30m`, `!bot setservicetime 90m`");
                return;
            }

            string durationText = string.Concat(parts.Skip(2));

            if (!TryParseDurationInput(durationText, out long totalSeconds) || totalSeconds < 0)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, 
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

            await SendBotOwnerMessageAsync(textChannel, message.Author.Id, $"✅ Service time set to **{FormatDuration(TimeSpan.FromSeconds(totalSeconds))}**.");
            return;
        }

        if (sub == "servers")
        {
            if (_client == null)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "Client not ready.");
                return;
            }

            var guilds = GetVisibleGuilds()
                .OrderByDescending(g => g.MemberCount)
                .ThenBy(g => g.Name)
                .Select((g, i) => $"**{i + 1}.** {g.Name}\nID: `{g.Id}` | Members: **{g.MemberCount}**")
                .ToList();

            if (guilds.Count == 0)
            {
                await SendBotOwnerMessageAsync(textChannel, message.Author.Id, "I'm not in any servers.");
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
                headerText: $"**Public Servers:** {GetVisibleServerCount()}\n**Public Users:** {GetVisibleUserCount()}\n**Excluded Servers:** {_statsExcludedGuildIds.Count}",
                ownerUserId: message.Author.Id);

            return;
        }

        if (sub == "topservers")
        {
            if (parts.Length >= 4 && parts[2].Equals("remove", StringComparison.OrdinalIgnoreCase))
            {
                await RemoveServerFromTopServersAsync(textChannel, message.Author.Id, parts);
                return;
            }

            await SendTopServersByUsageAsync(textChannel, message.Author.Id);
            return;
        }

        if (sub == "serverstats")
        {
            await SendSingleServerStatsAsync(textChannel, message.Author.Id, parts);
            return;
        }

        if (sub == "stats")
        {
            int serverCount = GetVisibleServerCount();
            int actualServerCount = _client?.Guilds.Count ?? 0;
            int visibleUserCount = GetVisibleUserCount();
            int relayCount = _relayStates.Count;
            int guildSettingsCount = _guildSettings.Count;
            int ignoredUsersCount = _userIgnoreSettings.Count;
            long currentSessionSeconds = GetCurrentSessionSeconds();
            long totalUptimeSeconds = GetTotalUptimeSeconds();
            TimeSpan uptime = TimeSpan.FromSeconds(totalUptimeSeconds);

            var embed = new EmbedBuilder()
                .WithTitle("Bot Stats")
                .AddField("Public Servers", serverCount, true)
                .AddField("Public Users", visibleUserCount, true)
                .AddField("Actual Servers", actualServerCount, true)
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

            await SendBotOwnerMessageAsync(textChannel, message.Author.Id, embed: embed);
            return;
        }

        await SendBotHelp(textChannel, message.Author.Id);
    }

    private IEnumerable<SocketGuild> GetVisibleGuilds()
    {
        if (_client == null)
            return Enumerable.Empty<SocketGuild>();

        return _client.Guilds.Where(g => !_statsExcludedGuildIds.Contains(g.Id));
    }

    private int GetVisibleServerCount()
    {
        return GetVisibleGuilds().Count();
    }

    private int GetVisibleUserCount()
    {
        return GetVisibleGuilds().Sum(g => g.MemberCount);
    }

    private async Task HandleStatsExclusionCommandAsync(SocketTextChannel textChannel, ulong ownerUserId, string[] parts, bool exclude)
    {
        string action = exclude ? "exclude" : "include";

        if (parts.Length < 3 || !ulong.TryParse(parts[2], out ulong guildId))
        {
            await SendBotOwnerMessageAsync(textChannel, ownerUserId, $"Usage: `!bot {action} <serverId>`");
            return;
        }

        if (exclude)
        {
            bool added = _statsExcludedGuildIds.Add(guildId);
            SaveStatsExcludedGuilds();

            await SendBotOwnerMessageAsync(textChannel, ownerUserId,
                added
                    ? $"✅ Excluded server `{guildId}` from public stats."
                    : $"ℹ️ Server `{guildId}` is already excluded from public stats.");
        }
        else
        {
            bool removed = _statsExcludedGuildIds.Remove(guildId);
            SaveStatsExcludedGuilds();

            await SendBotOwnerMessageAsync(textChannel, ownerUserId,
                removed
                    ? $"✅ Re-included server `{guildId}` in public stats."
                    : $"ℹ️ Server `{guildId}` was not excluded from public stats.");
        }
    }

    private async Task SendStatsExclusionsAsync(SocketTextChannel textChannel, ulong ownerUserId)
    {
        if (_statsExcludedGuildIds.Count == 0)
        {
            await SendBotOwnerMessageAsync(textChannel, ownerUserId, "No servers are currently excluded from public stats.");
            return;
        }

        var lines = _statsExcludedGuildIds
            .OrderBy(id => id)
            .Select(id =>
            {
                string name = _client?.GetGuild(id)?.Name ?? "Unknown/not currently connected";
                return $"• **{name}** — `{id}`";
            })
            .ToList();

        await SendBotOwnerMessageAsync(textChannel, ownerUserId,
            $"**Servers Excluded From Public Stats**\n{string.Join("\n", lines)}");
    }

    private async Task KeepSilentVoiceAliveAsync(ulong guildId, IAudioClient audioClient, CancellationToken cancellationToken)
    {
        try
        {
            Console.WriteLine($"[VOICE DEBUG] Keep-alive starting for guild {guildId}.");

            using AudioOutStream stream = audioClient.CreatePCMStream(AudioApplication.Mixed);

            Console.WriteLine($"[VOICE DEBUG] PCM stream created for guild {guildId}. Sending speaking=true...");
            await audioClient.SetSpeakingAsync(true);

            // 20ms of 48kHz stereo 16-bit PCM silence:
            // 48000 samples/sec * 2 channels * 2 bytes/sample * 0.02 sec = 3840 bytes.
            byte[] silenceFrame = new byte[3840];
            long framesSent = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                await stream.WriteAsync(silenceFrame, 0, silenceFrame.Length, cancellationToken);
                framesSent++;

                if (framesSent == 1 || framesSent % 250 == 0)
                    Console.WriteLine($"[VOICE DEBUG] Silent frames sent for guild {guildId}: {framesSent}");

                await Task.Delay(20, cancellationToken);
            }

            Console.WriteLine($"[VOICE DEBUG] Keep-alive cancellation requested for guild {guildId}. Flushing stream...");
            await stream.FlushAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"[VOICE DEBUG] Keep-alive cancelled for guild {guildId}.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[VOICE DEBUG] Silent keep-alive crashed for guild {guildId}: {ex}");
        }
        finally
        {
            try
            {
                await audioClient.SetSpeakingAsync(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[VOICE DEBUG] Failed to set speaking=false for guild {guildId}: {ex}");
            }
        }
    }

    private async Task SendBotHelp(SocketTextChannel channel, ulong ownerUserId)
    {
        await SendPaginatedEmbedAsync(
            channel,
            "👑 Bot Owner Commands",
            BuildBotOwnerHelpLines(),
            "bothelp",
            page: 0,
            pageSize: DefaultPageSize,
            color: Color.DarkPurple,
            headerText: "Owner-only controls and maintenance commands.",
            ownerUserId: ownerUserId);
    }

    private (string DisplayName, string AvatarUrl) GetRelayIdentity(IUser user, SocketGuild? guild = null)
    {
        SocketGuildUser? guildUser = user as SocketGuildUser;

        if (guildUser == null && guild != null)
            guildUser = guild.GetUser(user.Id);

        string displayName;
        string avatarUrl;

        if (guildUser != null)
        {
            displayName = guildUser.Nickname
                ?? guildUser.DisplayName
                ?? guildUser.GlobalName
                ?? guildUser.Username;

            avatarUrl = guildUser.GetGuildAvatarUrl(ImageFormat.Auto, 256)
                ?? guildUser.GetAvatarUrl(ImageFormat.Auto, 256)
                ?? guildUser.GetDefaultAvatarUrl();
        }
        else
        {
            displayName = user.GlobalName ?? user.Username;
            avatarUrl = user.GetAvatarUrl(ImageFormat.Auto, 256)
                ?? user.GetDefaultAvatarUrl();
        }

        displayName = Regex.Replace(displayName ?? user.Username, @"\s+", " ").Trim();

        if (displayName.Length > 32)
            displayName = displayName.Substring(0, 32);

        if (string.IsNullOrWhiteSpace(displayName))
            displayName = user.Username;

        return (displayName, avatarUrl);
    }

    private static readonly string[] OptionalUserCommands =
    {
        "fox", "cat", "dog", "roll", "userstats", "serverstats", "vote", "updates"
    };

    private static string FormatOptionalCommandName(string command) => command switch
    {
        "fox" => "🦊 Fox",
        "cat" => "🐱 Cat",
        "dog" => "🐶 Dog",
        "roll" => "🎲 Roll",
        "userstats" => "📊 User Stats",
        "serverstats" => "🏆 Server Stats",
        "vote" => "⭐ Vote",
        "updates" => "📋 Updates",
        _ => command
    };

    private bool IsOptionalCommandDisabled(ulong guildId, string command)
    {
        GuildSettings settings = GetOrCreateGuildSettings(guildId);
        settings.DisabledUserCommands ??= new List<string>();
        return settings.DisabledUserCommands.Contains(command, StringComparer.OrdinalIgnoreCase);
    }

    private async Task<bool> StopIfOptionalCommandDisabledAsync(SocketTextChannel channel, string command)
    {
        if (!IsOptionalCommandDisabled(channel.Guild.Id, command))
            return false;

        await channel.SendMessageAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "errors.command_disabled"));
        return true;
    }

    private async Task<bool> StopIfOptionalSlashCommandDisabledAsync(SocketSlashCommand command, string commandName)
    {
        if (command.Channel is not SocketTextChannel channel || !IsOptionalCommandDisabled(channel.Guild.Id, commandName))
            return false;

        await command.RespondAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "errors.command_disabled"), ephemeral: true);
        return true;
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
            if (await StopIfOptionalCommandDisabledAsync(textChannel, "updates")) return;
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

        if (sub is "fox" or "cat" or "dog")
        {
            if (await StopIfOptionalCommandDisabledAsync(textChannel, sub)) return;
            await SendRandomAnimalAsync(message.Author.Id, textChannel, sub);
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

        if (sub == "info")
        {
            await HandleInfoCommand(textChannel, parts);
            return;
        }

        if (sub == "usersettings" || sub == "ignore")
        {
            await SendUserSettingsAsync(textChannel, message.Author.Id, textChannel.Guild.Id);
            return;
        }

        if (sub == "update" && IsBotOwner(message.Author))
        {
            await HandlePlannedUpdateOwnerCommand(textChannel, message.Author.Id, new[] { "bot", "update" }.Concat(parts.Skip(1)).ToArray());
            return;
        }

        if (sub == "userstats")
        {
            if (await StopIfOptionalCommandDisabledAsync(textChannel, "userstats")) return;
            await SendUserStatsAsync(message, textChannel, parts);
            return;
        }

        if (sub == "serverstats")
        {
            if (await StopIfOptionalCommandDisabledAsync(textChannel, "serverstats")) return;
            await SendPublicServerStatsAsync(message, textChannel);
            return;
        }

        string[] adminCommands = { "on", "off", "silent", "togglebuttons", "cooldown", "reset", "whitelist" };
        if (!adminCommands.Contains(sub, StringComparer.OrdinalIgnoreCase))
        {
            await textChannel.SendMessageAsync(L(settings, "errors.unrecognized_prefix"));
            return;
        }

        if (message.Author is not SocketGuildUser guildUser || !guildUser.GuildPermissions.ManageGuild)
        {
            await textChannel.SendMessageAsync(L(settings, "errors.manage_server"));
            return;
        }

        if (raw.Equals("!embedfix on", StringComparison.OrdinalIgnoreCase) || sub == "on")
        {
            settings.Enabled = true;
            SaveGuildSettings();
            await textChannel.SendMessageAsync(L(settings, "admin.embed_enabled"));
            return;
        }

        if (raw.Equals("!embedfix off", StringComparison.OrdinalIgnoreCase) || sub == "off")
        {
            settings.Enabled = false;
            SaveGuildSettings();
            await textChannel.SendMessageAsync(L(settings, "admin.embed_disabled"));
            return;
        }

        if (sub == "silent")
        {
            if (parts.Length < 2)
            {
                await textChannel.SendMessageAsync(L(settings, "admin.silent_usage"));
                return;
            }

            string mode = parts[1].ToLowerInvariant();

            if (mode == "on")
            {
                settings.SilentMode = true;
                SaveGuildSettings();
                await textChannel.SendMessageAsync(L(settings, "admin.silent_enabled"));
                return;
            }

            if (mode == "off")
            {
                settings.SilentMode = false;
                SaveGuildSettings();
                await textChannel.SendMessageAsync(L(settings, "admin.silent_disabled"));
                return;
            }

            await textChannel.SendMessageAsync(L(settings, "admin.silent_usage"));
            return;
        }

        if (sub == "togglebuttons")
        {
            await HandleToggleButtonsCommand(textChannel, settings, parts);
            return;
        }

        if (sub == "cooldown")
        {
            await HandleCooldownCommand(textChannel, settings, parts);
            return;
        }

        if (sub == "reset")
        {
            await HandleResetCommand(textChannel, settings, parts);
            return;
        }

        if (sub == "whitelist")
        {
            await HandleWhitelistCommand(message, textChannel, settings, parts);
            return;
        }

        await textChannel.SendMessageAsync(L(settings, "errors.unrecognized_prefix"));
    }

    private async Task SendUserSettingsAsync(SocketTextChannel channel, ulong userId, ulong guildId)
    {
        UserIgnoreSettings settings = GetOrCreateUserIgnoreSettings(userId);
        Embed embed = BuildUserSettingsEmbed(settings, guildId);
        MessageComponent components = BuildUserSettingsComponents(settings, userId, guildId, "twitter");
        await channel.SendMessageAsync(embed: embed, components: components);
    }

    private Embed BuildUserSettingsEmbed(UserIgnoreSettings settings, ulong guildId)
    {
        GuildSettings? guildSettings = guildId != 0 ? GetOrCreateGuildSettings(guildId) : null;
        string T(string key, params object?[] args) =>
            guildSettings != null ? T(key, args) : LU(settings, key, args);

        bool ignoredHere = guildId != 0 && settings.IgnoredGuildIds.Contains(guildId);
        string fixing = settings.IgnoreAllServers
            ? T("usersettings.disabled_everywhere")
            : ignoredHere
                ? T("usersettings.disabled_server")
                : T("common.enabled");

        string providerSummary = string.Join("\n", _providers.Keys.OrderBy(FormatPlatformName).Select(platform =>
        {
            string value = settings.PreferredProviders.TryGetValue(platform, out string? preferred)
                ? preferred
                : T("usersettings.automatic");
            return $"**{FormatPlatformName(platform)}:** {value}";
        }));

        return new EmbedBuilder()
            .WithTitle(T("usersettings.title"))
            .WithDescription(T("usersettings.description"))
            .AddField(T("usersettings.embed_fixing"), fixing, false)
            .AddField(T("usersettings.preferred_providers"), providerSummary, false)
            .AddField(T("usersettings.reply_notifications"),
                T(settings.ReplyNotificationsEnabled ? "common.enabled" : "common.disabled"), true)
            .WithFooter(T("usersettings.footer"))
            .WithColor(Color.Teal)
            .Build();
    }

    private MessageComponent BuildUserSettingsComponents(UserIgnoreSettings settings, ulong userId, ulong guildId, string selectedPlatform)
    {
        GuildSettings? guildSettings = guildId != 0 ? GetOrCreateGuildSettings(guildId) : null;
        string T(string key, params object?[] args) =>
            guildSettings != null ? T(key, args) : LU(settings, key, args);

        if (!_providers.ContainsKey(selectedPlatform))
            selectedPlatform = _providers.Keys.FirstOrDefault() ?? "twitter";

        var platformMenu = new SelectMenuBuilder()
            .WithCustomId($"usersettings_platform:{userId}:{guildId}")
            .WithPlaceholder(T("usersettings.choose_platform"))
            .WithMinValues(1)
            .WithMaxValues(1);

        foreach (string platform in _providers.Keys.OrderBy(FormatPlatformName).Take(25))
            platformMenu.AddOption(FormatPlatformName(platform), platform, isDefault: platform.Equals(selectedPlatform, StringComparison.OrdinalIgnoreCase));

        var providerMenu = new SelectMenuBuilder()
            .WithCustomId($"usersettings_provider:{userId}:{guildId}:{selectedPlatform}")
            .WithPlaceholder(T("usersettings.preferred_provider_placeholder", FormatPlatformName(selectedPlatform)))
            .WithMinValues(1)
            .WithMaxValues(1)
            .AddOption(T("usersettings.automatic"), "__auto__",
                T("usersettings.automatic_description"),
                isDefault: !settings.PreferredProviders.ContainsKey(selectedPlatform));

        if (_providers.TryGetValue(selectedPlatform, out List<string>? providers))
        {
            foreach (string provider in providers.Take(24))
            {
                bool selected = settings.PreferredProviders.TryGetValue(selectedPlatform, out string? preferred) &&
                                string.Equals(preferred, provider, StringComparison.OrdinalIgnoreCase);
                providerMenu.AddOption(provider, provider, isDefault: selected);
            }
        }

        var fixingMenu = new SelectMenuBuilder()
            .WithCustomId($"usersettings_fixing:{userId}:{guildId}")
            .WithPlaceholder(T("usersettings.fixing_placeholder"))
            .WithMinValues(1)
            .WithMaxValues(1)
            .AddOption(T("common.enabled"), "enabled",
                T("usersettings.fix_normally"),
                isDefault: !settings.IgnoreAllServers && !settings.IgnoredGuildIds.Contains(guildId))
            .AddOption(T("usersettings.disabled_server"), "server",
                T("usersettings.disable_server_description"),
                isDefault: !settings.IgnoreAllServers && settings.IgnoredGuildIds.Contains(guildId))
            .AddOption(T("usersettings.disabled_everywhere"), "global",
                T("usersettings.disable_global_description"),
                isDefault: settings.IgnoreAllServers);

        var replyMenu = new SelectMenuBuilder()
            .WithCustomId($"usersettings_replies:{userId}:{guildId}")
            .WithPlaceholder(T("usersettings.reply_notifications"))
            .WithMinValues(1)
            .WithMaxValues(1)
            .AddOption(T("usersettings.replies_on"), "on",
                T("usersettings.replies_on_description"),
                isDefault: settings.ReplyNotificationsEnabled)
            .AddOption(T("usersettings.replies_off"), "off",
                T("usersettings.replies_off_description"),
                isDefault: !settings.ReplyNotificationsEnabled);

        var languageMenu = new SelectMenuBuilder()
            .WithCustomId($"usersettings_language:{userId}:{guildId}")
            .WithPlaceholder(T("usersettings.personal_language"))
            .WithMinValues(1)
            .WithMaxValues(1);

        string personalLanguage = _localization.NormalizeLanguage(settings.LanguageCode);
        foreach (string language in _localization.AvailableLanguages.OrderBy(x => x))
        {
            languageMenu.AddOption(
                GetLanguageDisplayName(language),
                language,
                isDefault: language.Equals(personalLanguage, StringComparison.OrdinalIgnoreCase));
        }

        return new ComponentBuilder()
            .WithSelectMenu(platformMenu)
            .WithSelectMenu(providerMenu)
            .WithSelectMenu(fixingMenu)
            .WithSelectMenu(replyMenu)
            .WithSelectMenu(languageMenu)
            .WithButton(T("usersettings.reset"), $"usersettings_reset:{userId}:{guildId}", ButtonStyle.Secondary)
            .Build();
    }

    private async Task SelectMenuExecuted(SocketMessageComponent component)
    {
        string customId = component.Data.CustomId;

        if (customId.StartsWith("serversetup_setlanguage:", StringComparison.Ordinal))
        {
            if (component.Channel is not SocketTextChannel setupChannel ||
                component.User is not SocketGuildUser setupUser ||
                !setupUser.GuildPermissions.ManageGuild)
            {
                await component.RespondAsync(LC(component, "errors.manage_server_change"), ephemeral: true);
                return;
            }

            string[] setupParts = customId.Split(':');
            if (setupParts.Length != 2 ||
                !ulong.TryParse(setupParts[1], out ulong setupGuildId) ||
                setupGuildId != setupChannel.Guild.Id)
            {
                await component.RespondAsync(LC(component, "errors.setup_expired"), ephemeral: true);
                return;
            }

            string? selectedLanguage = component.Data.Values.FirstOrDefault();
            if (string.IsNullOrWhiteSpace(selectedLanguage) || !_localization.IsSupported(selectedLanguage))
            {
                await component.RespondAsync(LC(component, "errors.language_unavailable"), ephemeral: true);
                return;
            }

            GuildSettings guildSettings = GetOrCreateGuildSettings(setupGuildId);
            guildSettings.LanguageCode = _localization.NormalizeLanguage(selectedLanguage);
            SaveGuildSettings();

            await component.UpdateAsync(msg =>
            {
                msg.Embed = Optional.Create(BuildLanguageSettingsEmbed(guildSettings));
                msg.Components = Optional.Create(BuildLanguageSettingsComponents(guildSettings, setupGuildId));
            });
            return;
        }

        if (customId.StartsWith("serversetup_disabledcommands:", StringComparison.Ordinal))
        {
            if (component.Channel is not SocketTextChannel setupChannel || component.User is not SocketGuildUser setupUser || !setupUser.GuildPermissions.ManageGuild)
            {
                await component.RespondAsync(LC(component, "errors.manage_server_change"), ephemeral: true);
                return;
            }
            string[] setupParts = customId.Split(':');
            if (setupParts.Length != 2 || !ulong.TryParse(setupParts[1], out ulong setupGuildId) || setupGuildId != setupChannel.Guild.Id)
            {
                await component.RespondAsync(LC(component, "errors.setup_expired"), ephemeral: true);
                return;
            }
            GuildSettings guildSettings = GetOrCreateGuildSettings(setupGuildId);
            guildSettings.DisabledUserCommands = component.Data.Values
                .Where(v => OptionalUserCommands.Contains(v, StringComparer.OrdinalIgnoreCase))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            SaveGuildSettings();
            await component.UpdateAsync(msg =>
            {
                msg.Embed = Optional.Create(BuildCommandSettingsEmbed(guildSettings));
                msg.Components = Optional.Create(BuildCommandSettingsComponents(guildSettings, setupGuildId));
            });
            return;
        }

        if (!customId.StartsWith("usersettings_", StringComparison.Ordinal))
            return;

        string[] parts = customId.Split(':');
        if (parts.Length < 3 || !ulong.TryParse(parts[1], out ulong ownerUserId) || !ulong.TryParse(parts[2], out ulong guildId))
        {
            await component.RespondAsync(LC(component, "errors.settings_expired"), ephemeral: true);
            return;
        }

        if (component.User.Id != ownerUserId)
        {
            await component.RespondAsync(LC(component, "errors.settings_owner"), ephemeral: true);
            return;
        }

        UserIgnoreSettings settings = GetOrCreateUserIgnoreSettings(ownerUserId);
        string value = component.Data.Values.FirstOrDefault() ?? "";
        string selectedPlatform = "twitter";

        if (customId.StartsWith("usersettings_platform:", StringComparison.Ordinal))
        {
            selectedPlatform = value;
        }
        else if (customId.StartsWith("usersettings_provider:", StringComparison.Ordinal) && parts.Length >= 4)
        {
            selectedPlatform = parts[3];
            if (value == "__auto__")
                settings.PreferredProviders.Remove(selectedPlatform);
            else if (_providers.TryGetValue(selectedPlatform, out List<string>? providers) && providers.Contains(value, StringComparer.OrdinalIgnoreCase))
                settings.PreferredProviders[selectedPlatform] = value;
            SaveUserIgnoreSettings();
        }
        else if (customId.StartsWith("usersettings_language:", StringComparison.Ordinal))
        {
            if (!_localization.IsSupported(value))
            {
                await component.RespondAsync(LC(component, "errors.language_unavailable"), ephemeral: true);
                return;
            }

            settings.LanguageCode = _localization.NormalizeLanguage(value);
            SaveUserIgnoreSettings();
        }
        else if (customId.StartsWith("usersettings_fixing:", StringComparison.Ordinal))
        {
            if (value == "global")
            {
                settings.IgnoreAllServers = true;
            }
            else
            {
                settings.IgnoreAllServers = false;
                if (guildId != 0)
                {
                    if (value == "server")
                    {
                        if (!settings.IgnoredGuildIds.Contains(guildId)) settings.IgnoredGuildIds.Add(guildId);
                    }
                    else
                    {
                        settings.IgnoredGuildIds.Remove(guildId);
                    }
                }
            }
            SaveUserIgnoreSettings();
        }
        else if (customId.StartsWith("usersettings_replies:", StringComparison.Ordinal))
        {
            settings.ReplyNotificationsEnabled = value != "off";
            SaveUserIgnoreSettings();
        }

        await component.UpdateAsync(msg =>
        {
            msg.Embed = Optional.Create(BuildUserSettingsEmbed(settings, guildId));
            msg.Components = Optional.Create(BuildUserSettingsComponents(settings, ownerUserId, guildId, selectedPlatform));
        });
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
            await textChannel.SendMessageAsync(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "ignore.server_on"));
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
                    await textChannel.SendMessageAsync(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "ignore.all_on"));
                    return;
                }

                if (mode == "off")
                {
                    ignoreSettings.IgnoreAllServers = false;
                    SaveUserIgnoreSettings();
                    await textChannel.SendMessageAsync(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "ignore.all_off"));
                    return;
                }

                await textChannel.SendMessageAsync(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "ignore.usage"));
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

    private async Task HandleToggleButtonsCommand(SocketTextChannel textChannel, GuildSettings settings, string[] parts)
    {
        if (parts.Length < 2)
        {
            await textChannel.SendMessageAsync(L(settings, "admin.buttons_current", L(settings, settings.ButtonsEnabled ? "common.enabled" : "common.disabled")));
            return;
        }
        string mode = parts[1].ToLowerInvariant();
        if (mode == "on") { settings.ButtonsEnabled = true; SaveGuildSettings(); await textChannel.SendMessageAsync(L(settings, "admin.buttons_enabled")); return; }
        if (mode == "off") { settings.ButtonsEnabled = false; SaveGuildSettings(); await textChannel.SendMessageAsync(L(settings, "admin.buttons_disabled")); return; }
        await textChannel.SendMessageAsync(L(settings, "admin.buttons_usage"));
    }

    private async Task HandleCooldownCommand(SocketTextChannel textChannel, GuildSettings settings, string[] parts)
    {
        if (parts.Length < 2) { await textChannel.SendMessageAsync(L(settings, "admin.cooldown_current", settings.ButtonCooldownSeconds)); return; }
        if (!int.TryParse(parts[1], out int seconds) || seconds < 1 || seconds > 30) { await textChannel.SendMessageAsync(L(settings, "admin.cooldown_invalid")); return; }
        settings.ButtonCooldownSeconds = seconds; SaveGuildSettings();
        await textChannel.SendMessageAsync(L(settings, "admin.cooldown_set", seconds));
    }

    private async Task HandleResetCommand(SocketTextChannel textChannel, GuildSettings settings, string[] parts)
    {
        if (parts.Length < 2 || !parts[1].Equals("confirm", StringComparison.OrdinalIgnoreCase)) { await textChannel.SendMessageAsync(L(settings, "admin.reset_confirm")); return; }
        settings.Enabled = true; settings.SilentMode = false; settings.ButtonsEnabled = true; settings.ButtonCooldownSeconds = 3;
        settings.WhitelistedChannelIds.Clear(); settings.DisabledUserCommands ??= new List<string>(); settings.DisabledUserCommands.Clear(); SaveGuildSettings();
        await textChannel.SendMessageAsync(L(settings, "admin.reset_done"));
    }

    private async Task HandleInfoCommand(SocketTextChannel textChannel, string[] parts)
    {
        GuildSettings settings = GetOrCreateGuildSettings(textChannel.Guild.Id);
        if (parts.Length < 2) { await textChannel.SendMessageAsync(L(settings, "info.usage")); return; }
        if (!TryExtractMessageIdFromDiscordLink(parts[1], out ulong messageId)) { await textChannel.SendMessageAsync(L(settings, "info.invalid_link")); return; }
        if (!_relayStates.TryGetValue(messageId, out RelayMessageState? state)) { await textChannel.SendMessageAsync(L(settings, "info.not_found")); return; }
        string platformText = state.Platforms.Count == 0 ? L(settings, "common.unknown") : string.Join(", ", state.Platforms.Select(FormatPlatformName));
        string providerText = FormatRelayProviders(state); string userText = $"<@{state.OriginalAuthorId}>";
        var embed = new EmbedBuilder().WithTitle(L(settings, "info.title")).WithColor(Color.Teal)
            .AddField(L(settings, "info.original_user"), userText, true).AddField(L(settings, "info.platforms"), platformText, true)
            .AddField(L(settings, "info.providers"), providerText, false).Build();
        await textChannel.SendMessageAsync(embed: embed);
    }

    private async Task SendUserStatsAsync(SocketUserMessage message, SocketTextChannel channel, string[] parts)
    {
        ulong targetUserId = message.Author.Id;

        if (parts.Length >= 2)
        {
            string rawTarget = parts[1].Trim();
            Match mentionMatch = Regex.Match(rawTarget, @"^<@!?(\d+)>$");

            if (mentionMatch.Success)
                ulong.TryParse(mentionMatch.Groups[1].Value, out targetUserId);
            else if (!ulong.TryParse(rawTarget, out targetUserId))
            {
                await channel.SendMessageAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "stats.userstats_usage"));
                return;
            }
        }

        IUser? targetUser = _client?.GetUser(targetUserId);
        string displayName = targetUser?.GlobalName ?? targetUser?.Username ?? $"User {targetUserId}";
        string avatarUrl = targetUser?.GetAvatarUrl(ImageFormat.Auto, 256) ?? targetUser?.GetDefaultAvatarUrl() ?? "";

        GuildSettings settings = GetOrCreateGuildSettings(channel.Guild.Id);
        Embed embed = BuildUserStatsEmbed(targetUserId, displayName, avatarUrl, settings);
        MessageComponent components = BuildStatsProfileComponents("user", targetUserId, message.Author.Id, settings);

        await channel.SendMessageAsync(embed: embed, components: components);
    }

    private async Task SendPublicServerStatsAsync(SocketUserMessage message, SocketTextChannel channel)
    {
        GuildSettings settings = GetOrCreateGuildSettings(channel.Guild.Id);
        Embed embed = BuildServerStatsEmbed(channel.Guild, settings);
        MessageComponent components = BuildStatsProfileComponents("server", channel.Guild.Id, message.Author.Id, settings);

        await channel.SendMessageAsync(embed: embed, components: components);
    }

    private Embed BuildUserStatsEmbed(ulong userId, string displayName, string avatarUrl, GuildSettings settings)
    {
        _userUsageStats.TryGetValue(userId, out UserUsageStats? stats);

        long totalFixes = stats?.EmbedFixCount ?? 0;
        int serverCount = stats?.GuildIds?.Count ?? 0;
        List<string> achievements = GetUserAchievements(userId, stats, settings);
        int regularUnlocked = GetUserRegularAchievementCount(userId, stats, settings);
        const int regularTotal = 10;

        string discordSince = SnowflakeUtils.FromSnowflake(userId).UtcDateTime.ToString("dd MMM yyyy");
        string firstUse = stats?.FirstUsedAtUtc != default
            ? stats!.FirstUsedAtUtc.ToString("dd MMM yyyy")
            : L(settings, "stats.not_recorded");

        string favouritePlatform = GetFavouritePlatform(stats?.PlatformUsage);
        string activity = FormatPlatformActivity(stats?.PlatformUsage, settings);

        var embed = new EmbedBuilder()
            .WithTitle(L(settings, "stats.user_title", displayName))
            .WithDescription(L(settings, "stats.user_id", userId))
            .AddField(L(settings, "stats.user_information"),
                L(settings, "stats.user_info_value", discordSince, firstUse, serverCount), false)
            .AddField(L(settings, "stats.embed_activity"),
                L(settings, "stats.total_fixes_activity", totalFixes, activity), false)
            .AddField(L(settings, "stats.activity"),
                L(settings, "stats.activity_value", favouritePlatform, FormatLastActivity(stats?.LastUsedAtUtc)), false)
            .AddField(L(settings, "stats.achievements"),
                L(settings, "stats.user_achievements_value", regularUnlocked, regularTotal, BuildAchievementPreview(achievements, settings)), false)
            .WithColor(userId == ApolloBotCreatorUserId ? Color.Gold : Color.Teal)
            .WithCurrentTimestamp();

        if (userId == ApolloBotCreatorUserId)
        {
            embed.AddField(L(settings, "stats.special_achievement"),
                L(settings, "stats.creator_special"), false);
        }

        if (!string.IsNullOrWhiteSpace(avatarUrl))
            embed.WithThumbnailUrl(avatarUrl);

        return embed.Build();
    }

    private Embed BuildServerStatsEmbed(SocketGuild guild, GuildSettings settings)
    {
        _guildUsageStats.TryGetValue(guild.Id, out GuildUsageStats? stats);
        _guildActivity.TryGetValue(guild.Id, out GuildActivityState? activityState);

        long totalFixes = stats?.EmbedFixCount ?? 0;
        List<string> achievements = GetServerAchievements(stats, settings);
        int unlocked = achievements.Count;
        const int totalAchievements = 11;

        string created = SnowflakeUtils.FromSnowflake(guild.Id).UtcDateTime.ToString("dd MMM yyyy");
        string joined = activityState?.LastJoinedAtUtc != default
            ? activityState!.LastJoinedAtUtc.ToString("dd MMM yyyy")
            : stats?.FirstSeenAtUtc != default
                ? stats!.FirstSeenAtUtc.ToString("dd MMM yyyy")
                : L(settings, "common.unknown");

        var embed = new EmbedBuilder()
            .WithTitle(L(settings, "stats.server_title", guild.Name))
            .WithDescription(L(settings, "stats.server_id", guild.Id))
            .AddField(L(settings, "stats.server_information"),
                L(settings, "stats.server_info_value", created, joined, guild.MemberCount), false)
            .AddField(L(settings, "stats.embed_activity"),
                L(settings, "stats.total_fixes_activity", totalFixes, FormatPlatformActivity(stats?.PlatformUsage, settings)), false)
            .AddField(L(settings, "stats.activity"),
                L(settings, "stats.activity_value", GetFavouritePlatform(stats?.PlatformUsage), FormatLastActivity(stats?.LastUsedAtUtc)), false)
            .AddField(L(settings, "stats.achievements"),
                L(settings, "stats.server_achievements_value", unlocked, totalAchievements, BuildAchievementPreview(achievements, settings)), false)
            .WithColor(Color.DarkTeal)
            .WithCurrentTimestamp();

        string iconUrl = guild.IconUrl;
        if (!string.IsNullOrWhiteSpace(iconUrl))
            embed.WithThumbnailUrl(iconUrl);

        return embed.Build();
    }

    private MessageComponent BuildStatsProfileComponents(string scope, ulong entityId, ulong ownerUserId, GuildSettings settings)
    {
        return new ComponentBuilder()
            .WithButton(L(settings, "stats.view_achievements"), $"stats_achievements:{scope}:{entityId}:{ownerUserId}", ButtonStyle.Secondary, new Emoji("🏅"))
            .WithButton(L(settings, "stats.close"), $"stats_close:{scope}:{entityId}:{ownerUserId}", ButtonStyle.Danger, new Emoji("✖️"))
            .Build();
    }

    private MessageComponent BuildAchievementComponents(string scope, ulong entityId, ulong ownerUserId, GuildSettings settings)
    {
        return new ComponentBuilder()
            .WithButton(L(settings, "stats.back_profile"), $"stats_profile:{scope}:{entityId}:{ownerUserId}", ButtonStyle.Secondary, new Emoji("◀️"))
            .WithButton(L(settings, "stats.close"), $"stats_close:{scope}:{entityId}:{ownerUserId}", ButtonStyle.Danger, new Emoji("✖️"))
            .Build();
    }

    private string FormatPlatformActivity(Dictionary<string, long>? platformUsage, GuildSettings settings)
    {
        if (platformUsage == null || platformUsage.Count == 0 || platformUsage.Values.Sum() <= 0)
            return L(settings, "stats.no_platform_activity");

        return string.Join("\n", platformUsage
            .Where(x => x.Value > 0)
            .OrderByDescending(x => x.Value)
            .Select(x => $"**{FormatPlatformName(x.Key)}:** {x.Value}"));
    }

    private string GetFavouritePlatform(Dictionary<string, long>? platformUsage)
    {
        if (platformUsage == null || platformUsage.Count == 0)
            return "—";

        KeyValuePair<string, long> favourite = platformUsage
            .Where(x => x.Value > 0)
            .OrderByDescending(x => x.Value)
            .FirstOrDefault();

        return favourite.Value > 0 ? FormatPlatformName(favourite.Key) : "—";
    }

    private string FormatLastActivity(DateTime? value)
    {
        if (!value.HasValue || value.Value == default)
            return "—";

        return value.Value.ToString("dd MMM yyyy HH:mm 'UTC'");
    }

    private string BuildAchievementPreview(List<string> achievements, GuildSettings settings)
    {
        if (achievements.Count == 0)
            return L(settings, "stats.no_regular_achievements");

        string creatorPreview = L(settings, "achievements.creator_preview");
        return string.Join("\n", achievements
            .Where(x => !x.Equals(creatorPreview, StringComparison.Ordinal))
            .Take(3));
    }

    private int GetUserRegularAchievementCount(ulong userId, UserUsageStats? stats, GuildSettings settings)
    {
        int count = GetUserAchievements(userId, stats, settings).Count;
        return userId == ApolloBotCreatorUserId ? Math.Max(0, count - 1) : count;
    }

    private List<string> GetUserAchievements(ulong userId, UserUsageStats? stats, GuildSettings settings)
    {
        var unlocked = new List<string>();
        long fixes = stats?.EmbedFixCount ?? 0;

        if (userId == ApolloBotCreatorUserId)
            unlocked.Add(L(settings, "achievements.creator_preview"));

        if (fixes >= 1) unlocked.Add(L(settings, "achievements.user_first"));
        if (fixes >= 10) unlocked.Add(L(settings, "achievements.user_hang"));
        if (fixes >= 25) unlocked.Add(L(settings, "achievements.user_regular"));
        if (fixes >= 50) unlocked.Add(L(settings, "achievements.user_doctor"));
        if (fixes >= 100) unlocked.Add(L(settings, "achievements.user_fixer"));
        if (fixes >= 250) unlocked.Add(L(settings, "achievements.user_enthusiast"));
        if (fixes >= 500) unlocked.Add(L(settings, "achievements.user_online"));
        if (fixes >= 1_000) unlocked.Add(L(settings, "achievements.user_grass"));

        HashSet<string> platforms = stats?.PlatformUsage?
            .Where(x => x.Value > 0)
            .Select(x => x.Key.ToLowerInvariant())
            .ToHashSet() ?? new HashSet<string>();

        if (platforms.Contains("twitter") && platforms.Contains("tiktok") && platforms.Contains("instagram"))
            unlocked.Add(L(settings, "achievements.user_hat"));

        if ((stats?.GuildIds?.Count ?? 0) >= 5)
            unlocked.Add(L(settings, "achievements.user_world"));

        return unlocked;
    }

    private List<string> GetServerAchievements(GuildUsageStats? stats, GuildSettings settings)
    {
        var unlocked = new List<string>();
        long fixes = stats?.EmbedFixCount ?? 0;

        unlocked.Add(L(settings, "achievements.server_welcome"));
        if (fixes >= 25) unlocked.Add(L(settings, "achievements.server_started"));
        if (fixes >= 100) unlocked.Add(L(settings, "achievements.server_fixers"));
        if (fixes >= 250) unlocked.Add(L(settings, "achievements.server_customers"));
        if (fixes >= 500) unlocked.Add(L(settings, "achievements.server_approved"));
        if (fixes >= 1_000) unlocked.Add(L(settings, "achievements.server_factory"));
        if (fixes >= 2_000) unlocked.Add(L(settings, "achievements.server_industrial"));
        if (fixes >= 2_500) unlocked.Add(L(settings, "achievements.server_seriously"));
        if (fixes >= 5_000) unlocked.Add(L(settings, "achievements.server_grass"));
        if (fixes >= 10_000) unlocked.Add(L(settings, "achievements.server_done"));

        Dictionary<string, long>? platforms = stats?.PlatformUsage;
        if (platforms != null && new[] { "twitter", "tiktok", "instagram" }.All(p => platforms.TryGetValue(p, out long count) && count >= 100))
            unlocked.Add(L(settings, "achievements.server_multimedia"));

        return unlocked;
    }

    private string Rarity(GuildSettings settings, string rarity) => L(settings, $"achievements.rarity_{rarity.ToLowerInvariant()}");

    private string FormatAchievementEntry(GuildSettings settings, string emoji, string nameKey, string rarity, string descriptionKey, long current, long target)
    {
        string name = L(settings, nameKey);
        string description = L(settings, descriptionKey, target);
        string rarityText = Rarity(settings, rarity);

        if (current >= target)
            return $"{emoji} **{name}** `{rarityText}`\n{description}\n{L(settings, "achievements.unlocked")}";

        double percent = target <= 0 ? 0 : Math.Clamp(current / (double)target, 0, 1);
        int filled = percent > 0 ? Math.Max(1, (int)Math.Floor(percent * 10)) : 0;
        string bar = new string('█', filled) + new string('░', 10 - filled);
        int percentDisplay = (int)Math.Floor(percent * 100);
        return $"🔒 **{name}** `{rarityText}`\n{description}\n`{bar}` **{percentDisplay}%**  ({current:N0} / {target:N0})";
    }

    private Embed BuildUserAchievementsEmbed(ulong userId, string displayName, GuildSettings settings)
    {
        _userUsageStats.TryGetValue(userId, out UserUsageStats? stats);
        long fixes = stats?.EmbedFixCount ?? 0;
        int servers = stats?.GuildIds?.Count ?? 0;
        var lines = new List<string>();

        if (userId == ApolloBotCreatorUserId)
            lines.Add($"👑 **{L(settings, "achievements.creator_name")}** `{Rarity(settings, "unique")}`\n*{L(settings, "achievements.creator_description")}*\n{L(settings, "achievements.special_unobtainable")}");

        lines.Add(FormatAchievementEntry(settings, "🔧", "achievements.user_first_name", "common", "achievements.user_first_desc", fixes, 1));
        lines.Add(FormatAchievementEntry(settings, "🛠️", "achievements.user_hang_name", "common", "achievements.user_hang_desc", fixes, 10));
        lines.Add(FormatAchievementEntry(settings, "🔗", "achievements.user_regular_name", "uncommon", "achievements.user_regular_desc", fixes, 25));
        lines.Add(FormatAchievementEntry(settings, "🩺", "achievements.user_doctor_name", "uncommon", "achievements.user_doctor_desc", fixes, 50));
        lines.Add(FormatAchievementEntry(settings, "🚀", "achievements.user_fixer_name", "rare", "achievements.user_fixer_desc", fixes, 100));
        lines.Add(FormatAchievementEntry(settings, "📡", "achievements.user_enthusiast_name", "epic", "achievements.user_enthusiast_desc", fixes, 250));
        lines.Add(FormatAchievementEntry(settings, "🌐", "achievements.user_online_name", "legendary", "achievements.user_online_desc", fixes, 500));
        lines.Add(FormatAchievementEntry(settings, "💀", "achievements.user_grass_name", "legendary", "achievements.user_grass_desc", fixes, 1000));

        bool hatTrick = new[] { "twitter", "tiktok", "instagram" }.All(p => stats?.PlatformUsage?.TryGetValue(p, out long count) == true && count > 0);
        string hat = $"🎩 **{L(settings, "achievements.user_hat_name")}** `{Rarity(settings, "rare")}`\n{L(settings, "achievements.user_hat_desc")}";
        lines.Add(hatTrick ? hat + "\n" + L(settings, "achievements.unlocked") : "🔒" + hat[1..]);
        lines.Add(FormatAchievementEntry(settings, "🌍", "achievements.user_world_name", "epic", "achievements.user_world_desc", servers, 5));

        return new EmbedBuilder().WithTitle(L(settings, "achievements.user_title", displayName)).WithDescription(string.Join("\n\n", lines))
            .WithColor(userId == ApolloBotCreatorUserId ? Color.Gold : Color.Teal)
            .WithFooter(L(settings, "achievements.user_footer", GetUserRegularAchievementCount(userId, stats, settings), 10)).Build();
    }

    private Embed BuildServerAchievementsEmbed(SocketGuild guild, GuildSettings settings)
    {
        _guildUsageStats.TryGetValue(guild.Id, out GuildUsageStats? stats);
        long fixes = stats?.EmbedFixCount ?? 0;
        var lines = new List<string>
        {
            $"👋 **{L(settings, "achievements.server_welcome_name")}** `{Rarity(settings, "common")}`\n{L(settings, "achievements.server_welcome_desc")}\n{L(settings, "achievements.unlocked")}",
            FormatAchievementEntry(settings, "🔧", "achievements.server_started_name", "common", "achievements.server_started_desc", fixes, 25),
            FormatAchievementEntry(settings, "🔗", "achievements.server_fixers_name", "common", "achievements.server_fixers_desc", fixes, 100),
            FormatAchievementEntry(settings, "🛠️", "achievements.server_customers_name", "uncommon", "achievements.server_customers_desc", fixes, 250),
            FormatAchievementEntry(settings, "⭐", "achievements.server_approved_name", "uncommon", "achievements.server_approved_desc", fixes, 500),
            FormatAchievementEntry(settings, "🏭", "achievements.server_factory_name", "rare", "achievements.server_factory_desc", fixes, 1000),
            FormatAchievementEntry(settings, "⚙️", "achievements.server_industrial_name", "epic", "achievements.server_industrial_desc", fixes, 2000),
            FormatAchievementEntry(settings, "🤨", "achievements.server_seriously_name", "epic", "achievements.server_seriously_desc", fixes, 2500),
            FormatAchievementEntry(settings, "🌱", "achievements.server_grass_name", "legendary", "achievements.server_grass_desc", fixes, 5000),
            FormatAchievementEntry(settings, "💀", "achievements.server_done_name", "legendary", "achievements.server_done_desc", fixes, 10000)
        };
        bool multimedia = new[] { "twitter", "tiktok", "instagram" }.All(p => stats?.PlatformUsage?.TryGetValue(p, out long count) == true && count >= 100);
        string multi = $"📡 **{L(settings, "achievements.server_multimedia_name")}** `{Rarity(settings, "epic")}`\n{L(settings, "achievements.server_multimedia_desc")}";
        lines.Add(multimedia ? multi + "\n" + L(settings, "achievements.unlocked") : "🔒" + multi[1..]);
        return new EmbedBuilder().WithTitle(L(settings, "achievements.server_title", guild.Name)).WithDescription(string.Join("\n\n", lines))
            .WithColor(Color.DarkTeal).WithFooter(L(settings, "achievements.server_footer", GetServerAchievements(stats, settings).Count, 11)).Build();
    }

    private async Task SendGuildUsageBreakdownAsync(SocketTextChannel textChannel)
    {
        if (!_guildUsageStats.TryGetValue(textChannel.Guild.Id, out GuildUsageStats? stats) || stats.PlatformUsage.Count == 0)
        {
            await textChannel.SendMessageAsync(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "stats.no_usage_data"));
            return;
        }

        long total = stats.PlatformUsage.Values.Sum();
        if (total <= 0)
        {
            await textChannel.SendMessageAsync(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "stats.no_usage_data"));
            return;
        }

        string lines = string.Join("\n", stats.PlatformUsage
            .OrderByDescending(x => x.Value)
            .Select(x =>
            {
                double percent = (x.Value / (double)total) * 100;
                return $"**{FormatPlatformName(x.Key)}:** {percent:F1}% ({x.Value})";
            }));

        var embed = new EmbedBuilder()
            .WithTitle(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "stats.usage_breakdown_title", textChannel.Guild.Name))
            .WithDescription(lines)
            .WithColor(Color.DarkBlue)
            .WithFooter(L(GetOrCreateGuildSettings(textChannel.Guild.Id), "stats.total_platform_hits", total))
            .Build();

        await textChannel.SendMessageAsync(embed: embed);
    }

    private bool TryExtractMessageIdFromDiscordLink(string input, out ulong messageId)
    {
        messageId = 0;

        if (string.IsNullOrWhiteSpace(input))
            return false;

        string trimmed = input.Trim().Trim('<', '>');
        Match match = Regex.Match(trimmed, @"https?://(?:canary\.|ptb\.)?discord\.com/channels/\d+/\d+/(\d+)", RegexOptions.IgnoreCase);
        if (!match.Success)
            return false;

        return ulong.TryParse(match.Groups[1].Value, out messageId);
    }

    private string FormatRelayProviders(RelayMessageState state)
    {
        if (state.Platforms.Count == 0)
            return "Unknown";

        var parts = new List<string>();

        foreach (string platform in state.Platforms.Distinct())
        {
            if (!_providers.TryGetValue(platform, out List<string>? providers) || providers.Count == 0)
            {
                parts.Add($"{FormatPlatformName(platform)}: unknown");
                continue;
            }

            int index = state.ProviderIndexes.TryGetValue(platform, out int providerIndex) ? providerIndex : 0;
            index = Math.Clamp(index, 0, providers.Count - 1);
            parts.Add($"{FormatPlatformName(platform)}: {providers[index]}");
        }

        return string.Join("\n", parts);
    }

    private async Task SendApolloBotHelp(SocketTextChannel channel, SocketUser user)
    {
        bool isAdmin = user is SocketGuildUser guildUser && guildUser.GuildPermissions.ManageGuild;
        List<string> lines = BuildApolloBotHelpLines(isAdmin, GetOrCreateGuildSettings(channel.Guild.Id));

        await SendPaginatedEmbedAsync(
            channel,
            L(GetOrCreateGuildSettings(channel.Guild.Id), "help.title"),
            lines,
            "abhelp",
            page: 0,
            pageSize: DefaultPageSize,
            color: Color.Teal,
            headerText: L(GetOrCreateGuildSettings(channel.Guild.Id), "help.header"));
    }

    private async Task SendAbout(SocketTextChannel channel)
    {
        GuildSettings settings = GetOrCreateGuildSettings(channel.Guild.Id);
        var embed = new EmbedBuilder().WithTitle(L(settings, "about.title")).WithDescription(L(settings, "about.description"))
            .AddField(L(settings, "about.platforms_title"), L(settings, "about.platforms"), false)
            .AddField(L(settings, "about.features_title"), L(settings, "about.features"), false).WithColor(Color.Gold).Build();
        await channel.SendMessageAsync(embed: embed);
    }

    private async Task SendRandomAnimalAsync(ulong userId, SocketTextChannel channel, string animal)
    {
        DateTime now = DateTime.UtcNow;
        if (_animalCooldowns.TryGetValue(userId, out DateTime lastUsed) &&
            now - lastUsed < TimeSpan.FromSeconds(5))
        {
            int remaining = Math.Max(1, (int)Math.Ceiling((TimeSpan.FromSeconds(5) - (now - lastUsed)).TotalSeconds));
            await channel.SendMessageAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "animals.cooldown", remaining));
            return;
        }

        _animalCooldowns[userId] = now;

        string emoji = animal switch { "cat" => "🐱", "dog" => "🐶", _ => "🦊" };
        string displayName = animal switch { "cat" => "cat", "dog" => "dog", _ => "fox" };

        try
        {
            string? imageUrl = null;
            _lastAnimalImageUrls.TryGetValue(animal, out string? previousImageUrl);

            for (int attempt = 0; attempt < 3; attempt++)
            {
                if (animal == "fox")
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get,
                        $"https://randomfox.ca/floof/?ts={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{attempt}");
                    request.Headers.UserAgent.ParseAdd("ApolloBot/1.0 (+https://apollobotdiscord.netlify.app/)");
                    request.Headers.Accept.ParseAdd("application/json");
                    using HttpResponseMessage response = await AnimalHttpClient.SendAsync(request);
                    response.EnsureSuccessStatusCode();
                    using JsonDocument document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
                    if (document.RootElement.TryGetProperty("image", out JsonElement imageElement))
                        imageUrl = imageElement.GetString()?.Trim();
                }
                else if (animal == "dog")
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get,
                        $"https://dog.ceo/api/breeds/image/random?ts={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{attempt}");
                    request.Headers.UserAgent.ParseAdd("ApolloBot/1.0 (+https://apollobotdiscord.netlify.app/)");
                    request.Headers.Accept.ParseAdd("application/json");
                    using HttpResponseMessage response = await AnimalHttpClient.SendAsync(request);
                    response.EnsureSuccessStatusCode();
                    using JsonDocument document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
                    if (document.RootElement.TryGetProperty("message", out JsonElement imageElement))
                        imageUrl = imageElement.GetString()?.Trim();
                }
                else
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get,
                        $"https://cataas.com/cat?json=true&ts={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{attempt}");
                    request.Headers.UserAgent.ParseAdd("ApolloBot/1.0 (+https://apollobotdiscord.netlify.app/)");
                    request.Headers.Accept.ParseAdd("application/json");
                    using HttpResponseMessage response = await AnimalHttpClient.SendAsync(request);
                    response.EnsureSuccessStatusCode();
                    using JsonDocument document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
                    if (document.RootElement.TryGetProperty("url", out JsonElement urlElement))
                    {
                        string? catUrl = urlElement.GetString()?.Trim();
                        if (!string.IsNullOrWhiteSpace(catUrl))
                            imageUrl = catUrl.StartsWith("http", StringComparison.OrdinalIgnoreCase) ? catUrl : $"https://cataas.com{catUrl}";
                    }
                    else if (document.RootElement.TryGetProperty("_id", out JsonElement idElement))
                    {
                        string? catId = idElement.GetString()?.Trim();
                        if (!string.IsNullOrWhiteSpace(catId))
                            imageUrl = $"https://cataas.com/cat/{catId}";
                    }
                }

                if (!string.IsNullOrWhiteSpace(imageUrl) &&
                    !string.Equals(imageUrl, previousImageUrl, StringComparison.OrdinalIgnoreCase))
                    break;
            }

            if (string.IsNullOrWhiteSpace(imageUrl) ||
                !Uri.TryCreate(imageUrl, UriKind.Absolute, out Uri? validatedImageUri) ||
                validatedImageUri.Scheme != Uri.UriSchemeHttps)
                throw new InvalidOperationException($"{displayName} service returned an invalid image URL.");

            imageUrl = validatedImageUri.ToString();
            _lastAnimalImageUrls[animal] = imageUrl;

            string issuerName = channel.Guild.GetUser(userId)?.DisplayName
                ?? _client?.GetUser(userId)?.GlobalName
                ?? _client?.GetUser(userId)?.Username
                ?? userId.ToString();

            var embed = new EmbedBuilder()
                .WithTitle(L(GetOrCreateGuildSettings(channel.Guild.Id), "animals.title", emoji, displayName))
                .WithImageUrl(imageUrl)
                .WithFooter($"Issued by: {issuerName}")
                .WithColor(animal == "fox" ? Color.Orange : animal == "cat" ? Color.Purple : Color.Blue)
                .Build();

            await channel.SendMessageAsync(embed: embed);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ANIMAL:{animal.ToUpperInvariant()}] Failed to fetch random {displayName}: {ex.Message}");
            _animalCooldowns.TryRemove(userId, out _);
            await channel.SendMessageAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "animals.unavailable", emoji, displayName));
        }
    }

    private async Task SendProviders(SocketTextChannel channel, ulong? ownerUserId = null)
    {
        var lines = _providers
            .OrderBy(p => p.Key)
            .Select(p => $"**{FormatPlatformName(p.Key)}**: {string.Join(", ", p.Value)}");

        var embed = new EmbedBuilder()
            .WithTitle(L(GetOrCreateGuildSettings(channel.Guild.Id), "providers.title"))
            .WithDescription(string.Join("\n", lines))
            .WithColor(Color.LightGrey)
            .Build();

        if (ownerUserId.HasValue)
            await SendBotOwnerMessageAsync(channel, ownerUserId.Value, embed: embed);
        else
            await channel.SendMessageAsync(embed: embed);
    }


    private async Task SendVoteMessage(SocketTextChannel channel, SocketUser user)
    {
        var embed = new EmbedBuilder()
            .WithTitle(L(GetOrCreateGuildSettings(channel.Guild.Id), "vote.title"))
            .WithDescription(L(GetOrCreateGuildSettings(channel.Guild.Id), "vote.description", user.Mention))
            .WithColor(Color.Gold)
            .Build();

        await channel.SendMessageAsync(embed: embed);
    }


    private async Task SendSupportMessage(SocketTextChannel channel)
    {
        if (string.IsNullOrWhiteSpace(_supportUrl))
        {
            await channel.SendMessageAsync(
                L(GetOrCreateGuildSettings(channel.Guild.Id), "support.not_configured"));
            return;
        }

        var embed = new EmbedBuilder()
            .WithTitle(L(GetOrCreateGuildSettings(channel.Guild.Id), "support.title"))
            .WithDescription(
                L(GetOrCreateGuildSettings(channel.Guild.Id), "support.description", _supportUrl))
            .WithColor(Color.Gold)
            .WithFooter(L(GetOrCreateGuildSettings(channel.Guild.Id), "support.footer"))
            .Build();

        var components = new ComponentBuilder()
            .WithButton(L(GetOrCreateGuildSettings(channel.Guild.Id), "support.button"), url: _supportUrl, style: ButtonStyle.Link)
            .Build();

        await channel.SendMessageAsync(embed: embed, components: components);
    }

    private async Task SendPlannedUpdates(SocketTextChannel channel, ulong? ownerUserId = null)
    {
        if (_plannedUpdates.Count == 0)
        {
            if (ownerUserId.HasValue)
                await SendBotOwnerMessageAsync(channel, ownerUserId.Value, "There are no planned updates listed right now.");
            else
                await channel.SendMessageAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "updates.none"));
            return;
        }

        string lines = string.Join("\n", _plannedUpdates.Select(kvp => $"`{kvp.Key}.` {kvp.Value}"));

        var embed = new EmbedBuilder()
            .WithTitle(L(GetOrCreateGuildSettings(channel.Guild.Id), "updates.title"))
            .WithDescription(lines)
            .WithColor(Color.Blue)
            .WithFooter(L(GetOrCreateGuildSettings(channel.Guild.Id), "updates.footer"))
            .Build();

        if (ownerUserId.HasValue)
            await SendBotOwnerMessageAsync(channel, ownerUserId.Value, embed: embed);
        else
            await channel.SendMessageAsync(embed: embed);
    }

    private async Task HandlePlannedUpdateOwnerCommand(SocketTextChannel channel, ulong ownerUserId, string[] parts)
    {
        if (parts.Length < 3)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, 
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
            await SendPlannedUpdates(channel, ownerUserId);
            return;
        }

        if (action == "clear")
        {
            _plannedUpdates.Clear();
            SavePlannedUpdates();
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Cleared all planned updates.");
            return;
        }

        if (action == "remove")
        {
            if (parts.Length < 4 || !int.TryParse(parts[3], out int removeId) || removeId <= 0)
            {
                await SendBotOwnerMessageAsync(channel, ownerUserId, "Please provide a valid positive update ID.");
                return;
            }

            bool removed = _plannedUpdates.Remove(removeId);
            SavePlannedUpdates();
            await SendBotOwnerMessageAsync(channel, ownerUserId, removed
                ? $"Removed update `{removeId}`."
                : $"Update `{removeId}` was not found.");
            return;
        }

        if (action is not ("add" or "edit"))
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Unknown update action. Use add, edit, remove, list, or clear.");
            return;
        }

        if (parts.Length < 5 || !int.TryParse(parts[3], out int id) || id <= 0)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Please provide a valid positive update ID.");
            return;
        }

        string textValue = string.Join(" ", parts.Skip(4)).Trim();

        if (string.IsNullOrWhiteSpace(textValue))
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Please provide update text.");
            return;
        }

        _plannedUpdates[id] = textValue;
        SavePlannedUpdates();

        await SendBotOwnerMessageAsync(channel, ownerUserId, action == "add"
            ? $"Added update `{id}`."
            : $"Updated update `{id}`.");
    }

    private async Task HandleSpecialTwitterUserCommand(SocketTextChannel channel, ulong ownerUserId, string[] parts)
    {
        if (parts.Length < 3)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, 
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
                await SendBotOwnerMessageAsync(channel, ownerUserId, "No users are approved for the silly Twitter/X providers.");
                return;
            }

            string users = string.Join("\n", _specialTwitterUsers.Select(x => $"• `{x}`"));
            await SendBotOwnerMessageAsync(channel, ownerUserId, $"**Approved silly Twitter/X users:**\n{users}");
            return;
        }

        if (action == "clear")
        {
            _specialTwitterUsers.Clear();
            SaveSpecialTwitterUsers();
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Cleared the silly Twitter/X user list.");
            return;
        }

        if (parts.Length < 4 || !ulong.TryParse(parts[3], out ulong userId))
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Please provide a valid user ID.");
            return;
        }

        if (action == "add")
        {
            bool added = _specialTwitterUsers.Add(userId);
            SaveSpecialTwitterUsers();
            await SendBotOwnerMessageAsync(channel, ownerUserId, added
                ? $"Added `{userId}` to the silly Twitter/X list."
                : $"`{userId}` is already on the silly Twitter/X list.");
            return;
        }

        if (action == "remove")
        {
            bool removed = _specialTwitterUsers.Remove(userId);
            SaveSpecialTwitterUsers();
            await SendBotOwnerMessageAsync(channel, ownerUserId, removed
                ? $"Removed `{userId}` from the silly Twitter/X list."
                : $"`{userId}` was not on the silly Twitter/X list.");
            return;
        }

        await SendBotOwnerMessageAsync(channel, ownerUserId, "Unknown special action. Use add, remove, list, or clear.");
    }

    private async Task HandleProviderCommand(SocketTextChannel channel, ulong ownerUserId, string[] parts)
    {
        if (parts.Length < 3)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, 
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
                await SendProviders(channel, ownerUserId);
                return;
            }

            string platform = parts[3].ToLowerInvariant();

            if (!_providers.TryGetValue(platform, out List<string>? listedProviders))
            {
                await SendBotOwnerMessageAsync(channel, ownerUserId, "Invalid platform. Use twitter, reddit, tiktok, instagram, kick, bluesky, or threads.");
                return;
            }

            await SendBotOwnerMessageAsync(channel, ownerUserId, 
                $"**{FormatPlatformName(platform)} providers:** {string.Join(", ", listedProviders)}");
            return;
        }

        if (parts.Length < 4)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Please provide a platform.");
            return;
        }

        string targetPlatform = parts[3].ToLowerInvariant();

        if (!_providers.ContainsKey(targetPlatform))
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Invalid platform. Use twitter, reddit, tiktok, instagram, kick, bluesky, or threads.");
            return;
        }

        if (action == "clear")
        {
            _providers[targetPlatform].Clear();
            SaveProviders();
            await SendBotOwnerMessageAsync(channel, ownerUserId, $"Cleared all providers for {FormatPlatformName(targetPlatform)}.");
            return;
        }

        if (parts.Length < 5)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Please provide a provider domain.");
            return;
        }

        string domain = SanitizeProviderDomain(parts[4]);

        if (string.IsNullOrWhiteSpace(domain))
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Please provide a valid provider domain.");
            return;
        }

        if (action == "add")
        {
            if (_providers[targetPlatform].Any(x => string.Equals(x, domain, StringComparison.OrdinalIgnoreCase)))
            {
                await SendBotOwnerMessageAsync(channel, ownerUserId, $"{domain} is already configured for {FormatPlatformName(targetPlatform)}.");
                return;
            }

            _providers[targetPlatform].Add(domain);
            SaveProviders();
            await SendBotOwnerMessageAsync(channel, ownerUserId, $"Added `{domain}` to {FormatPlatformName(targetPlatform)} providers.");
            return;
        }

        if (action == "remove")
        {
            string? existing = _providers[targetPlatform]
                .FirstOrDefault(x => string.Equals(x, domain, StringComparison.OrdinalIgnoreCase));

            if (existing == null)
            {
                await SendBotOwnerMessageAsync(channel, ownerUserId, $"{domain} was not found for {FormatPlatformName(targetPlatform)}.");
                return;
            }

            _providers[targetPlatform].Remove(existing);
            SaveProviders();
            await SendBotOwnerMessageAsync(channel, ownerUserId, $"Removed `{existing}` from {FormatPlatformName(targetPlatform)} providers.");
            return;
        }

        await SendBotOwnerMessageAsync(channel, ownerUserId, "Unknown provider action. Use add, remove, list, or clear.");
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
                await textChannel.SendMessageAsync(L(settings, "whitelist.empty"));
                return;
            }

            string listedChannels = string.Join(
                "\n",
                settings.WhitelistedChannelIds.Select(id => $"• <#{id}>"));

            await textChannel.SendMessageAsync(L(settings, "whitelist.list", listedChannels));
            return;
        }

        if (action == "clear")
        {
            settings.WhitelistedChannelIds.Clear();
            SaveGuildSettings();

            await textChannel.SendMessageAsync(L(settings, "whitelist.cleared"));
            return;
        }

        ulong targetChannelId = textChannel.Id;

        if (message.MentionedChannels.Count > 0)
            targetChannelId = message.MentionedChannels.First().Id;

        if (action == "add")
        {
            if (settings.WhitelistedChannelIds.Contains(targetChannelId))
            {
                await textChannel.SendMessageAsync(L(settings, "whitelist.already", targetChannelId));
                return;
            }

            settings.WhitelistedChannelIds.Add(targetChannelId);
            SaveGuildSettings();

            await textChannel.SendMessageAsync(L(settings, "whitelist.added", targetChannelId));
            return;
        }

        if (action == "remove")
        {
            bool removed = settings.WhitelistedChannelIds.Remove(targetChannelId);
            SaveGuildSettings();

            if (removed)
                await textChannel.SendMessageAsync(L(settings, "whitelist.removed", targetChannelId));
            else
                await textChannel.SendMessageAsync(L(settings, "whitelist.not_present", targetChannelId));

            return;
        }

        await textChannel.SendMessageAsync(L(settings, "whitelist.unknown_action"));
    }

    private async Task SendGuildStatus(SocketTextChannel channel, GuildSettings settings)
    {
        string whitelistText = settings.WhitelistedChannelIds.Count == 0 ? L(settings, "setup.all_channels") : string.Join("\n", settings.WhitelistedChannelIds.Select(id => $"• <#{id}>"));
        var embed = new EmbedBuilder().WithTitle(L(settings, "status.title")).WithDescription(L(settings, "status.description", channel.Guild.Name))
            .AddField(L(settings, "status.status"), L(settings, settings.Enabled ? "common.enabled" : "common.disabled"), true)
            .AddField(L(settings, "setup.silent_mode"), L(settings, settings.SilentMode ? "common.enabled" : "common.disabled"), true)
            .AddField(L(settings, "setup.relay_buttons"), L(settings, settings.ButtonsEnabled ? "common.enabled" : "common.disabled"), true)
            .AddField(L(settings, "setup.button_cooldown"), L(settings, "common.seconds", settings.ButtonCooldownSeconds), true)
            .AddField(L(settings, "status.allowed_channels"), whitelistText, false).WithColor(settings.Enabled ? Color.Green : Color.Red).WithCurrentTimestamp().Build();
        await channel.SendMessageAsync(embed: embed);
    }

    private async Task SendPermissionReport(SocketTextChannel channel)
    {
        if (_client?.CurrentUser == null)
        {
            await channel.SendMessageAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.current_user_null"));
            return;
        }

        SocketGuildUser? botUser = channel.Guild.GetUser(_client.CurrentUser.Id);
        if (botUser == null)
        {
            await channel.SendMessageAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.bot_unresolved"));
            return;
        }

        ChannelPermissions perms = botUser.GetPermissions(channel);
        GuildPermissions guildPerms = botUser.GuildPermissions;
        List<string> missing = GetLikelyMissingPermissions(channel);

        string likelyMissingText = missing.Count == 0
            ? L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.none_missing")
            : string.Join(", ", missing);

        var embed = new EmbedBuilder()
            .WithTitle(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.title"))
            .WithDescription(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.description", channel.Guild.Name, channel.Id))
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.view_channel"), perms.ViewChannel, true)
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.send_messages"), perms.SendMessages, true)
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.embed_links"), perms.EmbedLinks, true)
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.read_history"), perms.ReadMessageHistory, true)
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.manage_messages"), perms.ManageMessages, true)
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.manage_webhooks"), perms.ManageWebhooks, true)
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.app_commands"), guildPerms.UseApplicationCommands, true)
            .AddField(L(GetOrCreateGuildSettings(channel.Guild.Id), "perms.likely_missing"), likelyMissingText, false)
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
            switch (command.Data.Name)
            {
                case "roll": if (await StopIfOptionalSlashCommandDisabledAsync(command, "roll")) return; await HandleRollSlashCommand(command); return;
                case "fix": await HandleFixSlashCommand(command); return;
                case "usersettings": await HandleUserSettingsSlashCommand(command); return;
                case "userstats": if (await StopIfOptionalSlashCommandDisabledAsync(command, "userstats")) return; await HandleUserStatsSlashCommand(command); return;
                case "serverstats": if (await StopIfOptionalSlashCommandDisabledAsync(command, "serverstats")) return; await HandleServerStatsSlashCommand(command); return;
                case "fox": case "cat": case "dog": if (await StopIfOptionalSlashCommandDisabledAsync(command, command.Data.Name)) return; await HandleAnimalSlashCommand(command); return;
                case "ping": await command.RespondAsync(LI(command, "common.pong", _client?.Latency ?? 0)); return;
                case "embedfix": case "silent": case "togglebuttons": case "cooldown": case "whitelist": case "reset":
                    await HandleAdminSlashCommand(command); return;
                case "setup": await HandleSetupSlashCommand(command); return;
                case "help": case "about": case "updates": case "support": case "vote": case "providers": case "perms": case "status": case "info":
                    await HandleLegacyPublicSlashCommand(command); return;
                default:
                    if (!command.HasResponded)
                        await command.RespondAsync(LI(command, "errors.unrecognized_slash"), ephemeral: true);
                    return;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error handling slash command '{command.Data.Name}': {ex}");
            try
            {
                string responseText = command.Channel is SocketTextChannel textChannel && IsMissingPermissionsError(ex)
                    ? BuildSlashPermissionFailureMessage(command, textChannel)
                    : LI(command, "errors.command_failed");
                if (!command.HasResponded) await command.RespondAsync(responseText, ephemeral: true);
                else await command.FollowupAsync(responseText, ephemeral: true);
            }
            catch { }
        }
    }

    private async Task HandleUserSettingsSlashCommand(SocketSlashCommand command)
    {
        UserIgnoreSettings settings = GetOrCreateUserIgnoreSettings(command.User.Id);
        ulong guildId = command.Channel is SocketTextChannel channel ? channel.Guild.Id : 0UL;

        await command.RespondAsync(
            embed: BuildUserSettingsEmbed(settings, guildId),
            components: BuildUserSettingsComponents(settings, command.User.Id, guildId, "twitter"),
            ephemeral: true);
    }

    private async Task HandleUserStatsSlashCommand(SocketSlashCommand command)
    {
        IUser target = command.Data.Options.FirstOrDefault(x => x.Name == "user")?.Value as IUser ?? command.User;
        string displayName = target.GlobalName ?? target.Username;
        string avatarUrl = target.GetAvatarUrl(ImageFormat.Auto, 256) ?? target.GetDefaultAvatarUrl() ?? "";
        await command.RespondAsync(embed: BuildUserStatsEmbed(target.Id, displayName, avatarUrl, GetOrCreateGuildSettings(((SocketTextChannel)command.Channel).Guild.Id)), components: BuildStatsProfileComponents("user", target.Id, command.User.Id, GetOrCreateGuildSettings(((SocketTextChannel)command.Channel).Guild.Id)));
    }

    private async Task HandleServerStatsSlashCommand(SocketSlashCommand command)
    {
        if (command.Channel is not SocketTextChannel channel) { await command.RespondAsync(LI(command, "errors.server_only"), ephemeral: true); return; }
        await command.RespondAsync(embed: BuildServerStatsEmbed(channel.Guild, GetOrCreateGuildSettings(channel.Guild.Id)), components: BuildStatsProfileComponents("server", channel.Guild.Id, command.User.Id, GetOrCreateGuildSettings(channel.Guild.Id)));
    }

    private async Task HandleAnimalSlashCommand(SocketSlashCommand command)
    {
        if (command.Channel is not SocketTextChannel channel) { await command.RespondAsync(LU(GetOrCreateUserIgnoreSettings(command.User.Id), "errors.animals_server_only"), ephemeral: true); return; }
        await command.DeferAsync(ephemeral: true);
        await SendRandomAnimalAsync(command.User.Id, channel, command.Data.Name);
        await command.DeleteOriginalResponseAsync();
    }

    private bool SlashUserCanManageGuild(SocketSlashCommand command) =>
        command.User is SocketGuildUser guildUser && guildUser.GuildPermissions.ManageGuild;

    private string L(GuildSettings settings, string key, params object?[] args) =>
        _localization.Get(key, settings.LanguageCode, args);

    private string LT(string languageCode, string key, params object?[] args) =>
        _localization.Get(key, languageCode, args);

    private string LU(UserIgnoreSettings settings, string key, params object?[] args) =>
        _localization.Get(key, settings.LanguageCode, args);

    private string LI(SocketSlashCommand command, string key, params object?[] args) =>
        command.Channel is SocketTextChannel textChannel
            ? L(GetOrCreateGuildSettings(textChannel.Guild.Id), key, args)
            : LU(GetOrCreateUserIgnoreSettings(command.User.Id), key, args);

    private string LC(SocketMessageComponent component, string key, params object?[] args) =>
        component.Channel is SocketTextChannel textChannel
            ? L(GetOrCreateGuildSettings(textChannel.Guild.Id), key, args)
            : LU(GetOrCreateUserIgnoreSettings(component.User.Id), key, args);

    private Embed BuildServerSetupEmbed(SocketGuild guild, GuildSettings settings)
    {
        settings.DisabledUserCommands ??= new List<string>();
        string channels = settings.WhitelistedChannelIds.Count == 0
            ? L(settings, "setup.all_channels")
            : L(settings, "setup.whitelisted_channels", settings.WhitelistedChannelIds.Count);
        string optional = settings.DisabledUserCommands.Count == 0
            ? L(settings, "setup.all_enabled")
            : L(settings, "setup.disabled_count", settings.DisabledUserCommands.Count);

        return new EmbedBuilder()
            .WithTitle(L(settings, "setup.title"))
            .WithDescription(L(settings, "setup.description"))
            .AddField(L(settings, "setup.embed_fixing"), L(settings, settings.Enabled ? "common.enabled" : "common.disabled"), true)
            .AddField(L(settings, "setup.channels"), channels, true)
            .AddField(L(settings, "setup.relay_buttons"), L(settings, settings.ButtonsEnabled ? "common.enabled" : "common.disabled"), true)
            .AddField(L(settings, "setup.silent_mode"), L(settings, settings.SilentMode ? "common.enabled" : "common.disabled"), true)
            .AddField(L(settings, "setup.button_cooldown"), L(settings, "common.seconds", settings.ButtonCooldownSeconds), true)
            .AddField(L(settings, "setup.optional_commands"), optional, true)
            .AddField(L(settings, "language.field"), GetLanguageDisplayName(settings.LanguageCode), true)
            .WithColor(Color.Teal)
            .Build();
    }

    private MessageComponent BuildServerSetupComponents(ulong guildId)
    {
        GuildSettings settings = GetOrCreateGuildSettings(guildId);
        return new ComponentBuilder()
            .WithButton(L(settings, "setup.command_settings_button"), $"serversetup_commands:{guildId}", ButtonStyle.Primary)
            .WithButton(L(settings, "language.button"), $"serversetup_language:{guildId}", ButtonStyle.Secondary)
            .WithButton(L(settings, "common.refresh"), $"serversetup_refresh:{guildId}", ButtonStyle.Secondary)
            .Build();
    }

    private static string GetLanguageDisplayName(string? languageCode)
    {
        return languageCode?.ToLowerInvariant() switch
        {
            "en-gb" => "🇬🇧 English (UK)",
            "en-us" => "🇺🇸 English (US)",
            "de-de" => "🇩🇪 Deutsch",
            "es-es" => "🇪🇸 Español",
            "fr-fr" => "🇫🇷 Français",
            "pt-br" => "🇧🇷 Português (Brasil)",
            _ => $"🌐 {languageCode ?? LocalizationManager.DefaultLanguage}"
        };
    }

    private Embed BuildLanguageSettingsEmbed(GuildSettings settings)
    {
        return new EmbedBuilder()
            .WithTitle(L(settings, "language.title"))
            .WithDescription(L(settings, "language.description"))
            .AddField(L(settings, "language.current"), GetLanguageDisplayName(settings.LanguageCode), false)
            .WithColor(Color.Teal)
            .Build();
    }

    private MessageComponent BuildLanguageSettingsComponents(GuildSettings settings, ulong guildId)
    {
        string current = _localization.NormalizeLanguage(settings.LanguageCode);
        var menu = new SelectMenuBuilder()
            .WithCustomId($"serversetup_setlanguage:{guildId}")
            .WithPlaceholder(L(settings, "language.select"))
            .WithMinValues(1)
            .WithMaxValues(1);

        foreach (string language in _localization.AvailableLanguages.OrderBy(x => x))
        {
            menu.AddOption(
                GetLanguageDisplayName(language),
                language,
                isDefault: language.Equals(current, StringComparison.OrdinalIgnoreCase));
        }

        return new ComponentBuilder()
            .WithSelectMenu(menu)
            .WithButton(L(settings, "common.back"), $"serversetup_back:{guildId}", ButtonStyle.Secondary)
            .Build();
    }

    private Embed BuildCommandSettingsEmbed(GuildSettings settings)
    {
        settings.DisabledUserCommands ??= new List<string>();
        string disabled = settings.DisabledUserCommands.Count == 0
            ? L(settings, "common.none")
            : string.Join("\n", settings.DisabledUserCommands.Where(OptionalUserCommands.Contains).Select(x => $"• {FormatOptionalCommandName(x)}"));

        return new EmbedBuilder()
            .WithTitle(L(settings, "setup.optional_title"))
            .WithDescription(L(settings, "setup.optional_description"))
            .AddField(L(settings, "setup.currently_disabled"), disabled, false)
            .WithColor(Color.Teal)
            .Build();
    }

    private MessageComponent BuildCommandSettingsComponents(GuildSettings settings, ulong guildId)
    {
        settings.DisabledUserCommands ??= new List<string>();
        var menu = new SelectMenuBuilder()
            .WithCustomId($"serversetup_disabledcommands:{guildId}")
            .WithPlaceholder(L(settings, "setup.select_commands"))
            .WithMinValues(0)
            .WithMaxValues(OptionalUserCommands.Length);

        foreach (string command in OptionalUserCommands)
        {
            menu.AddOption(FormatOptionalCommandName(command), command, L(settings, "setup.disable_command_description", command),
                isDefault: settings.DisabledUserCommands.Contains(command, StringComparer.OrdinalIgnoreCase));
        }

        return new ComponentBuilder()
            .WithSelectMenu(menu)
            .WithButton(L(settings, "setup.enable_all"), $"serversetup_enableall:{guildId}", ButtonStyle.Secondary)
            .WithButton(L(settings, "common.back"), $"serversetup_back:{guildId}", ButtonStyle.Secondary)
            .Build();
    }

    private async Task HandleSetupSlashCommand(SocketSlashCommand command)
    {
        if (command.Channel is not SocketTextChannel channel) { await command.RespondAsync(LU(GetOrCreateUserIgnoreSettings(command.User.Id), "errors.setup_server_only"), ephemeral: true); return; }
        if (!SlashUserCanManageGuild(command)) { await command.RespondAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "errors.manage_server_setup"), ephemeral: true); return; }
        GuildSettings settings = GetOrCreateGuildSettings(channel.Guild.Id);
        await command.RespondAsync(embed: BuildServerSetupEmbed(channel.Guild, settings), components: BuildServerSetupComponents(channel.Guild.Id), ephemeral: true);
    }

    private async Task HandleAdminSlashCommand(SocketSlashCommand command)
    {
        if (command.Channel is not SocketTextChannel channel) { await command.RespondAsync(LI(command, "errors.server_only"), ephemeral: true); return; }
        if (!SlashUserCanManageGuild(command)) { await command.RespondAsync(L(GetOrCreateGuildSettings(channel.Guild.Id), "errors.manage_server"), ephemeral: true); return; }
        GuildSettings settings = GetOrCreateGuildSettings(channel.Guild.Id);
        string name = command.Data.Name;
        if (name == "embedfix") { settings.Enabled = Convert.ToBoolean(command.Data.Options.First(x => x.Name == "enabled").Value); SaveGuildSettings(); await command.RespondAsync(L(settings, settings.Enabled ? "admin.embed_enabled_check" : "admin.embed_disabled_check"), ephemeral: true); return; }
        if (name == "silent") { settings.SilentMode = Convert.ToBoolean(command.Data.Options.First(x => x.Name == "enabled").Value); SaveGuildSettings(); await command.RespondAsync(L(settings, settings.SilentMode ? "admin.silent_enabled_check" : "admin.silent_disabled_check"), ephemeral: true); return; }
        if (name == "togglebuttons") { settings.ButtonsEnabled = Convert.ToBoolean(command.Data.Options.First(x => x.Name == "enabled").Value); SaveGuildSettings(); await command.RespondAsync(L(settings, settings.ButtonsEnabled ? "admin.buttons_enabled" : "admin.buttons_disabled"), ephemeral: true); return; }
        if (name == "cooldown") { settings.ButtonCooldownSeconds = Convert.ToInt32(command.Data.Options.First(x => x.Name == "seconds").Value); SaveGuildSettings(); await command.RespondAsync(L(settings, "admin.cooldown_set", settings.ButtonCooldownSeconds), ephemeral: true); return; }
        if (name == "reset") { settings.Enabled = true; settings.SilentMode = false; settings.ButtonsEnabled = true; settings.ButtonCooldownSeconds = 3; settings.WhitelistedChannelIds.Clear(); settings.DisabledUserCommands.Clear(); SaveGuildSettings(); await command.RespondAsync(L(settings, "admin.reset_defaults"), ephemeral: true); return; }
        if (name == "whitelist")
        {
            string action = command.Data.Options.First(x => x.Name == "action").Value?.ToString() ?? "list";
            SocketGuildChannel? target = command.Data.Options.FirstOrDefault(x => x.Name == "channel")?.Value as SocketGuildChannel;
            if (action == "list") { string list = settings.WhitelistedChannelIds.Count == 0 ? L(settings, "whitelist.none") : string.Join("\n", settings.WhitelistedChannelIds.Select(id => $"<#{id}>")); await command.RespondAsync(list, ephemeral: true); return; }
            if (action == "clear") { settings.WhitelistedChannelIds.Clear(); SaveGuildSettings(); await command.RespondAsync(L(settings, "whitelist.cleared_check"), ephemeral: true); return; }
            if (target == null) { await command.RespondAsync(L(settings, "whitelist.choose_channel"), ephemeral: true); return; }
            if (action == "add") { if (!settings.WhitelistedChannelIds.Contains(target.Id)) settings.WhitelistedChannelIds.Add(target.Id); SaveGuildSettings(); await command.RespondAsync(L(settings, "whitelist.added_check", target.Id), ephemeral: true); return; }
            if (action == "remove") { settings.WhitelistedChannelIds.Remove(target.Id); SaveGuildSettings(); await command.RespondAsync(L(settings, "whitelist.removed_check", target.Id), ephemeral: true); return; }
        }
    }

    private async Task HandleLegacyPublicSlashCommand(SocketSlashCommand command)
    {
        if (command.Data.Name is "updates" or "vote")
        {
            if (await StopIfOptionalSlashCommandDisabledAsync(command, command.Data.Name)) return;
        }
        if (command.Channel is not SocketTextChannel channel) { await command.RespondAsync(LU(GetOrCreateUserIgnoreSettings(command.User.Id), "errors.legacy_server_only"), ephemeral: true); return; }
        await command.DeferAsync(ephemeral: true);
        switch (command.Data.Name)
        {
            case "help": await SendApolloBotHelp(channel, command.User); break;
            case "about": await SendAbout(channel); break;
            case "updates": await SendPlannedUpdates(channel); break;
            case "support": await SendSupportMessage(channel); break;
            case "vote": await SendVoteMessage(channel, command.User); break;
            case "providers": await SendProviders(channel); break;
            case "perms": await SendPermissionReport(channel); break;
            case "status": await SendGuildStatus(channel, GetOrCreateGuildSettings(channel.Guild.Id)); break;
            case "info":
                string link = command.Data.Options.FirstOrDefault(x => x.Name == "message")?.Value?.ToString() ?? "";
                await HandleInfoCommand(channel, new[] { "info", link });
                break;
        }
        await command.DeleteOriginalResponseAsync();
    }

    private async Task HandleFixSlashCommand(SocketSlashCommand command)
    {
        string rawText = command.Data.Options
            .FirstOrDefault(x => x.Name == "url")?.Value?.ToString()?.Trim()
            ?? "";

        if (string.IsNullOrWhiteSpace(rawText))
        {
            await command.RespondAsync(LI(command, "fix.provide_link"), ephemeral: true);
            return;
        }

        List<string> detectedPlatforms = GetPlatformsInText(rawText);
        if (detectedPlatforms.Count == 0)
        {
            await command.RespondAsync(
                "I couldn't find a supported link in that input. Supported platforms: Twitter/X, Reddit, TikTok, Instagram, Kick, Bluesky, Threads (experimental).",
                ephemeral: true);
            return;
        }

        Dictionary<string, int> providerIndexes = CreateDefaultProviderIndexes(detectedPlatforms, command.User.Id);
        string newContent = ApplyAllReplacements(rawText, providerIndexes, command.User.Id);

        if (newContent == rawText)
        {
            await command.RespondAsync(LI(command, "fix.nothing_changed"), ephemeral: true);
            return;
        }

        if (command.Channel is not SocketTextChannel textChannel)
        {
            await command.RespondAsync(
                $"Here you go here's the fixed version:\n{newContent}",
                ephemeral: false);
            return;
        }

        if (!ShouldProcessMessageInChannel(textChannel))
        {
            await command.RespondAsync(
                "ApolloBot is disabled in this channel right now. Ask an admin to use `!embedfix on` or whitelist this channel.",
                ephemeral: true);
            return;
        }

        List<string> missing = GetLikelyMissingPermissions(textChannel);
        if (missing.Count > 0)
        {
            Console.WriteLine(
                $"[PRECHECK] Slash /fix may be missing permissions in guild '{textChannel.Guild.Name}' channel '#{textChannel.Name}': {string.Join(", ", missing)}");
        }

        await command.DeferAsync(ephemeral: true);

        RestWebhook? webhook = await GetOrCreateWebhookAsync(textChannel);
        if (webhook == null)
        {
            await command.FollowupAsync(
                LI(command, "fix.webhook_failed", newContent),
                ephemeral: true);
            return;
        }

        if (string.IsNullOrWhiteSpace(webhook.Token))
        {
            await command.FollowupAsync(
                LI(command, "fix.webhook_token_missing", newContent),
                ephemeral: true);
            return;
        }

        (string displayName, string avatarUrl) = GetRelayIdentity(command.User, textChannel.Guild);

        var webhookClient = new DiscordWebhookClient(webhook.Id, webhook.Token);

        MessageComponent buttons = BuildButtonsForGuild(textChannel.Guild.Id, detectedPlatforms, false);

        ulong relayedMessageId = await webhookClient.SendMessageAsync(
            text: newContent,
            username: displayName,
            avatarUrl: avatarUrl,
            components: buttons);

        _relayStates[relayedMessageId] = new RelayMessageState
        {
            OriginalContent = rawText,
            WebhookId = webhook.Id,
            WebhookToken = webhook.Token,
            OriginalAuthorId = command.User.Id,
            SilentMode = false,
            GuildId = textChannel.Guild.Id,
            Platforms = detectedPlatforms,
            ProviderIndexes = providerIndexes
        };

        SaveRelayStates();
        IncrementEmbedsFixedCount();
        RecordGuildEmbedFix(textChannel.Guild, detectedPlatforms);
        RecordUserEmbedFix(command.User, textChannel.Guild.Id, detectedPlatforms);

        await command.FollowupAsync(LI(command, "fix.done"), ephemeral: true);
    }

    private async Task HandleRollSlashCommand(SocketSlashCommand command)
    {
        string diceText = "1d20";
        string modeText = "normal";
        int exhaustionLevel = 0;
        bool resistant = false;
        bool vulnerable = false;

        foreach (SocketSlashCommandDataOption option in command.Data.Options)
        {
            if (option.Name == "dice" && option.Value is string diceValue && !string.IsNullOrWhiteSpace(diceValue))
                diceText = diceValue.Trim();

            if (option.Name == "mode" && option.Value is string modeValue && !string.IsNullOrWhiteSpace(modeValue))
                modeText = modeValue.Trim().ToLowerInvariant();

            if (option.Name == "exhaustion" && option.Value != null)
                exhaustionLevel = Convert.ToInt32(option.Value, CultureInfo.InvariantCulture);

            if (option.Name == "resistant" && option.Value is bool resistantValue)
                resistant = resistantValue;

            if (option.Name == "vulnerable" && option.Value is bool vulnerableValue)
                vulnerable = vulnerableValue;
        }

        if (resistant && vulnerable)
        {
            await command.RespondAsync(
                LI(command, "roll.resist_or_vulnerable"),
                ephemeral: true);
            return;
        }

        if (exhaustionLevel < 0 || exhaustionLevel > 6)
        {
            await command.RespondAsync(LI(command, "roll.exhaustion_range"), ephemeral: true);
            return;
        }

        RollParseResult parseResult = ParseRollCommand(diceText);

        if (!parseResult.Success || parseResult.Request == null)
        {
            await command.RespondAsync(
                LI(command, "roll.invalid_format"),
                ephemeral: true);
            return;
        }

        RollRequest request = parseResult.Request;

        request.Advantage = modeText == "advantage";
        request.Disadvantage = modeText == "disadvantage";
        request.ExhaustionLevel = exhaustionLevel;
        request.Resistant = resistant;
        request.Vulnerable = vulnerable;

        if ((request.Advantage || request.Disadvantage) &&
            !(request.DiceCount == 1 && request.DieSize == 20))
        {
            await command.RespondAsync(LI(command, "roll.advantage_d20"), ephemeral: true);
            return;
        }

        RollResult result = ExecuteRoll(request);

        var embed = new EmbedBuilder()
            .WithTitle(LI(command, "roll.title"))
            .WithDescription(LI(command, "roll.requested_by", command.User.Mention))
            .AddField(LI(command, "roll.roll"), result.RollLabel, true)
            .AddField(LI(command, "roll.mode"), LI(command, request.Advantage ? "roll.advantage" : request.Disadvantage ? "roll.disadvantage" : "roll.normal"), true)
            .AddField(LI(command, "roll.total"), result.Total, true)
            .WithColor(Color.DarkGreen)
            .WithCurrentTimestamp();

        if (result.AdvantageRolls.Count > 0)
        {
            embed.AddField(
                LI(command, "roll.dice"),
                LI(command, "roll.kept", result.AdvantageRolls[0], result.AdvantageRolls[1], result.BaseRollTotal),
                false);
        }
        else
        {
            embed.AddField(LI(command, "roll.dice"), string.Join(", ", result.IndividualRolls), false);
        }

        if (request.Modifier != 0)
        {
            string modText = request.Modifier > 0
                ? $"+{request.Modifier}"
                : request.Modifier.ToString(CultureInfo.InvariantCulture);

            embed.AddField(LI(command, "roll.modifier"), modText, true);
        }

        if (request.ExhaustionLevel > 0)
        {
            embed.AddField(
                LI(command, "roll.exhaustion"),
                LI(command, "roll.exhaustion_value", request.ExhaustionLevel, request.ExhaustionLevel * 2),
                true);
        }

        if (request.Resistant)
            embed.AddField(LI(command, "roll.resistance"), $"{result.PreDamageAdjustmentTotal} → **{result.Total}**", true);
        else if (request.Vulnerable)
            embed.AddField(LI(command, "roll.vulnerability"), $"{result.PreDamageAdjustmentTotal} → **{result.Total}**", true);

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

        int rolledTotal;

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
            rolledTotal = kept;
        }
        else
        {
            int total = 0;

            for (int i = 0; i < request.DiceCount; i++)
            {
                int roll = _random.Next(1, request.DieSize + 1);
                result.IndividualRolls.Add(roll);
                total += roll;
            }

            result.BaseRollTotal = total;
            rolledTotal = total;
        }

        int exhaustionPenalty = request.ExhaustionLevel * 2;
        int adjustedTotal = rolledTotal + request.Modifier - exhaustionPenalty;

        result.PreDamageAdjustmentTotal = adjustedTotal;

        if (request.Resistant)
            adjustedTotal = (int)Math.Floor(adjustedTotal / 2.0);
        else if (request.Vulnerable)
            adjustedTotal *= 2;

        result.Total = adjustedTotal;
        return result;
    }

    private MessageComponent BuildOwnerDeleteButton(ulong ownerUserId)
    {
        return new ComponentBuilder()
            .WithButton("✖", $"owner_delete:{ownerUserId}", ButtonStyle.Danger)
            .Build();
    }

    private async Task SendBotOwnerMessageAsync(SocketTextChannel channel, ulong ownerUserId, string? text = null, Embed? embed = null)
    {
        await channel.SendMessageAsync(
            text: text,
            embed: embed,
            components: BuildOwnerDeleteButton(ownerUserId));
    }

    private async Task HandleOwnerDeleteButtonAsync(SocketMessageComponent component)
    {
        string[] parts = component.Data.CustomId.Split(':', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length != 2 || !ulong.TryParse(parts[1], out ulong ownerUserId))
        {
            await component.RespondAsync(LC(component, "buttons.delete_invalid"), ephemeral: true);
            return;
        }

        if (component.User.Id != ownerUserId)
        {
            await component.RespondAsync(LC(component, "buttons.delete_unauthorized"), ephemeral: true);
            return;
        }

        await component.DeferAsync(ephemeral: true);
        await component.Message.DeleteAsync();
    }

    private async Task HandleStatsProfileButtonAsync(SocketMessageComponent component)
    {
        string[] parts = component.Data.CustomId.Split(':', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 4 ||
            !ulong.TryParse(parts[2], out ulong entityId) ||
            !ulong.TryParse(parts[3], out ulong ownerUserId))
        {
            await component.RespondAsync(LC(component, "buttons.profile_expired"), ephemeral: true);
            return;
        }

        if (component.User.Id != ownerUserId)
        {
            await component.RespondAsync(
                "These profile buttons belong to the person who opened this menu. Run `!ab UserStats` or `!ab ServerStats` to open your own.",
                ephemeral: true);
            return;
        }

        string action = parts[0];
        string scope = parts[1];

        if (action == "stats_close")
        {
            await component.Message.DeleteAsync();
            return;
        }

        if (scope == "user")
        {
            IUser? user = _client?.GetUser(entityId);
            string displayName = user?.GlobalName ?? user?.Username ?? $"User {entityId}";
            string avatarUrl = user?.GetAvatarUrl(ImageFormat.Auto, 256) ?? user?.GetDefaultAvatarUrl() ?? "";

            Embed embed = action == "stats_achievements"
                ? BuildUserAchievementsEmbed(entityId, displayName, GetOrCreateGuildSettings(((SocketTextChannel)component.Channel).Guild.Id))
                : BuildUserStatsEmbed(entityId, displayName, avatarUrl, GetOrCreateGuildSettings(((SocketTextChannel)component.Channel).Guild.Id));

            MessageComponent components = action == "stats_achievements"
                ? BuildAchievementComponents("user", entityId, ownerUserId, GetOrCreateGuildSettings(((SocketTextChannel)component.Channel).Guild.Id))
                : BuildStatsProfileComponents("user", entityId, ownerUserId, GetOrCreateGuildSettings(((SocketTextChannel)component.Channel).Guild.Id));

            await component.UpdateAsync(msg =>
            {
                msg.Embed = Optional.Create(embed);
                msg.Components = Optional.Create(components);
            });
            return;
        }

        if (scope == "server")
        {
            SocketGuild? guild = _client?.GetGuild(entityId);
            if (guild == null)
            {
                await component.RespondAsync(LC(component, "buttons.server_missing"), ephemeral: true);
                return;
            }

            Embed embed = action == "stats_achievements"
                ? BuildServerAchievementsEmbed(guild, GetOrCreateGuildSettings(guild.Id))
                : BuildServerStatsEmbed(guild, GetOrCreateGuildSettings(guild.Id));

            MessageComponent components = action == "stats_achievements"
                ? BuildAchievementComponents("server", entityId, ownerUserId, GetOrCreateGuildSettings(guild.Id))
                : BuildStatsProfileComponents("server", entityId, ownerUserId, GetOrCreateGuildSettings(guild.Id));

            await component.UpdateAsync(msg =>
            {
                msg.Embed = Optional.Create(embed);
                msg.Components = Optional.Create(components);
            });
            return;
        }

        await component.RespondAsync(LC(component, "buttons.profile_expired"), ephemeral: true);
    }

    private async Task ButtonExecuted(SocketMessageComponent component)
    {
        string customId = component.Data.CustomId;

        if (customId.StartsWith("serversetup_", StringComparison.Ordinal))
        {
            if (component.Channel is not SocketTextChannel setupChannel || component.User is not SocketGuildUser setupUser || !setupUser.GuildPermissions.ManageGuild)
            {
                await component.RespondAsync(LC(component, "errors.manage_server_change"), ephemeral: true);
                return;
            }
            string[] setupParts = customId.Split(':');
            if (setupParts.Length != 2 || !ulong.TryParse(setupParts[1], out ulong setupGuildId) || setupGuildId != setupChannel.Guild.Id)
            {
                await component.RespondAsync(LC(component, "errors.setup_expired"), ephemeral: true);
                return;
            }
            GuildSettings guildSettings = GetOrCreateGuildSettings(setupGuildId);
            if (customId.StartsWith("serversetup_commands:", StringComparison.Ordinal))
            {
                await component.UpdateAsync(msg => { msg.Embed = Optional.Create(BuildCommandSettingsEmbed(guildSettings)); msg.Components = Optional.Create(BuildCommandSettingsComponents(guildSettings, setupGuildId)); });
                return;
            }
            if (customId.StartsWith("serversetup_language:", StringComparison.Ordinal))
            {
                await component.UpdateAsync(msg => { msg.Embed = Optional.Create(BuildLanguageSettingsEmbed(guildSettings)); msg.Components = Optional.Create(BuildLanguageSettingsComponents(guildSettings, setupGuildId)); });
                return;
            }
            if (customId.StartsWith("serversetup_enableall:", StringComparison.Ordinal))
            {
                guildSettings.DisabledUserCommands.Clear();
                SaveGuildSettings();
                await component.UpdateAsync(msg => { msg.Embed = Optional.Create(BuildCommandSettingsEmbed(guildSettings)); msg.Components = Optional.Create(BuildCommandSettingsComponents(guildSettings, setupGuildId)); });
                return;
            }
            if (customId.StartsWith("serversetup_back:", StringComparison.Ordinal) || customId.StartsWith("serversetup_refresh:", StringComparison.Ordinal))
            {
                await component.UpdateAsync(msg => { msg.Embed = Optional.Create(BuildServerSetupEmbed(setupChannel.Guild, guildSettings)); msg.Components = Optional.Create(BuildServerSetupComponents(setupGuildId)); });
                return;
            }
        }

        if (customId.StartsWith("usersettings_reset:", StringComparison.Ordinal))
        {
            string[] parts = customId.Split(':');
            if (parts.Length != 3 || !ulong.TryParse(parts[1], out ulong ownerUserId) || !ulong.TryParse(parts[2], out ulong guildId))
            {
                await component.RespondAsync(LC(component, "errors.settings_button_expired"), ephemeral: true);
                return;
            }
            if (component.User.Id != ownerUserId)
            {
                await component.RespondAsync(LC(component, "errors.settings_owner"), ephemeral: true);
                return;
            }
            UserIgnoreSettings settings = GetOrCreateUserIgnoreSettings(ownerUserId);
            settings.IgnoreAllServers = false;
            settings.IgnoredGuildIds.Clear();
            settings.PreferredProviders.Clear();
            settings.ReplyNotificationsEnabled = true;
            settings.LanguageCode = LocalizationManager.DefaultLanguage;
            SaveUserIgnoreSettings();
            await component.UpdateAsync(msg =>
            {
                msg.Embed = Optional.Create(BuildUserSettingsEmbed(settings, guildId));
                msg.Components = Optional.Create(BuildUserSettingsComponents(settings, ownerUserId, guildId, "twitter"));
            });
            return;
        }

        if (customId.StartsWith("owner_delete:", StringComparison.Ordinal))
        {
            await HandleOwnerDeleteButtonAsync(component);
            return;
        }

        if (customId.StartsWith("page:", StringComparison.Ordinal))
        {
            await HandlePaginatorButtonAsync(component);
            return;
        }

        if (customId.StartsWith("stats_achievements:", StringComparison.Ordinal) ||
            customId.StartsWith("stats_profile:", StringComparison.Ordinal) ||
            customId.StartsWith("stats_close:", StringComparison.Ordinal))
        {
            await HandleStatsProfileButtonAsync(component);
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

        if (state.GuildId == 0 && component.Channel is SocketGuildChannel relayChannel)
        {
            state.GuildId = relayChannel.Guild.Id;
            _relayStates[component.Message.Id] = state;
            SaveRelayStates();
        }

        GuildSettings stateSettings = GetOrCreateGuildSettings(state.GuildId);

        if (!stateSettings.ButtonsEnabled)
        {
            await component.RespondAsync(
                "Relay buttons are disabled in this server right now.",
                ephemeral: true);
            return;
        }

        TimeSpan buttonCooldown = TimeSpan.FromSeconds(Math.Clamp(stateSettings.ButtonCooldownSeconds, 1, 30));
        (ulong MessageId, ulong UserId) cooldownKey = (component.Message.Id, component.User.Id);

        double? cooldownRemaining = null;
        lock (_cooldownsLock)
        {
            if (_cooldowns.TryGetValue(cooldownKey, out DateTime lastUsed))
            {
                TimeSpan elapsed = DateTime.UtcNow - lastUsed;
                if (elapsed < buttonCooldown)
                    cooldownRemaining = (buttonCooldown - elapsed).TotalSeconds;
            }

            if (!cooldownRemaining.HasValue)
            {
                _cooldowns[cooldownKey] = DateTime.UtcNow;
                CleanupOldCooldownsUnsafe();
            }
        }

        if (cooldownRemaining.HasValue)
        {
            await component.RespondAsync(
                $"Slow down a bit 😅 Try again in {cooldownRemaining.Value:F1}s.",
                ephemeral: true);
            return;
        }

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

                _relayStates.TryRemove(component.Message.Id, out _);
                SaveRelayStates();

                await component.FollowupAsync(LC(component, "buttons.relay_deleted"), ephemeral: true);
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
                props.Components = Optional.Create(BuildButtonsForGuild(state.GuildId, state.Platforms, state.SilentMode));
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
                    LC(component, "buttons.failed"),
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
        string? headerText = null,
        ulong? ownerUserId = null)
    {
        if (lines.Count == 0)
        {
            var emptyEmbed = new EmbedBuilder()
                .WithTitle(title)
                .WithDescription(channel is SocketTextChannel tc ? L(GetOrCreateGuildSettings(tc.Guild.Id), "pagination.empty") : LU(GetOrCreateUserIgnoreSettings(ownerUserId), "pagination.empty"))
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
            .WithFooter(channel is SocketTextChannel tc2 ? L(GetOrCreateGuildSettings(tc2.Guild.Id), "pagination.page", page + 1, totalPages) : $"Page {page + 1}/{totalPages}")
            .Build();

        await channel.SendMessageAsync(
            embed: embed,
            components: BuildPaginatorComponents(paginatorType, page, totalPages, ownerUserId));
    }

    private MessageComponent BuildPaginatorComponents(string paginatorType, int page, int totalPages, ulong? ownerUserId = null)
    {
        bool hasPrevious = page > 0;
        bool hasNext = page < totalPages - 1;

        string ownerSuffix = ownerUserId.HasValue ? $":{ownerUserId.Value}" : "";

        var builder = new ComponentBuilder()
            .WithButton("◀", $"page:{paginatorType}:{page - 1}{ownerSuffix}", ButtonStyle.Secondary, disabled: !hasPrevious)
            .WithButton("▶", $"page:{paginatorType}:{page + 1}{ownerSuffix}", ButtonStyle.Primary, disabled: !hasNext);

        if (ownerUserId.HasValue)
            builder.WithButton("✖", $"owner_delete:{ownerUserId.Value}", ButtonStyle.Danger);

        return builder.Build();
    }

    private async Task HandlePaginatorButtonAsync(SocketMessageComponent component)
    {
        try
        {
            string[] parts = component.Data.CustomId.Split(':', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 3 && parts.Length != 4)
            {
                await component.RespondAsync(LC(component, "pagination.invalid_button"), ephemeral: true);
                return;
            }

            string paginatorType = parts[1];
            ulong? ownerUserId = null;

            if (parts.Length == 4 && ulong.TryParse(parts[3], out ulong parsedOwnerUserId))
                ownerUserId = parsedOwnerUserId;

            if (!int.TryParse(parts[2], out int page))
            {
                await component.RespondAsync(LC(component, "pagination.invalid_page"), ephemeral: true);
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
                        await component.RespondAsync(LC(component, "pagination.client_not_ready"), ephemeral: true);
                        return;
                    }

                    lines = GetVisibleGuilds()
                        .OrderByDescending(g => g.MemberCount)
                        .ThenBy(g => g.Name)
                        .Select((g, i) => $"**{i + 1}.** {g.Name}\nID: `{g.Id}` | Members: **{g.MemberCount}**")
                        .ToList();
                    title = "ApolloBot Connected Servers";
                    headerText = $"**Public Servers:** {GetVisibleServerCount()}\n**Public Users:** {GetVisibleUserCount()}\n**Excluded Servers:** {_statsExcludedGuildIds.Count}";
                    color = Color.Gold;
                    break;

                case "abhelp":
                    bool isAdmin = component.User is SocketGuildUser guildUser && guildUser.GuildPermissions.ManageGuild;
                    lines = BuildApolloBotHelpLines(isAdmin, GetOrCreateGuildSettings(((SocketTextChannel)component.Channel).Guild.Id));
                    GuildSettings pageSettings = GetOrCreateGuildSettings(((SocketTextChannel)component.Channel).Guild.Id);
                    title = L(pageSettings, "help.title");
                    headerText = L(pageSettings, "help.header");
                    color = Color.Teal;
                    break;

                case "bothelp":
                    lines = BuildBotOwnerHelpLines();
                    title = "👑 Bot Owner Commands";
                    headerText = "Owner-only controls and maintenance commands.";
                    color = Color.DarkPurple;
                    break;

                default:
                    await component.RespondAsync(LC(component, "pagination.unknown"), ephemeral: true);
                    return;
            }

            if (lines.Count == 0)
            {
                await component.RespondAsync(LC(component, "pagination.empty"), ephemeral: true);
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
                .WithFooter(LC(component, "pagination.page", page + 1, totalPages))
                .Build();

            await component.UpdateAsync(msg =>
            {
                msg.Embed = Optional.Create(embed);
                msg.Components = Optional.Create(BuildPaginatorComponents(paginatorType, page, totalPages, ownerUserId));
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to handle paginator button: {ex}");

            try
            {
                await component.RespondAsync(LC(component, "pagination.failed"), ephemeral: true);
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
            "`!bot servercount` – Show public and actual connected server counts",
            "`!bot exclude <serverId>` – Exclude a server from public stats",
            "`!bot include <serverId>` – Re-include a server in public stats",
            "`!bot exclusions` – List servers excluded from public stats",
            "`!bot chat <ServerID> <ChannelID> <Message>` – Make ApolloBot speak in a connected server/channel",
            "`!bot reply <ServerID> <ChannelID> <MessageID> <Message>` – Make ApolloBot reply to a specific message",
            "`!bot delete <MessageID>` – Delete an ApolloBot message in the current channel",
            "`!bot delete <ServerID> <ChannelID> <MessageID>` – Delete an ApolloBot message remotely",
            "`!bot leave <ServerID>` – Leave and blocklist a server",
            "`!bot unblock <ServerID>` – Remove a server from the blocklist",
            "`!bot blocklist` – Show blocked server IDs",
            "`!bot join [voiceChannelId]` – Silently join your current VC or a specific VC by ID",
            "`!bot leave` – Leave the current voice channel",
            "`!bot servers` – List public-counted servers with pagination",
            "`!bot topservers` – Show most-used servers by embed fixes",
            "`!bot topservers remove <serverId>` – Remove a server from usage analytics",
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

    private List<string> BuildApolloBotHelpLines(bool isAdmin, GuildSettings? settings = null)
    {
        settings ??= new GuildSettings { LanguageCode = LocalizationManager.DefaultLanguage };
        bool Enabled(string command) => settings.GuildId == 0 || !IsOptionalCommandDisabled(settings.GuildId, command);
        var lines = new List<string> { L(settings, "help.getting_started"), L(settings, "help.intro"), L(settings, "help.usersettings"), L(settings, "help.about"), L(settings, "help.support"), "", L(settings, "help.embeds_title"), L(settings, "help.providers"), L(settings, "help.info"), L(settings, "help.perms"), "", L(settings, "help.stats_title") };
        if (Enabled("userstats")) lines.Add(L(settings, "help.userstats"));
        if (Enabled("serverstats")) lines.Add(L(settings, "help.serverstats"));
        var animals = new List<string>(); if (Enabled("fox")) animals.Add("`/fox`"); if (Enabled("cat")) animals.Add("`/cat`"); if (Enabled("dog")) animals.Add("`/dog`");
        if (animals.Count > 0) lines.Add(L(settings, "help.animals", string.Join(" ", animals)));
        if (Enabled("roll")) lines.Add(L(settings, "help.roll")); if (Enabled("updates")) lines.Add(L(settings, "help.updates")); if (Enabled("vote")) lines.Add(L(settings, "help.vote")); lines.Add(L(settings, "help.ping"));
        if (isAdmin) { lines.Add(""); lines.Add(L(settings, "help.server_settings")); lines.Add(L(settings, "help.setup")); lines.Add(L(settings, "help.embedfix")); lines.Add(L(settings, "help.whitelist")); lines.Add(L(settings, "help.silent")); lines.Add(L(settings, "help.togglebuttons")); lines.Add(L(settings, "help.cooldown")); lines.Add(L(settings, "help.reset")); }
        lines.Add(""); lines.Add(L(settings, "help.dm_title")); lines.Add(L(settings, "help.dm_commands")); return lines;
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
            existing.PlatformUsage ??= new Dictionary<string, long>();
            return;
        }

        _guildUsageStats[guild.Id] = new GuildUsageStats
        {
            GuildId = guild.Id,
            ServerName = guild.Name,
            LastKnownMemberCount = guild.MemberCount,
            FirstSeenAtUtc = DateTime.UtcNow,
            LastUpdatedAtUtc = DateTime.UtcNow,
            PlatformUsage = new Dictionary<string, long>()
        };
    }

    private void RecordUserEmbedFix(IUser user, ulong guildId, IEnumerable<string>? platforms = null)
    {
        UserUsageStats stats = _userUsageStats.GetOrAdd(user.Id, _ => new UserUsageStats
        {
            UserId = user.Id,
            Username = user.Username,
            FirstUsedAtUtc = DateTime.UtcNow
        });

        stats.Username = user.Username;
        stats.EmbedFixCount++;
        stats.LastUsedAtUtc = DateTime.UtcNow;
        stats.GuildIds ??= new HashSet<ulong>();
        stats.GuildIds.Add(guildId);
        stats.PlatformUsage ??= new Dictionary<string, long>();

        if (platforms != null)
        {
            foreach (string platform in platforms.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                string key = platform.ToLowerInvariant();
                stats.PlatformUsage.TryGetValue(key, out long current);
                stats.PlatformUsage[key] = current + 1;
            }
        }

        SaveUserUsageStats();
    }

    private void RecordGuildEmbedFix(SocketGuild guild, IEnumerable<string>? platforms = null)
    {
        EnsureGuildUsageStatsEntry(guild);

        GuildUsageStats stats = _guildUsageStats[guild.Id];
        stats.EmbedFixCount++;
        stats.ServerName = guild.Name;
        stats.LastKnownMemberCount = guild.MemberCount;
        stats.LastUsedAtUtc = DateTime.UtcNow;
        stats.LastUpdatedAtUtc = DateTime.UtcNow;

        if (platforms != null)
        {
            foreach (string platform in platforms.Where(p => !string.IsNullOrWhiteSpace(p)).Distinct())
            {
                if (!stats.PlatformUsage.ContainsKey(platform))
                    stats.PlatformUsage[platform] = 0;

                stats.PlatformUsage[platform]++;
            }
        }

        SaveGuildUsageStats();
    }

    private async Task RemoveServerFromTopServersAsync(SocketTextChannel channel, ulong ownerUserId, string[] parts)
    {
        if (parts.Length < 4 || !ulong.TryParse(parts[3], out ulong guildId))
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Usage: `!bot topservers remove <serverId>`");
            return;
        }

        bool removedUsage = _guildUsageStats.TryRemove(guildId, out _);
        bool removedActivity = _guildActivity.TryRemove(guildId, out _);

        SaveGuildUsageStats();
        SaveGuildActivityState();

        if (!removedUsage && !removedActivity)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "That server ID was not found in tracked analytics.");
            return;
        }

        string extra = removedActivity ? " and guild activity history" : "";
        await SendBotOwnerMessageAsync(channel, ownerUserId, $"✅ Removed server `{guildId}` from tracked analytics{extra}.");
    }

    private async Task SendTopServersByUsageAsync(SocketTextChannel channel, ulong ownerUserId)
    {
        var top = _guildUsageStats.Values
            .OrderByDescending(x => x.EmbedFixCount)
            .ThenByDescending(x => x.LastUsedAtUtc)
            .Take(10)
            .ToList();

        if (top.Count == 0)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "No per-server embed usage has been recorded yet.");
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

        await SendBotOwnerMessageAsync(channel, ownerUserId, embed: embed);
    }

    private async Task SendSingleServerStatsAsync(SocketTextChannel channel, ulong ownerUserId, string[] parts)
    {
        if (parts.Length < 3 || !ulong.TryParse(parts[2], out ulong guildId))
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "Usage: `!bot serverstats <serverId>`");
            return;
        }

        _guildUsageStats.TryGetValue(guildId, out GuildUsageStats? usageStats);
        _guildActivity.TryGetValue(guildId, out GuildActivityState? activity);

        SocketGuild? liveGuild = _client?.GetGuild(guildId);

        if (usageStats == null && activity == null && liveGuild == null)
        {
            await SendBotOwnerMessageAsync(channel, ownerUserId, "I don't have any tracked data for that server ID.");
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

        await SendBotOwnerMessageAsync(channel, ownerUserId, embed: embed);
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

        int liveServerCount = GetVisibleServerCount();
        int liveUserCount = GetVisibleUserCount();

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
        if (Interlocked.Exchange(ref _guildUsageSaveScheduled, 1) != 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(500);
                string json = JsonSerializer.Serialize(_guildUsageStats.ToDictionary(x => x.Key, x => x.Value), new JsonSerializerOptions { WriteIndented = true });
                lock (_guildUsageFileLock)
                    File.WriteAllText(GuildUsageStatsFilePath, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to save guild usage stats: {ex}");
            }
            finally
            {
                Interlocked.Exchange(ref _guildUsageSaveScheduled, 0);
            }
        });
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
                {
                    stats.PlatformUsage ??= new Dictionary<string, long>();
                    _guildUsageStats[guildId] = stats;
                }
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

    private void SaveUserUsageStats()
    {
        try
        {
            string json = JsonSerializer.Serialize(_userUsageStats, new JsonSerializerOptions
            {
                WriteIndented = true
            });
            File.WriteAllText(UserUsageStatsFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save user usage stats: {ex}");
        }
    }

    private void LoadUserUsageStats()
    {
        try
        {
            if (!File.Exists(UserUsageStatsFilePath))
                return;

            string json = File.ReadAllText(UserUsageStatsFilePath);
            Dictionary<ulong, UserUsageStats>? loaded = JsonSerializer.Deserialize<Dictionary<ulong, UserUsageStats>>(json);

            _userUsageStats.Clear();
            if (loaded == null)
                return;

            foreach ((ulong userId, UserUsageStats stats) in loaded)
            {
                stats.PlatformUsage ??= new Dictionary<string, long>();
                stats.GuildIds ??= new HashSet<ulong>();
                _userUsageStats[userId] = stats;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load user usage stats: {ex}");
        }
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
            SilentMode = false,
            ButtonsEnabled = true,
            ButtonCooldownSeconds = 3,
            WhitelistedChannelIds = new List<ulong>(),
            DisabledUserCommands = new List<string>()
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

    private string BuildSlashPermissionFailureMessage(SocketSlashCommand command, SocketTextChannel channel)
    {
        List<string> missing = GetLikelyMissingPermissions(channel);
        string missingText = missing.Count == 0
            ? "I hit Discord's **Missing Permissions (50013)** error in this channel, but I couldn't pinpoint the exact missing permission from the live snapshot. This is often caused by a channel deny override, webhook restriction, or another permission mismatch."
            : $"I couldn't complete `/{command.Data.Name}` in this channel because I'm missing: **{string.Join(", ", missing)}**.";

        string guidance = command.Data.Name == "fix"
            ? "For `/fix`, the most important ones are usually **View Channel**, **Send Messages**, **Embed Links**, **Read Message History**, and **Manage Webhooks**."
            : $"Please make sure I have the permissions needed for `/{command.Data.Name}` in this channel.";

        return missingText + "\n" +
               guidance + "\n" +
               "An admin can also run `!ab perms` here to see my permission report.";
    }


    private MessageComponent BuildButtonsForGuild(ulong guildId, List<string> platforms, bool silentMode = false)
    {
        GuildSettings settings = GetOrCreateGuildSettings(guildId);
        return BuildButtons(platforms, silentMode || !settings.ButtonsEnabled);
    }

    private MessageComponent BuildButtons(List<string> platforms, bool silentMode = false)
    {
        if (silentMode)
            return new ComponentBuilder().Build();

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
            "twitter" => "Fix Embed",
            "reddit" => "Fix Embed",
            "tiktok" => "Fix Embed",
            "instagram" => "Fix Embed",
            "kick" => "Fix Embed",
            "bluesky" => "Fix Embed",
            "threads" => "Fix Embed (Exp)",
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
            "kick" => "Kick",
            "bluesky" => "Bluesky",
            "threads" => "Threads",
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

        if (ContainsKickLink(text))
            platforms.Add("kick");

        if (ContainsBlueskyLink(text))
            platforms.Add("bluesky");

        if (ContainsThreadsLink(text))
            platforms.Add("threads");

        return platforms;
    }

    private Dictionary<string, int> CreateDefaultProviderIndexes(List<string> platforms, ulong userId)
    {
        var indexes = new Dictionary<string, int>();
        UserIgnoreSettings userSettings = GetOrCreateUserIgnoreSettings(userId);

        foreach (string platform in platforms)
        {
            int index = 0;
            List<string> providers = GetProvidersForPlatform(platform, userId);

            if (userSettings.PreferredProviders.TryGetValue(platform, out string? preferredProvider))
            {
                int preferredIndex = providers.FindIndex(x =>
                    string.Equals(x, preferredProvider, StringComparison.OrdinalIgnoreCase));
                if (preferredIndex >= 0)
                    index = preferredIndex;
            }

            indexes[platform] = index;
        }

        return indexes;
    }

    private string ApplyAllReplacements(string text, Dictionary<string, int> providerIndexes, ulong originalAuthorId)
    {
        string result = CleanAllUrls(text);

        if (providerIndexes.ContainsKey("twitter"))
            result = ReplaceTwitterLinks(result, providerIndexes["twitter"], originalAuthorId);

        if (providerIndexes.ContainsKey("reddit"))
            result = ReplaceRedditLinks(result, providerIndexes["reddit"], originalAuthorId);

        if (providerIndexes.ContainsKey("tiktok"))
            result = ReplaceTikTokLinks(result, providerIndexes["tiktok"], originalAuthorId);

        if (providerIndexes.ContainsKey("instagram"))
            result = ReplaceInstagramLinks(result, providerIndexes["instagram"], originalAuthorId);

        if (providerIndexes.ContainsKey("kick"))
            result = ReplaceKickLinks(result, providerIndexes["kick"], originalAuthorId);

        if (providerIndexes.ContainsKey("bluesky"))
            result = ReplaceBlueskyLinks(result, providerIndexes["bluesky"], originalAuthorId);

        if (providerIndexes.ContainsKey("threads"))
            result = ReplaceThreadsLinks(result, providerIndexes["threads"], originalAuthorId);

        return result;
    }

    private string CleanAllUrls(string text)
    {
        return Regex.Replace(text, @"https?://[^\s<>()]+", match => CleanUrl(match.Value));
    }

    private string CleanUrl(string url)
    {
        try
        {
            string trailing = "";
            while (url.Length > 0 && ".,!?;:)".Contains(url[^1]))
            {
                trailing = url[^1] + trailing;
                url = url[..^1];
            }

            var builder = new UriBuilder(url);
            builder.Query = "";
            builder.Fragment = "";
            return builder.Uri.GetLeftPart(UriPartial.Path) + trailing;
        }
        catch
        {
            return url;
        }
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

    private bool ContainsKickLink(string text)
    {
        return Regex.IsMatch(
            text,
            @"https?://(www\.)?kick\.com(/|$)",
            RegexOptions.IgnoreCase);
    }

    private bool ContainsBlueskyLink(string text)
    {
        return Regex.IsMatch(
            text,
            @"https?://(www\.)?bsky\.app(/|$)",
            RegexOptions.IgnoreCase);
    }

    private bool ContainsThreadsLink(string text)
    {
        return Regex.IsMatch(
            text,
            @"https?://(www\.)?threads\.com/@[^\s/]+/post/[A-Za-z0-9_-]+",
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

    private string ReplaceKickLinks(string text, int providerIndex, ulong originalAuthorId)
    {
        List<string> kickProviders = GetProvidersForPlatform("kick", originalAuthorId);

        if (kickProviders.Count == 0)
            return text;

        if (providerIndex < 0 || providerIndex >= kickProviders.Count)
            providerIndex = 0;

        string replacementDomain = kickProviders[providerIndex];

        text = Regex.Replace(
            text,
            @"https?://(www\.)?kick\.com",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        return text;
    }

    private string ReplaceBlueskyLinks(string text, int providerIndex, ulong originalAuthorId)
    {
        List<string> blueskyProviders = GetProvidersForPlatform("bluesky", originalAuthorId);

        if (blueskyProviders.Count == 0)
            return text;

        if (providerIndex < 0 || providerIndex >= blueskyProviders.Count)
            providerIndex = 0;

        string replacementDomain = blueskyProviders[providerIndex];

        text = Regex.Replace(
            text,
            @"https?://(www\.)?bsky\.app",
            $"https://{replacementDomain}",
            RegexOptions.IgnoreCase);

        return text;
    }

    private string ReplaceThreadsLinks(string text, int providerIndex, ulong originalAuthorId)
    {
        List<string> threadProviders = GetProvidersForPlatform("threads", originalAuthorId);

        if (threadProviders.Count == 0)
            return text;

        if (providerIndex < 0 || providerIndex >= threadProviders.Count)
            providerIndex = 0;

        string replacementDomain = threadProviders[providerIndex];

        text = Regex.Replace(
            text,
            @"https?://(www\.)?threads\.com",
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
        if (Interlocked.Exchange(ref _relayStateSaveScheduled, 1) != 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(500);
                string json = JsonSerializer.Serialize(_relayStates.ToDictionary(x => x.Key, x => x.Value), new JsonSerializerOptions { WriteIndented = true });
                lock (_relayStateFileLock)
                    File.WriteAllText(StateFilePath, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to save relay states: {ex}");
            }
            finally
            {
                Interlocked.Exchange(ref _relayStateSaveScheduled, 0);
            }
        });
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
            {
                if (state.GuildId == 0)
                    state.GuildId = 0;

                _relayStates[messageId] = state;
            }

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

            UserIgnoreSettings userSettings = GetOrCreateUserIgnoreSettings(relayState.OriginalAuthorId);
            if (!userSettings.ReplyNotificationsEnabled)
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
        int serverCount = GetVisibleServerCount();
        int totalUsers = GetVisibleUserCount();

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

    private bool IsGuildBlocked(ulong guildId)
    {
        lock (_blockedGuildsLock)
            return _blockedGuildIds.Contains(guildId);
    }

    private void AddBlockedGuild(ulong guildId)
    {
        lock (_blockedGuildsLock)
            _blockedGuildIds.Add(guildId);

        SaveBlockedGuilds();
    }

    private bool RemoveBlockedGuild(ulong guildId)
    {
        bool removed;
        lock (_blockedGuildsLock)
            removed = _blockedGuildIds.Remove(guildId);

        if (removed)
            SaveBlockedGuilds();

        return removed;
    }

    private void SaveBlockedGuilds()
    {
        try
        {
            List<ulong> blocked;
            lock (_blockedGuildsLock)
                blocked = _blockedGuildIds.OrderBy(id => id).ToList();

            string json = JsonSerializer.Serialize(blocked, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(BlockedGuildsFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save blocked guilds: {ex}");
        }
    }

    private void LoadBlockedGuilds()
    {
        try
        {
            if (!File.Exists(BlockedGuildsFilePath))
            {
                SaveBlockedGuilds();
                return;
            }

            string json = File.ReadAllText(BlockedGuildsFilePath);
            List<ulong>? loaded = JsonSerializer.Deserialize<List<ulong>>(json);
            if (loaded == null)
                return;

            lock (_blockedGuildsLock)
            {
                _blockedGuildIds.Clear();
                foreach (ulong guildId in loaded)
                    _blockedGuildIds.Add(guildId);
            }

            Console.WriteLine($"Loaded {loaded.Count} blocked guild(s).");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load blocked guilds: {ex}");
        }
    }

    private void SaveStatsExcludedGuilds()
    {
        try
        {
            string json = JsonSerializer.Serialize(_statsExcludedGuildIds.OrderBy(id => id).ToList(), new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(StatsExcludedGuildsFilePath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save stats excluded guilds: {ex}");
        }
    }

    private void LoadStatsExcludedGuilds()
    {
        try
        {
            if (!File.Exists(StatsExcludedGuildsFilePath))
            {
                SaveStatsExcludedGuilds();
                return;
            }

            string json = File.ReadAllText(StatsExcludedGuildsFilePath);
            List<ulong>? loaded = JsonSerializer.Deserialize<List<ulong>>(json);

            if (loaded == null)
                return;

            _statsExcludedGuildIds.Clear();

            foreach (ulong guildId in loaded)
                _statsExcludedGuildIds.Add(guildId);

        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load stats excluded guilds: {ex}");
        }
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
                    "kkinstagram.com"
                }
            },
            {
                "kick",
                new List<string>
                {
                    "clkick.com"
                }
            },
            {
                "bluesky",
                new List<string>
                {
                    "fxbsky.app",
                    "bskye.app",
                    "bskx.app"
                }
            },
            {
                "threads",
                new List<string>
                {
                    "fixthreads.seria.moe"
                }
            }
        };
    }


    private static string SanitizeProviderDomain(string domain)
    {
        domain = (domain ?? string.Empty).Trim().ToLowerInvariant();

        if (domain.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            domain = domain["http://".Length..];

        if (domain.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            domain = domain["https://".Length..];

        domain = domain.Trim();
        domain = domain.Trim('/', ',', '.', ';', ':', ')', ']', '}', '>', '\'', '"');

        int slashIndex = domain.IndexOf('/');
        if (slashIndex >= 0)
            domain = domain[..slashIndex];

        return domain;
    }

    private void NormalizeProviders()
    {
        foreach (string platform in _providers.Keys.ToList())
        {
            _providers[platform] = _providers[platform]
                .Select(SanitizeProviderDomain)
                .Where(domain => !string.IsNullOrWhiteSpace(domain))
                .Where(domain => !string.Equals(domain, "eeinstagram.com", StringComparison.OrdinalIgnoreCase))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
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
            NormalizeProviders();

            foreach ((string key, List<string> defaults) in CreateDefaultProviders())
            {
                if (!_providers.ContainsKey(key))
                    _providers[key] = new List<string>(defaults);
            }

            NormalizeProviders();
            SaveProviders();
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
            {
                settings.WhitelistedChannelIds ??= new List<ulong>();
                settings.DisabledUserCommands ??= new List<string>();
                settings.DisabledUserCommands = settings.DisabledUserCommands
                    .Where(x => OptionalUserCommands.Contains(x, StringComparer.OrdinalIgnoreCase))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                if (settings.ButtonCooldownSeconds <= 0)
                {
                    settings.ButtonCooldownSeconds = 3;
                    settings.ButtonsEnabled = true;
                }

                _guildSettings[guildId] = settings;
            }

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
            {
                settings.IgnoredGuildIds ??= new List<ulong>();
                settings.PreferredProviders ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                _userIgnoreSettings[userId] = settings;
            }

            Console.WriteLine($"Loaded {_userIgnoreSettings.Count} user ignore profile(s) from disk.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load user ignore settings: {ex}");
        }
    }

    private void CleanupOldCooldownsUnsafe()
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

class UserUsageStats
{
    public ulong UserId { get; set; }
    public string Username { get; set; } = "";
    public long EmbedFixCount { get; set; }
    public DateTime FirstUsedAtUtc { get; set; }
    public DateTime LastUsedAtUtc { get; set; }
    public HashSet<ulong> GuildIds { get; set; } = new();
    public Dictionary<string, long> PlatformUsage { get; set; } = new();
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
    public Dictionary<string, long> PlatformUsage { get; set; } = new();
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
    public bool SilentMode { get; set; }
    public ulong GuildId { get; set; }
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
    public int ExhaustionLevel { get; set; }
    public bool Resistant { get; set; }
    public bool Vulnerable { get; set; }
}

class RollResult
{
    public string RollLabel { get; set; } = "";
    public string ModeLabel { get; set; } = "Normal";
    public int BaseRollTotal { get; set; }
    public int PreDamageAdjustmentTotal { get; set; }
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
    public bool SilentMode { get; set; } = false;
    public bool ButtonsEnabled { get; set; } = true;
    public int ButtonCooldownSeconds { get; set; } = 3;
    public string LanguageCode { get; set; } = LocalizationManager.DefaultLanguage;
    public List<ulong> WhitelistedChannelIds { get; set; } = new();
    public List<string> DisabledUserCommands { get; set; } = new();
}

class UserIgnoreSettings
{
    public ulong UserId { get; set; }
    public bool IgnoreAllServers { get; set; } = false;
    public List<ulong> IgnoredGuildIds { get; set; } = new();
    public Dictionary<string, string> PreferredProviders { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public bool ReplyNotificationsEnabled { get; set; } = true;
    public string LanguageCode { get; set; } = LocalizationManager.DefaultLanguage;
}
