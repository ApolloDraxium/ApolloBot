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

class Program
{
    private DiscordSocketClient? _client;
    private readonly Random _random = new();
    private bool _slashCommandsRegistered = false;
    private long _embedsFixedCount = 0;
    private long _accumulatedUptimeSeconds = 0;
    private DateTime _lastStartedAtUtc = DateTime.UtcNow;
    private List<UptimeSession> _uptimeHistory = new();
    private DateTime _sessionStartUtc = DateTime.UtcNow;

    // Use your test server ID for fast slash command registration.
    // Set to 0 to register globally instead.
    private static readonly ulong TestGuildId = 1486178596765565009;

    private readonly Dictionary<string, List<string>> _providers = new()
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
                "rxddit.com",
                "vxreddit.com"
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
                "kkinstagram.com",
                "toinstagram.com"
            }
        }
    };

    private readonly Dictionary<ulong, RelayMessageState> _relayStates = new();
    private readonly Dictionary<(ulong MessageId, ulong UserId), DateTime> _cooldowns = new();
    private readonly Dictionary<ulong, GuildSettings> _guildSettings = new();
    private readonly Dictionary<ulong, UserIgnoreSettings> _userIgnoreSettings = new();

    private const string WebhookName = "Apollo Bot Relay";

    private static readonly string DataDirectory =
        Environment.GetEnvironmentVariable("APP_DATA_PATH") ?? "data";

    private static readonly string BotStatsStateFilePath =
        Path.Combine(DataDirectory, "bot_stats_state.json");

    private static readonly string StateFilePath =
        Path.Combine(DataDirectory, "relay_states.json");

    private static readonly string GuildSettingsFilePath =
        Path.Combine(DataDirectory, "guild_settings.json");

    private static readonly string UserIgnoreSettingsFilePath =
        Path.Combine(DataDirectory, "user_ignore_settings.json");

    private static readonly HashSet<ulong> BotOwnerIds = new()
    {
        127877921464385537,
        846147700700610600
    };

    private static readonly TimeSpan ButtonCooldown = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan CooldownRetention = TimeSpan.FromMinutes(10);
    private long _accumulatedUptimeSeconds = 0;
    private DateTime _lastStartedAtUtc = DateTime.UtcNow;
    private long _accumulatedUptimeSeconds = 0;
    private DateTime _lastStartedAtUtc = DateTime.UtcNow;
    private List<UptimeSession> _uptimeHistory = new();

    static Task Main(string[] args) => new Program().MainAsync();

    public async Task MainAsync()
    {
        Directory.CreateDirectory(DataDirectory);
        LoadRelayStates();
        LoadGuildSettings();
        LoadUserIgnoreSettings();
        LoadBotStatsState();

        _client = new DiscordSocketClient(new DiscordSocketConfig
        {
            GatewayIntents =
                GatewayIntents.Guilds |
                GatewayIntents.GuildMessages |
                GatewayIntents.MessageContent
        });

        _client.Log += Log;
        _client.Ready += OnReady;
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

                var now = DateTime.UtcNow;
                long sessionSeconds = (long)(now - _lastStartedAtUtc).TotalSeconds;

                if (sessionSeconds > 0)
                {
                    _accumulatedUptimeSeconds += sessionSeconds;
                    _lastStartedAtUtc = now;
                    SaveBotStatsState();
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
            await _client.SetActivityAsync(new Game("Running 24/7", ActivityType.Watching));

        if (!_slashCommandsRegistered)
        {
            await RegisterSlashCommandsAsync();
            _slashCommandsRegistered = true;
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

        if (content.StartsWith("!bot", StringComparison.OrdinalIgnoreCase))
        {
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
        string newContent = ApplyAllReplacements(originalContent, providerIndexes);

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

        if (!IsBotOwner(message.Author))
        {
            await textChannel.SendMessageAsync("🚫 You don't have access to that command.");
            return;
        }

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

        if (sub == "servercount")
        {
            int count = _client?.Guilds.Count ?? 0;
            await textChannel.SendMessageAsync($"🌐 Connected to **{count}** server(s).");
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
                .OrderBy(g => g.Name)
                .Select(g => $"• {g.Name} (`{g.Id}`) - {g.MemberCount} members")
                .ToList();

            if (guilds.Count == 0)
            {
                await textChannel.SendMessageAsync("I'm not in any servers.");
                return;
            }

            string output = string.Join("\n", guilds);

            if (output.Length > 1900)
            {
                await textChannel.SendMessageAsync(
                    $"I'm in **{guilds.Count}** servers. Too many to display in one message.");
            }
            else
            {
                await textChannel.SendMessageAsync($"**Connected servers ({guilds.Count}):**\n{output}");
            }

            return;
        }

        if (sub == "stats")
        {
            int serverCount = _client?.Guilds.Count ?? 0;
            int relayCount = _relayStates.Count;
            int guildSettingsCount = _guildSettings.Count;
            int ignoredUsersCount = _userIgnoreSettings.Count;
            TimeSpan uptime = DateTime.UtcNow - _startedAtUtc;

            var embed = new EmbedBuilder()
                .WithTitle("Bot Stats")
                .AddField("Servers", serverCount, true)
                .AddField("Relay States", relayCount, true)
                .AddField("Guild Settings", guildSettingsCount, true)
                .AddField("Ignored User Profiles", ignoredUsersCount, true)
                .AddField("Platforms Supported", _providers.Count, true)
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
        var embed = new EmbedBuilder()
            .WithTitle("Bot Owner Commands")
            .WithDescription("Owner-only controls.")
            .AddField("!bot help", "Show this help.", false)
            .AddField("!bot servercount", "Show how many servers the bot is in.", false)
            .AddField("!bot servers", "List connected servers.", false)
            .AddField("!bot stats", "Show bot stats and uptime.", false)
            .WithColor(Color.DarkPurple)
            .Build();

        await channel.SendMessageAsync(embed: embed);
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

        if (sub == "about")
        {
            await SendAbout(textChannel);
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

        var embed = new EmbedBuilder()
            .WithTitle("📦 ApolloBot Commands")
            .WithDescription("Embed fixing + utility commands.")
            .AddField("🌐 Public Commands",
                "`!ab help` – Show this menu\n" +
                "`!ab about` – What the bot does\n" +
                "`!ab ping` – Check if bot is alive\n" +
                "`!ab providers` – Show providers\n" +
                "`!ab perms` – Check permissions\n" +
                "`!ab status` – Server settings\n" +
                "`!ab ignore on` – Ignore your embeds in this server\n" +
                "`!ab ignore off` – Stop ignoring your embeds in this server\n" +
                "`!ab ignore all` – Toggle ignore in all servers\n" +
                "`/roll` – Roll dice", false);

        if (isAdmin)
        {
            embed.AddField("🛠 Admin Commands",
                "`!embedfix on` – Enable bot\n" +
                "`!embedfix off` – Disable bot\n" +
                "`!ab whitelist add #channel`\n" +
                "`!ab whitelist remove #channel`\n" +
                "`!ab whitelist list`\n" +
                "`!ab whitelist clear`", false);
        }

        embed
            .AddField("🎲 Roll Examples",
                "`/roll`\n" +
                "`/roll dice:1d20`\n" +
                "`/roll dice:1d20+6`\n" +
                "`/roll dice:1d20 mode:advantage`\n" +
                "`/roll dice:1d20+4 mode:disadvantage`\n" +
                "`/roll dice:1d6`\n" +
                "`/roll dice:2d6+3`", false)
            .WithColor(Color.Teal)
            .WithFooter("Tip: Use the buttons on relayed messages to fix broken embeds!");

        await channel.SendMessageAsync(embed: embed.Build());
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
                "• Slash roll command", false)
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

            string newContent = ApplyAllReplacements(state.OriginalContent, state.ProviderIndexes);

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
            return $"{span.Hours}h {span.Minutes}m";

        return $"{Math.Max(1, span.Minutes)}m";
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

    private string ApplyAllReplacements(string text, Dictionary<string, int> providerIndexes)
    {
        string result = text;

        if (providerIndexes.ContainsKey("twitter"))
            result = ReplaceTwitterLinks(result, providerIndexes["twitter"]);

        if (providerIndexes.ContainsKey("reddit"))
            result = ReplaceRedditLinks(result, providerIndexes["reddit"]);

        if (providerIndexes.ContainsKey("tiktok"))
            result = ReplaceTikTokLinks(result, providerIndexes["tiktok"]);

        if (providerIndexes.ContainsKey("instagram"))
            result = ReplaceInstagramLinks(result, providerIndexes["instagram"]);

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

    private string ReplaceTwitterLinks(string text, int providerIndex)
    {
        List<string> twitterProviders = _providers["twitter"];

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

    private string ReplaceRedditLinks(string text, int providerIndex)
    {
        List<string> redditProviders = _providers["reddit"];

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

    private string ReplaceTikTokLinks(string text, int providerIndex)
    {
        List<string> tikTokProviders = _providers["tiktok"];

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

    private string ReplaceInstagramLinks(string text, int providerIndex)
    {
        List<string> instagramProviders = _providers["instagram"];

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
        _embedsFixedCount++;
        SaveBotStatsState();
    }

    private void SaveBotStatsState()
    {
        try
        {
            var state = new BotStatsState
            {
                EmbedsFixedCount = _embedsFixedCount,
                AccumulatedUptimeSeconds = _accumulatedUptimeSeconds,
                LastStartedAtUtc = _lastStartedAtUtc,
                UptimeHistory = _uptimeHistory
            };

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

    private void LoadBotStatsState()
    {
        try
        {
            if (!File.Exists(BotStatsStateFilePath))
            {
                Console.WriteLine("No bot stats state file found. Starting fresh.");
                _lastStartedAtUtc = DateTime.UtcNow;
                return;
            }

            string json = File.ReadAllText(BotStatsStateFilePath);

            BotStatsState? loaded =
                JsonSerializer.Deserialize<BotStatsState>(json);

            if (loaded == null)
            {
                Console.WriteLine("Bot stats state file invalid. Starting fresh.");
                _lastStartedAtUtc = DateTime.UtcNow;
                return;
            }

            _embedsFixedCount = loaded.EmbedsFixedCount;
            _accumulatedUptimeSeconds = loaded.AccumulatedUptimeSeconds;
            _uptimeHistory = loaded.UptimeHistory ?? new List<UptimeSession>();

            var now = DateTime.UtcNow;

            if (loaded.LastStartedAtUtc != default)
            {
                long previousSessionSeconds = (long)(now - loaded.LastStartedAtUtc).TotalSeconds;

                if (previousSessionSeconds > 0)
                {
                    _uptimeHistory.Insert(0, new UptimeSession
                    {
                        StartedAtUtc = loaded.LastStartedAtUtc,
                        EndedAtUtc = now,
                        DurationSeconds = previousSessionSeconds
                    });
                }
            }

            _lastStartedAtUtc = now;

            Console.WriteLine($"Loaded embeds fixed count: {_embedsFixedCount}");
            Console.WriteLine($"Loaded accumulated uptime: {_accumulatedUptimeSeconds}s");
            Console.WriteLine($"Loaded uptime history entries: {_uptimeHistory.Count}");
            _accumulatedUptimeSeconds = loaded.AccumulatedUptimeSeconds;
            _uptimeHistory = loaded.UptimeHistory ?? new List<UptimeSession>();
            _sessionStartUtc = DateTime.UtcNow;
            _lastStartedAtUtc = DateTime.UtcNow;

            Console.WriteLine($"Loaded embeds fixed: {_embedsFixedCount}");
            Console.WriteLine($"Loaded accumulated uptime: {_accumulatedUptimeSeconds}s");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to load bot stats state: {ex}");
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
                while (true)
                {
                    await Task.Delay(TimeSpan.FromSeconds(60));

                    var now = DateTime.UtcNow;
                    long sessionSeconds = (long)(now - _lastStartedAtUtc).TotalSeconds;

                    // Add to accumulated uptime
                    _accumulatedUptimeSeconds += sessionSeconds;

                    // Update last tick
                    _lastStartedAtUtc = now;

                    SaveBotStatsState();
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
                    uptimeHistory = _uptimeHistory.Take(10)
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

        var now = DateTime.UtcNow;
        long currentSessionSeconds = (long)(now - _lastStartedAtUtc).TotalSeconds;
        long totalUptimeSeconds = _accumulatedUptimeSeconds + currentSessionSeconds;

        long longestSessionSeconds = _uptimeHistory.Count == 0
            ? currentSessionSeconds
            : Math.Max(_uptimeHistory.Max(x => x.DurationSeconds), currentSessionSeconds);

        var payload = new PublicStatsPayload
        {
            EmbedsFixed = _embedsFixedCount,
            ServerCount = serverCount,
            TotalUsers = totalUsers,
            Uptime = FormatDuration(TimeSpan.FromSeconds(totalUptimeSeconds)),
            PlatformCount = _providers.Count,
            TotalUptimeSeconds = totalUptimeSeconds,
            CurrentSessionSeconds = currentSessionSeconds,
            LongestSessionSeconds = longestSessionSeconds,
            RestartCount = _uptimeHistory.Count,
            UptimeHistory = _uptimeHistory.Take(10).ToList()
        };

        return JsonSerializer.Serialize(payload, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        });
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
    public DateTime LastStartedAtUtc { get; set; }
    public List<UptimeSession> UptimeHistory { get; set; } = new();
}

class UptimeSession
{
    public DateTime StartedAtUtc { get; set; }
    public DateTime EndedAtUtc { get; set; }
    public long DurationSeconds { get; set; }
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
    public string Uptime { get; set; } = "";
    public int PlatformCount { get; set; }

    public long TotalUptimeSeconds { get; set; }
    public long CurrentSessionSeconds { get; set; }
    public long LongestSessionSeconds { get; set; }
    public int RestartCount { get; set; }

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