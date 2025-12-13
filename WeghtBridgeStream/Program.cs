using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

// =======================
// Device ID Manager
// =======================

public static class DeviceIdManager
{
    private static readonly string DeviceIdFile;
    private static string? _cachedDeviceId;

    static DeviceIdManager()
    {
        var baseDirectory = AppContext.BaseDirectory;
        DeviceIdFile = Path.Combine(baseDirectory, "device.id.txt");
    }

    public static string GetOrCreateDeviceId()
    {
        if (_cachedDeviceId != null)
            return _cachedDeviceId;

        if (File.Exists(DeviceIdFile))
        {
            var existing = File.ReadAllText(DeviceIdFile).Trim();
            if (!string.IsNullOrWhiteSpace(existing))
            {
                _cachedDeviceId = existing;
                return existing;
            }
        }

        var newId = Guid.NewGuid().ToString();
        File.WriteAllText(DeviceIdFile, newId);
        _cachedDeviceId = newId;
        return newId;
    }

    public static string GetDeviceIdFilePath() => DeviceIdFile;
}

// =======================
// Options
// =======================

public sealed class AppOptions
{
    public string? ForceDeviceId { get; set; }
    public int HttpPort { get; set; } = 5001;
}

public sealed class SignalROptions
{
    public string ConnectionString { get; set; } = default!;
    public string HubName { get; set; } = "entry_weight_hub";
    public string MethodName { get; set; } = "ReceivefirstWeight";
}

public sealed class ScaleOptions
{
    public string Host { get; set; } = "10.8.197.21";
    public int Port { get; set; } = 4001;
    public int ReadTimeoutMs { get; set; } = 3000;
    public int ReconnectDelayMs { get; set; } = 1500;

    public decimal Divisor { get; set; } = 1m;
    public int MinDigits { get; set; } = 3;

    public decimal StableToleranceKg { get; set; } = 20m;
    public int StableSamples { get; set; } = 6;
    public int PublishIntervalMs { get; set; } = 150;

    public bool TestMode { get; set; } = false;
    public int TestTickMs { get; set; } = 150;
    public int TestMaxKg { get; set; } = 16000;
    public int TestRampStepKg { get; set; } = 250;
    public int TestNoiseMaxKg { get; set; } = 25;
}

// =======================
// Publisher (Azure SignalR)
// =======================

public sealed class WeightSignalRPublisher : IHostedService, IAsyncDisposable
{
    private readonly ILogger<WeightSignalRPublisher> _log;
    private readonly SignalROptions _opt;
    private readonly string _deviceId;
    private readonly ServiceManager _mgr;
    private ServiceHubContext? _hub;

    public WeightSignalRPublisher(
        ILogger<WeightSignalRPublisher> log,
        IOptions<SignalROptions> opt,
        IOptions<AppOptions> appOpt,
        ServiceManager mgr)
    {
        _log = log;
        _opt = opt.Value;
        _mgr = mgr;

        _deviceId = !string.IsNullOrWhiteSpace(appOpt.Value.ForceDeviceId)
            ? appOpt.Value.ForceDeviceId!
            : DeviceIdManager.GetOrCreateDeviceId();

        _log.LogInformation("Device ID: {deviceId}", _deviceId);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_opt.ConnectionString))
            throw new InvalidOperationException("SignalR:ConnectionString missing");

        _log.LogInformation("Initializing Azure SignalR for hub '{Hub}'...", _opt.HubName);
        _hub = await _mgr.CreateHubContextAsync(_opt.HubName, cancellationToken);
        _log.LogInformation("Azure SignalR hub context ready for '{Hub}'", _opt.HubName);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _log.LogInformation("Stopping WeightSignalRPublisher...");
        try
        {
            if (_hub is not null)
            {
                await _hub.DisposeAsync();
                _hub = null;
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Error while stopping WeightSignalRPublisher");
        }
    }

    public async Task PublishAsync(decimal weightKg, bool isStable, CancellationToken ct = default)
    {
        if (_hub is null) return;

        var payload = new
        {
            weight = decimal.Round(weightKg, 1, MidpointRounding.AwayFromZero),
            isStable,
            deviceId = _deviceId,
            tsUtc = DateTime.UtcNow
        };

        try
        {
            await _hub.Clients.User(_deviceId).SendAsync(_opt.MethodName, payload, ct);
            _log.LogInformation("Sent to user({user}) via SignalR: {payload}",
                _deviceId, System.Text.Json.JsonSerializer.Serialize(payload));
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "SignalR send failed");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_hub is not null)
        {
            await _hub.DisposeAsync();
            _hub = null;
        }
    }
}

// =======================
// Weight Parser (shared logic)
// =======================

internal static class WeightParser
{
    private static readonly Regex ValueWithUnit = new(
        @"(?<!\S)(?<num>[+-]?\d+(?:[.,]\d+)?)[ ]*(?<unit>kg|g|t|lb|oz)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex AnyNumber = new(
        @"[+-]?\d+(?:[.,]\d+)?",
        RegexOptions.Compiled);

    public static string StripControlChars(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s)
            if (!char.IsControl(ch)) sb.Append(ch);
        return sb.ToString();
    }

    public static bool TryParseWeight(string line, int minDigits, out decimal value)
    {
        // 1) Try "value + unit" (e.g. "  12345 kg")
        var unitMatches = ValueWithUnit.Matches(line);
        if (unitMatches.Count > 0)
        {
            var m = unitMatches[^1];
            var raw = m.Groups["num"].Value;
            if (TryParseDecimalFlexible(raw, out value))
                return true;
        }

        // 2) Fallback: longest numeric token with at least minDigits
        Match? best = null;
        foreach (Match m in AnyNumber.Matches(line))
        {
            int digits = CountDigits(m.Value);
            if (digits >= minDigits && (best is null || m.Value.Length > best.Value.Length))
                best = m;
        }

        if (best is not null && TryParseDecimalFlexible(best.Value, out value))
            return true;

        value = default;
        return false;
    }

    private static int CountDigits(string s)
    {
        int c = 0;
        foreach (var ch in s)
            if (char.IsDigit(ch)) c++;
        return c;
    }

    private static bool TryParseDecimalFlexible(string s, out decimal v)
    {
        if (decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out v)) return true;
        if (decimal.TryParse(s, NumberStyles.Float, CultureInfo.GetCultureInfo("fr-FR"), out v)) return true;
        return false;
    }
}

// =======================
// Weight Bridge (live or test) — BYTE READER STYLE
// =======================

public interface IWeightBridge
{
    event EventHandler<WeightReading>? Reading;
}

public sealed record WeightReading(decimal WeightKg, bool IsStable, DateTime At);

public sealed class WeightBridgeService : BackgroundService, IWeightBridge
{
    public event EventHandler<WeightReading>? Reading;

    private readonly ILogger<WeightBridgeService> _log;
    private readonly ScaleOptions _opt;
    private readonly WeightSignalRPublisher _publisher;

    public WeightBridgeService(
        ILogger<WeightBridgeService> log,
        IOptions<ScaleOptions> opt,
        WeightSignalRPublisher publisher)
    {
        _log = log;
        _opt = opt.Value;
        _publisher = publisher;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        decimal lastValue = 0m;
        int stableCounter = 0;
        bool haveLast = false;

        async Task PublishAsync(decimal w, bool stable)
        {
            Reading?.Invoke(this, new WeightReading(w, stable, DateTime.UtcNow));
            await _publisher.PublishAsync(w, stable, stoppingToken);
        }

        // ==========================
        // TEST MODE
        // ==========================
        if (_opt.TestMode)
        {
            var rnd = new Random();
            _log.LogWarning("WeightBridge TEST MODE enabled.");

            while (!stoppingToken.IsCancellationRequested)
            {
                // 10 UNSTABLE VALUES
                for (int i = 0; i < 10 && !stoppingToken.IsCancellationRequested; i++)
                {
                    int weight = rnd.Next(0, _opt.TestMaxKg);
                    bool stable = false;

                    await PublishAsync(weight, stable);
                    await Task.Delay(_opt.TestTickMs, stoppingToken);
                }

                if (stoppingToken.IsCancellationRequested) break;

                // 11th VALUE — STABLE
                {
                    int weight = rnd.Next(0, _opt.TestMaxKg);
                    bool stable = true;

                    await PublishAsync(weight, stable);
                    await Task.Delay(_opt.TestTickMs, stoppingToken);
                }

                // STOP AND WAIT FOR USER INPUT
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("▶ TEST MODE paused. Press ENTER to send a new batch...");
                Console.ResetColor();

                Console.ReadLine(); // Wait here
            }

            return;
        }

        // ==========================
        // REAL LIVE MODE — BYTE READER
        // ==========================
        byte[] buffer = new byte[2048];

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var tcp = new TcpClient();
                _log.LogInformation("Connecting to scale {Host}:{Port} ...", _opt.Host, _opt.Port);

                await tcp.ConnectAsync(_opt.Host, _opt.Port, stoppingToken);

                if (!tcp.Connected)
                {
                    _log.LogWarning("Scale connection failed.");
                    await Task.Delay(_opt.ReconnectDelayMs, stoppingToken);
                    continue;
                }

                tcp.ReceiveTimeout = _opt.ReadTimeoutMs;
                _log.LogInformation("Scale connected to {Host}:{Port}", _opt.Host, _opt.Port);

                using NetworkStream stream = tcp.GetStream();

                // Flush any garbage already buffered
                while (stream.DataAvailable)
                {
                    await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), stoppingToken);
                }

                while (!stoppingToken.IsCancellationRequested)
                {
                    if (!stream.DataAvailable)
                    {
                        await Task.Delay(5, stoppingToken);
                        continue;
                    }

                    int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), stoppingToken);
                    if (bytesRead <= 0)
                        throw new IOException("Scale closed connection.");

                    string chunk = Encoding.ASCII.GetString(buffer, 0, bytesRead);

                    // Take only the *last* frame in this chunk
                    string[] frames = chunk.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                    if (frames.Length == 0) continue;

                    string lastFrame = WeightParser.StripControlChars(frames[^1]).Trim();
                    if (lastFrame.Length == 0) continue;

                    if (!WeightParser.TryParseWeight(lastFrame, _opt.MinDigits, out var raw))
                        continue;

                    var current = raw / _opt.Divisor;

                    // STABILITY RULES:
                    //   - same value repeated many times
                    //   - AND >= 2000 kg
                    if (haveLast && Math.Abs(current - lastValue) < 0.01m)
                        stableCounter++;
                    else
                        stableCounter = 1;

                    bool isStable = stableCounter >= 15 && current >= 2000m;

                    Console.WriteLine("Current Weight: " + current);

                    // Always publish above a threshold
                    if (current > 2000m)
                    {
                        await PublishAsync(current, isStable);
                    }

                    lastValue = current;
                    haveLast = true;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Scale loop error; reconnecting in {ms}ms...", _opt.ReconnectDelayMs);
                try
                {
                    await Task.Delay(_opt.ReconnectDelayMs, stoppingToken);
                }
                catch
                {
                    // ignore
                }
            }
        }
    }
}

// =======================
// Program
// =======================

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Configuration
        builder.Configuration
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables();

        // App options
        builder.Services.Configure<AppOptions>(builder.Configuration.GetSection("App"));

        // SignalR options (bind from configuration)
        builder.Services.AddOptions<SignalROptions>()
            .Bind(builder.Configuration.GetSection("SignalR"))
            .ValidateOnStart();

        // Scale options (bind from configuration)
        builder.Services.AddOptions<ScaleOptions>()
            .Bind(builder.Configuration.GetSection("Scale"))
            .ValidateOnStart();

        // ===== ServiceManager (shared) =====
        builder.Services.AddSingleton(sp =>
        {
            var sro = sp.GetRequiredService<IOptions<SignalROptions>>().Value;
            return new ServiceManagerBuilder()
                .WithOptions(o => o.ConnectionString = sro.ConnectionString)
                .BuildServiceManager();
        });

        // Services
        builder.Services.AddSingleton<WeightSignalRPublisher>();
        builder.Services.AddHostedService(sp => sp.GetRequiredService<WeightSignalRPublisher>());
        builder.Services.AddHostedService<WeightBridgeService>();

        // Logging
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        // Configure HTTP port
        var httpPort = builder.Configuration.GetValue<int>("App:HttpPort", 5001);
        builder.WebHost.UseUrls($"http://localhost:{httpPort}");

        builder.Services.AddCors();

        var app = builder.Build();

        app.UseCors(policy => policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod());

        // HTTP Endpoint: GET /device-id (local helper only)
        app.MapGet("/device-id", () =>
        {
            var deviceId = DeviceIdManager.GetOrCreateDeviceId();
            var filePath = DeviceIdManager.GetDeviceIdFilePath();

            return Results.Json(new
            {
                deviceId,
                filePath,
                timestamp = DateTime.UtcNow
            });
        });

        // Root endpoint
        app.MapGet("/", () => Results.Json(new
        {
            message = "Entry Weight Bridge Service API",
            endpoints = new[]
            {
                "/device-id - Get device identifier"
                // NOTE: negotiate is handled by another backend
            }
        }));

        Console.WriteLine("ENTRY Weight Bridge → Azure SignalR");
        Console.WriteLine($"HTTP API listening on http://localhost:{httpPort}");
        Console.WriteLine($"Device ID file: {DeviceIdManager.GetDeviceIdFilePath()}");
        Console.WriteLine("Ctrl+C to exit.");

        await app.RunAsync();
    }
}
