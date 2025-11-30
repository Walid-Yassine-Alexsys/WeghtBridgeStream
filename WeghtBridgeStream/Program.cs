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
using System.Globalization;
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
    public string HubName { get; set; } = "pabexit_weight_hub";
    public string MethodName { get; set; } = "ReceiveExitWeight";
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

    public bool TestMode { get; set; } = true;
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
// Weight Bridge (live or test)
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

    private readonly Regex _valueWithUnit = new(@"(?<!\S)(?<num>[+-]?\d+(?:[.,]\d+)?)[ ]*(?<unit>kg|g|t|lb|oz)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private readonly Regex _anyNumber = new(@"[+-]?\d+(?:[.,]\d+)?", RegexOptions.Compiled);

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
        // TEST MODE (modified)
        // ==========================
        if (_opt.TestMode)
        {
            var rnd = new Random();
            _log.LogWarning("WeightBridge TEST MODE enabled.");

            while (!stoppingToken.IsCancellationRequested)
            {
                // 10 UNSTABLE VALUES
                for (int i = 0; i < 10; i++)
                {
                    int weight = rnd.Next(0, _opt.TestMaxKg);
                    bool stable = false;

                    await PublishAsync(weight, stable);
                    await Task.Delay(_opt.TestTickMs, stoppingToken);
                }

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
        // REAL LIVE MODE
        // ==========================
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var tcp = new TcpClient();
                var connectTask = tcp.ConnectAsync(_opt.Host, _opt.Port);
                var winner = await Task.WhenAny(connectTask, Task.Delay(_opt.ReadTimeoutMs, stoppingToken));
                if (winner != connectTask) throw new TimeoutException("Scale connect timeout");

                await connectTask;

                using var stream = tcp.GetStream();
                using var reader = new StreamReader(stream, Encoding.ASCII, false, 8192, leaveOpen: true);

                _log.LogInformation("Scale connected to {host}:{port}", _opt.Host, _opt.Port);

                while (!stoppingToken.IsCancellationRequested)
                {
                    string? line;
                    try
                    {
                        using var lineCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                        lineCts.CancelAfter(_opt.ReadTimeoutMs);
                        line = await reader.ReadLineAsync(lineCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        continue;
                    }

                    if (line is null)
                        throw new IOException("Scale closed connection.");

                    var clean = Strip(line).Trim();
                    if (clean.Length == 0) continue;

                    if (TryParseWeight(clean, _opt.MinDigits, _valueWithUnit, _anyNumber, out var raw))
                    {
                        var current = raw / _opt.Divisor;

                        // ==========================
                        // NEW STABILITY RULES:
                        // Stable only if:
                        //   - Same value appears 3 times consecutively
                        //   - AND weight >= 2000
                        // ==========================
                        if (haveLast && Math.Abs(current - lastValue) < 0.01m)
                            stableCounter++;
                        else
                            stableCounter = 1;

                        bool isStable =
                            stableCounter >= 15
                            && current >= 2000m;

                        Console.WriteLine("Current Weight:  " + current);

                        // ALWAYS publish real-time
                        if(current > 2000){
                            await PublishAsync(current, isStable);
                        }

                        lastValue = current;
                        haveLast = true;
                    }


                    // Optional small delay (VERY LOW)
                    //await Task.Delay(50, stoppingToken);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Scale loop error; reconnecting in {ms}ms...", _opt.ReconnectDelayMs);
                await Task.Delay(_opt.ReconnectDelayMs, stoppingToken);
            }
        }

        // ==========================
        // Helpers
        // ==========================
        static string Strip(string s)
        {
            var sb = new StringBuilder(s.Length);
            foreach (var ch in s)
                if (!char.IsControl(ch)) sb.Append(ch);
            return sb.ToString();
        }

        static bool TryParseWeight(string line, int minDigits, Regex vw, Regex any, out decimal value)
        {
            var unitMatches = vw.Matches(line);
            if (unitMatches.Count > 0)
            {
                var m = unitMatches[^1].Groups["num"].Value;
                if (decimal.TryParse(m, NumberStyles.Float, CultureInfo.InvariantCulture, out value) ||
                    decimal.TryParse(m, NumberStyles.Float, CultureInfo.GetCultureInfo("fr-FR"), out value))
                    return true;
            }

            Match? best = null;
            foreach (Match m in any.Matches(line))
            {
                int digits = 0;
                foreach (var ch in m.Value) if (char.IsDigit(ch)) digits++;
                if (digits >= minDigits && (best is null || m.Value.Length > best.Value.Length))
                    best = m;
            }

            if (best is not null &&
                (decimal.TryParse(best.Value, NumberStyles.Float, CultureInfo.InvariantCulture, out value) ||
                 decimal.TryParse(best.Value, NumberStyles.Float, CultureInfo.GetCultureInfo("fr-FR"), out value)))
                return true;

            value = default;
            return false;
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

        // SignalR options
        builder.Services.AddOptions<SignalROptions>()
            .Configure(o =>
            {
                o.ConnectionString = "Endpoint=https://mycimarfluxsignalr.service.signalr.net;AccessKey=2aFWipEfcQGVj6VDehqMuGYwbqKG9tDrCSzWh7FgNUGj6UlZKTNJJQQJ99BKACi5YpzXJ3w3AAAAASRS8VVJ;Version=1.0;";
                o.HubName = "pabexit_weight_hub";
                o.MethodName = "ReceiveExitWeight";
            })
            .ValidateOnStart();

        // Scale options
        builder.Services.AddOptions<ScaleOptions>()
            .Configure(o =>
            {
                o.TestMode = false;
                o.TestTickMs = 150;
                o.TestMaxKg = 16000;
                o.TestRampStepKg = 250;
                o.TestNoiseMaxKg = 25;
                o.Host = "10.8.197.26";
                o.Port = 4001;
                o.ReadTimeoutMs = 3000;
                o.ReconnectDelayMs = 1500;
                o.Divisor = 1m;
                o.MinDigits = 3;
                o.StableToleranceKg = 20m;
                o.StableSamples = 6;
                o.PublishIntervalMs = 150;
            })
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
            message = "Weight Bridge Service API",
            endpoints = new[]   
            {
                "/device-id - Get device identifier"
                // NOTE: negotiate is handled by another backend
            }
        }));

        Console.WriteLine("Weight Bridge → Azure SignalR");
        Console.WriteLine($"HTTP API listening on http://localhost:{httpPort}");
        Console.WriteLine($"Device ID file: {DeviceIdManager.GetDeviceIdFilePath()}");
        Console.WriteLine("Ctrl+C to exit.");

        await app.RunAsync();
    }
}
