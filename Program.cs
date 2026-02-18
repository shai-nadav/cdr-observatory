using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.FileProviders;
using Pipeline.Components.OSVParser;
using Pipeline.Components.OSVParser.Cache;
using Pipeline.Components.OSVParser.Config;
using Pipeline.Components.OSVParser.Models;
using Pipeline.Components.OSVParser.Processing;
using CdrObservatory;

var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

// Serve static files (wwwroot/index.html)
var wwwrootPath = Path.Combine(AppContext.BaseDirectory, "wwwroot");
if (!Directory.Exists(wwwrootPath))
    wwwrootPath = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot");

if (Directory.Exists(wwwrootPath))
{
    app.UseDefaultFiles(new DefaultFilesOptions
    {
        FileProvider = new PhysicalFileProvider(wwwrootPath)
    });
    app.UseStaticFiles(new StaticFileOptions
    {
        FileProvider = new PhysicalFileProvider(wwwrootPath)
    });
}

var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Converters = { new JsonStringEnumConverter() },
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    WriteIndented = false,
};

app.MapPost("/api/process", async (HttpRequest request) =>
{
    string tempDir = null;
    var serverStartTime = DateTime.UtcNow;
    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
    try
    {
        if (!request.HasFormContentType)
            return Results.BadRequest(new { error = "Expected multipart form data" });

        var form = await request.ReadFormAsync();

        // Create temp directories
        tempDir = Path.Combine(Path.GetTempPath(), "cdr-observatory-" + Guid.NewGuid().ToString("N"));
        var inputDir = Path.Combine(tempDir, "input");
        var outputDir = Path.Combine(tempDir, "output");
        Directory.CreateDirectory(inputDir);
        Directory.CreateDirectory(outputDir);

        // Save SIP endpoints file if provided
        string sipFilePath = null;
        var sipFile = form.Files.GetFile("sipFile");
        if (sipFile != null && sipFile.Length > 0)
        {
            sipFilePath = Path.Combine(tempDir, "SipEndpoints.xml");
            using (var stream = new FileStream(sipFilePath, FileMode.Create))
            {
                await sipFile.CopyToAsync(stream);
            }
        }

        // Save CDR files
        var cdrFiles = form.Files.GetFiles("cdrFiles");
        if (cdrFiles == null || cdrFiles.Count == 0)
            return Results.BadRequest(new { error = "No CDR files provided" });

        foreach (var cdrFile in cdrFiles)
        {
            if (cdrFile.Length > 0)
            {
                var destPath = Path.Combine(inputDir, cdrFile.FileName);
                using (var stream = new FileStream(destPath, FileMode.Create))
                {
                    await cdrFile.CopyToAsync(stream);
                }
            }
        }

        // Parse optional form fields
        var extensionRanges = ParseFormList(form, "extensionRanges");
        var routingNumbers = ParseFormList(form, "routingNumbers");
        var huntGroupNumbers = ParseFormList(form, "huntGroupNumbers");
        var voicemailNumber = form.ContainsKey("voicemailNumber") ? form["voicemailNumber"].ToString().Trim() : null;

        // Build settings
        var settings = new ObservatorySettings
        {
            InputFolder = inputDir,
            OutputFolder = outputDir,
            ArchiveFolder = Path.Combine(tempDir, "archive"),
            DecodedFolder = Path.Combine(tempDir, "decoded"),
            SipEndpointsFile = sipFilePath,
            WriteDecodedCdrs = true,
            DeleteInputFiles = false,
            ExtensionRanges = extensionRanges,
            RoutingNumbers = routingNumbers,
            HuntGroupNumbers = huntGroupNumbers,
            VoicemailNumber = string.IsNullOrWhiteSpace(voicemailNumber) ? null : voicemailNumber,
            FilePattern = "*.*",
        };

        Directory.CreateDirectory(settings.ArchiveFolder);
        Directory.CreateDirectory(settings.DecodedFolder);

        // Run processing
        var cache = new InMemoryCacheStore();
        var logger = new LogCollector();
        var tracer = new ProcessingTracer(logger);
        ISipEndpointsProvider sipProvider = sipFilePath != null
            ? (ISipEndpointsProvider)new FileSipEndpointsProvider(sipFilePath)
            : new FileSipEndpointsProvider("");
        var pendingRepo = new NullPendingCallsRepository();
        var engine = new CdrProcessorEngine(settings, logger, sipProvider, pendingRepo, cache, tracer);
        var result = engine.ProcessFolder();

        // Write session manifest
        stopwatch.Stop();
        var clientIp = request.Headers["X-Forwarded-For"].FirstOrDefault()
            ?? request.Headers["X-Real-IP"].FirstOrDefault()
            ?? request.HttpContext.Connection.RemoteIpAddress?.ToString()
            ?? "unknown";
        // Strip port if present
        if (clientIp.Contains(':') && !clientIp.Contains('[')) 
        {
            var lastColon = clientIp.LastIndexOf(':');
            if (lastColon > clientIp.LastIndexOf('.')) clientIp = clientIp.Substring(0, lastColon);
        }

        // Geo lookup (best effort, free API)
        string country = null, city = null;
        try
        {
            using var http = new System.Net.Http.HttpClient { Timeout = TimeSpan.FromSeconds(3) };
            var geoJson = await http.GetStringAsync($"http://ip-api.com/json/{clientIp}?fields=country,city");
            var geoDoc = JsonDocument.Parse(geoJson);
            country = geoDoc.RootElement.TryGetProperty("country", out var c) ? c.GetString() : null;
            city = geoDoc.RootElement.TryGetProperty("city", out var ci) ? ci.GetString() : null;
        }
        catch { }

        var manifest = new
        {
            timestamp = serverStartTime,
            serverProcessingMs = stopwatch.ElapsedMilliseconds,
            clientIp,
            country,
            city,
            sipFile = sipFile?.FileName,
            cdrFiles = cdrFiles.Select(f => f.FileName).ToList(),
            stats = new { result.TotalFilesProcessed, result.TotalRecordsProcessed, result.TotalCallsIdentified }
        };
        File.WriteAllText(Path.Combine(tempDir, "manifest.json"), JsonSerializer.Serialize(manifest, jsonOptions));

        // Read decoded CDR output if available
        var decodedCdrs = new List<Dictionary<string, string>>();
        var decodedDir = settings.DecodedFolder ?? Path.Combine(inputDir, "output", "cdrsDecoded");
        if (Directory.Exists(decodedDir))
        {
            foreach (var decodedFile in Directory.GetFiles(decodedDir, "*.csv"))
            {
                decodedCdrs.AddRange(ReadCsvAsDicts(decodedFile));
            }
        }

        // Read legs output
        var legsData = new List<Dictionary<string, string>>();
        if (Directory.Exists(outputDir))
        {
            foreach (var legsFile in Directory.GetFiles(outputDir, "legs_*.csv"))
            {
                legsData.AddRange(ReadCsvAsDicts(legsFile));
            }
        }

        // Build response
        var response = new
        {
            legs = legsData,
            decodedCdrs = decodedCdrs,
            log = logger.Entries,
            serverProcessingMs = stopwatch.ElapsedMilliseconds,
            clientIp,
            country,
            city,
            stats = new
            {
                totalFilesProcessed = result.TotalFilesProcessed,
                totalRecordsProcessed = result.TotalRecordsProcessed,
                totalCallsIdentified = result.TotalCallsIdentified,
                processingTimeMs = result.ProcessingTimeMs,
                recordsPerSecond = result.RecordsPerSecond,
                errors = result.Errors,
                warnings = result.Warnings,
                candidateExtensions = result.CandidateExtensions.Select(c => new
                {
                    number = c.Number,
                    occurrences = c.Occurrences,
                    reasons = c.Reasons,
                }),
            },
        };

        return Results.Json(response, jsonOptions);
    }
    catch (Exception ex)
    {
        return Results.Json(new { error = ex.Message, stackTrace = ex.StackTrace }, jsonOptions, statusCode: 500);
    }
    finally
    {
        // Keep temp files for later access; cleanup handled by retention policy
    }
});

// Health check
app.MapGet("/api/health", () => Results.Ok(new { status = "ok", timestamp = DateTime.UtcNow }));

// List recent processing sessions
app.MapGet("/api/sessions", () =>
{
    var dirs = Directory.GetDirectories(Path.GetTempPath(), "cdr-observatory-*")
        .Select(d => new DirectoryInfo(d))
        .OrderByDescending(d => d.CreationTimeUtc)
        .Select(d =>
        {
            object manifest = null;
            var mPath = Path.Combine(d.FullName, "manifest.json");
            if (File.Exists(mPath))
            {
                try { manifest = JsonSerializer.Deserialize<object>(File.ReadAllText(mPath)); } catch { }
            }
            return new { name = d.Name, created = d.CreationTimeUtc, sizeMb = Math.Round(GetDirSize(d) / 1024.0 / 1024.0, 1), manifest };
        })
        .ToList();
    return Results.Json(dirs, jsonOptions);
});

// Background cleanup: 5 day retention, 500MB max
const int RetentionDays = 5;
const long MaxTotalBytes = 500L * 1024 * 1024;

var cleanupTimer = new System.Threading.Timer(_ =>
{
    try
    {
        var dirs = Directory.GetDirectories(Path.GetTempPath(), "cdr-observatory-*")
            .Select(d => new DirectoryInfo(d))
            .OrderByDescending(d => d.CreationTimeUtc)
            .ToList();

        // Delete dirs older than retention
        foreach (var d in dirs.Where(d => d.CreationTimeUtc < DateTime.UtcNow.AddDays(-RetentionDays)))
        {
            try { d.Delete(true); } catch { }
        }

        // If still over max size, delete oldest first
        dirs = dirs.Where(d => d.Exists).OrderBy(d => d.CreationTimeUtc).ToList();
        long total = dirs.Sum(d => GetDirSize(d));
        while (total > MaxTotalBytes && dirs.Count > 0)
        {
            var oldest = dirs[0];
            total -= GetDirSize(oldest);
            try { oldest.Delete(true); } catch { }
            dirs.RemoveAt(0);
        }
    }
    catch { }
}, null, TimeSpan.Zero, TimeSpan.FromHours(1));

static long GetDirSize(DirectoryInfo dir)
{
    try { return dir.EnumerateFiles("*", SearchOption.AllDirectories).Sum(f => f.Length); }
    catch { return 0; }
}

app.Run();

// Helper functions
static List<string> ParseFormList(IFormCollection form, string key)
{
    if (!form.ContainsKey(key))
        return new List<string>();

    var value = form[key].ToString().Trim();
    if (string.IsNullOrEmpty(value))
        return new List<string>();

    return value.Split(new[] { ',', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
        .Select(s => s.Trim())
        .Where(s => !string.IsNullOrEmpty(s))
        .ToList();
}

static List<Dictionary<string, string>> ReadCsvAsDicts(string csvPath)
{
    var results = new List<Dictionary<string, string>>();
    var lines = File.ReadAllLines(csvPath);
    if (lines.Length < 2) return results;

    var headers = ParseCsvLine(lines[0]);
    for (int i = 1; i < lines.Length; i++)
    {
        var values = ParseCsvLine(lines[i]);
        var dict = new Dictionary<string, string>();
        for (int j = 0; j < headers.Length && j < values.Length; j++)
        {
            dict[headers[j]] = values[j];
        }
        results.Add(dict);
    }
    return results;
}

static string[] ParseCsvLine(string line)
{
    var fields = new List<string>();
    bool inQuote = false;
    var current = new System.Text.StringBuilder();

    for (int i = 0; i < line.Length; i++)
    {
        char c = line[i];
        if (inQuote)
        {
            if (c == '"')
            {
                if (i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                }
                else
                {
                    inQuote = false;
                }
            }
            else
            {
                current.Append(c);
            }
        }
        else
        {
            if (c == '"')
            {
                inQuote = true;
            }
            else if (c == ',')
            {
                fields.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(c);
            }
        }
    }
    fields.Add(current.ToString());
    return fields.ToArray();
}
