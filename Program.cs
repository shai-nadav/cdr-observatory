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

        // Read decoded CDR output if available
        var decodedCdrs = new List<Dictionary<string, string>>();
        var decodedDir = Path.Combine(inputDir, "output", "cdrsDecoded");
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
        // Cleanup temp files
        if (tempDir != null && Directory.Exists(tempDir))
        {
            try { Directory.Delete(tempDir, recursive: true); }
            catch { /* best effort */ }
        }
    }
});

// Health check
app.MapGet("/api/health", () => Results.Ok(new { status = "ok", timestamp = DateTime.UtcNow }));

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
