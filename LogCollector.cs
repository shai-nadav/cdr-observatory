using System;
using System.Collections.Generic;
using Pipeline.Components.OSVParser;

namespace CdrObservatory
{
    /// <summary>
    /// IProcessorLogger implementation that collects all log lines into a list for returning to the frontend.
    /// </summary>
    public class LogCollector : IProcessorLogger
    {
        private readonly List<LogEntry> _entries = new List<LogEntry>();
        private readonly object _lock = new object();

        public IReadOnlyList<LogEntry> Entries
        {
            get { lock (_lock) return _entries.ToArray(); }
        }

        public void Info(string message) => Add("INFO", message);
        public void Info(string message, params object[] args) => Add("INFO", SafeFormat(message, args));
        public void Warn(string message) => Add("WARN", message);
        public void Warn(string message, params object[] args) => Add("WARN", SafeFormat(message, args));
        public void Error(string message) => Add("ERROR", message);
        public void Error(string message, Exception ex) => Add("ERROR", $"{message}: {ex?.Message}");
        public void Error(string message, params object[] args) => Add("ERROR", SafeFormat(message, args));
        public void Debug(string message) => Add("DEBUG", message);
        public void Debug(string message, params object[] args) => Add("DEBUG", SafeFormat(message, args));

        private void Add(string level, string message)
        {
            lock (_lock)
            {
                _entries.Add(new LogEntry
                {
                    Timestamp = DateTime.UtcNow.ToString("HH:mm:ss.fff"),
                    Level = level,
                    Message = message
                });
            }
        }

        private static string SafeFormat(string message, object[] args)
        {
            try
            {
                return args != null && args.Length > 0 ? string.Format(message, args) : message;
            }
            catch
            {
                return message;
            }
        }
    }

    public class LogEntry
    {
        public string Timestamp { get; set; }
        public string Level { get; set; }
        public string Message { get; set; }
    }
}
