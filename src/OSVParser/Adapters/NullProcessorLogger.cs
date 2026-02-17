using System;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// No-op logger for standalone usage.
    /// </summary>
    public class NullProcessorLogger : IProcessorLogger
    {
        public void Info(string message) { }
        public void Info(string message, params object[] args) { }
        public void Warn(string message) { }
        public void Warn(string message, params object[] args) { }
        public void Error(string message) { }
        public void Error(string message, Exception ex) { }
        public void Error(string message, params object[] args) { }
        public void Debug(string message) { }
        public void Debug(string message, params object[] args) { }
    } 
}

