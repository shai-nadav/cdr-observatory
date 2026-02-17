using System;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// Logger abstraction for CDR processor.
    /// Supports INFO, WARN, ERROR, and DEBUG levels.
    /// Abstracted for testing and TEM-CA LogManager integration.
    /// </summary>
    public interface IProcessorLogger
    {
        /// <summary>Log informational message</summary>
        void Info(string message);
        
        /// <summary>Log informational message with context</summary>
        void Info(string message, params object[] args);
        
        /// <summary>Log warning message</summary>
        void Warn(string message);
        
        /// <summary>Log warning message with context</summary>
        void Warn(string message, params object[] args);
        
        /// <summary>Log error message</summary>
        void Error(string message);
        
        /// <summary>Log error message with exception</summary>
        void Error(string message, Exception ex);
        
        /// <summary>Log error message with context</summary>
        void Error(string message, params object[] args);
        
        /// <summary>Log debug message (detailed operations)</summary>
        void Debug(string message);
        
        /// <summary>Log debug message with context</summary>
        void Debug(string message, params object[] args);
    }

    /// <summary>
    /// Debug log categories for detailed operation tracing.
    /// </summary>
    public static class DebugCategory
    {
        public const string LegSuppression = "LegSuppression";
        public const string ValueUpdate = "ValueUpdate";
        public const string LegMerging = "LegMerging";
        public const string PendingCalls = "PendingCalls";
        public const string HuntGroup = "HuntGroup";
        public const string Transfer = "Transfer";
        public const string CallLinking = "CallLinking";
        public const string Forwarding = "Forwarding";
    }
}


