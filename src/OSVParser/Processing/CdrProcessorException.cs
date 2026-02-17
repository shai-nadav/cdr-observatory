using System;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Custom exception for CDR processing errors.
    /// </summary>
    public class CdrProcessorException : Exception
    {
        public string SourceFile { get; }
        public int SourceLine { get; }

        public CdrProcessorException(string message) : base(message) { }

        public CdrProcessorException(string message, string sourceFile, int sourceLine)
            : base($"{message} [File: {sourceFile}, Line: {sourceLine}]")
        {
            SourceFile = sourceFile;
            SourceLine = sourceLine;
        }

        public CdrProcessorException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}

