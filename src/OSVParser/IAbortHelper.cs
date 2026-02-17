namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// Provides stop signal for graceful shutdown.
    /// Abstracted for testing and TEM-CA integration.
    /// </summary>
    public interface IAbortHelper
    {
        /// <summary>
        /// Check if stop has been requested.
        /// Call this frequently during processing loops.
        /// </summary>
        bool IsAbortRequested { get; }
        
        /// <summary>
        /// Request abort (for testing or manual stop).
        /// </summary>
        void RequestAbort();
        
        /// <summary>
        /// Reset abort flag (for reuse in tests).
        /// </summary>
        void Reset();
    }
    
    /// <summary>
    /// Simple implementation for standalone/testing use.
    /// </summary>
    public class SimpleAbortHelper : IAbortHelper
    {
        private volatile bool _abortRequested;
        
        public bool IsAbortRequested => _abortRequested;
        
        public void RequestAbort() => _abortRequested = true;
        
        public void Reset() => _abortRequested = false;
    }
    
    /// <summary>
    /// No-op implementation (never aborts).
    /// </summary>
    public class NullAbortHelper : IAbortHelper
    {
        public static readonly NullAbortHelper Instance = new NullAbortHelper();
        
        public bool IsAbortRequested => false;
        public void RequestAbort() { }
        public void Reset() { }
    }
}

