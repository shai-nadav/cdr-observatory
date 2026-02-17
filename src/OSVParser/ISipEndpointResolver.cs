namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// Unified interface for SIP endpoint resolution.
    /// Implementations may load data from XML files, databases, or adapters.
    /// </summary>
    public interface ISipEndpointResolver
    {
        /// <summary>Check if an endpoint is a PSTN gateway (external).</summary>
        bool IsPstn(string endpoint);

        /// <summary>Check if an endpoint is known (either PSTN or internal).</summary>
        bool IsKnown(string endpoint);

        /// <summary>Returns true if no endpoints are configured.</summary>
        bool IsEmpty { get; }

        /// <summary>Returns true if endpoint data was loaded successfully.</summary>
        bool IsLoaded { get; }

        /// <summary>Number of PSTN endpoints loaded.</summary>
        int PstnCount { get; }
    }
}
