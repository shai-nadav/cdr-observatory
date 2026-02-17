using System.Collections.Generic;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// Provides SIP endpoint data for address-to-trunk translation.
    /// Abstracted for testing and TEM-CA DB integration.
    /// </summary>
    public interface ISipEndpointsProvider
    {
        /// <summary>
        /// Load SIP endpoints and return address-to-trunk mapping.
        /// For TEM-CA: loads from SIPEndpoints DB table.
        /// For standalone: loads from XML file.
        /// </summary>
        /// <returns>Dictionary mapping SIP address to trunk name</returns>
        IDictionary<string, string> LoadAddressToTrunkMap();
        
        /// <summary>
        /// Check if an endpoint is a PSTN gateway (external).
        /// Used for direction detection.
        /// </summary>
        bool IsPstn(string endpoint);
        
        /// <summary>
        /// Check if endpoints were loaded successfully.
        /// </summary>
        bool IsLoaded { get; }
        
        /// <summary>
        /// Number of PSTN endpoints loaded.
        /// </summary>
        int PstnCount { get; }
    }

    /// <summary>
    /// SIP endpoint info for TEM-CA integration.
    /// </summary>
    public class SipEndpointInfo
    {
        /// <summary>SIP address (FQDN or IP)</summary>
        public string Address { get; set; }
        
        /// <summary>Trunk name (for output replacement)</summary>
        public string Trunk { get; set; }
        
        /// <summary>Trunk group name</summary>
        public string TrunkGroup { get; set; }
        
        /// <summary>Endpoint type (Gateway, Internal, etc.)</summary>
        public string Type { get; set; }
    }
}

