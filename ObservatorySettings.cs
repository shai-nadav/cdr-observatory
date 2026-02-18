using System.Collections.Generic;
using Pipeline.Components.OSVParser;

namespace CdrObservatory
{
    /// <summary>
    /// ISettingsProvider implementation for the CDR Observatory web API.
    /// </summary>
    public class ObservatorySettings : ISettingsProvider
    {
        public string InputFolder { get; set; }
        public string OutputFolder { get; set; }
        public string ArchiveFolder { get; set; }
        public string WorkFolder { get; set; }
        public string DecodedFolder { get; set; }
        public string OrphanFolder { get; set; }
        public string SipEndpointsFile { get; set; }
        public IList<string> ExtensionRanges { get; set; } = new List<string>();
        public string VoicemailNumber { get; set; }
        public IList<string> RoutingNumbers { get; set; } = new List<string>();
        public IList<string> HuntGroupNumbers { get; set; } = new List<string>();
        public string InstanceId { get; set; } = "observatory";
        public string FilePattern { get; set; } = "*.*";
        public int IncompleteRetentionHours { get; set; } = 0;
        public int MaxPendingQueueSize { get; set; } = 0;
        public bool WriteDecodedCdrs { get; set; } = true;
        public bool DeleteInputFiles { get; set; } = false;
    }
}
