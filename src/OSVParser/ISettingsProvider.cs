using System.Collections.Generic;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// Provides configuration settings. Abstracted for testing and TEM-CA integration.
    /// </summary>
    public interface ISettingsProvider
    {
        // -------------------------------------------------------------------
        // Paths
        // -------------------------------------------------------------------
        
        /// <summary>Input folder containing CDR files (.bf or .csv)</summary>
        string InputFolder { get; }
        
        /// <summary>Output folder for processed legs</summary>
        string OutputFolder { get; }
        
        /// <summary>Archive folder for processed source files</summary>
        string ArchiveFolder { get; }

        /// <summary>Work folder for temp/intermediate parser artifacts</summary>
        string WorkFolder { get; }

        /// <summary>Folder for decoded CDR output (when enabled)</summary>
        string DecodedFolder { get; }
        
        /// <summary>Folder for orphaned calls output</summary>
        string OrphanFolder { get; }
        
        /// <summary>SIP endpoints file path (XML)</summary>
        string SipEndpointsFile { get; }

        // -------------------------------------------------------------------
        // Extension Ranges
        // -------------------------------------------------------------------
        
        /// <summary>Extension ranges defining internal numbers</summary>
        IList<string> ExtensionRanges { get; }
        
        /// <summary>Voicemail pilot number</summary>
        string VoicemailNumber { get; }
        
        /// <summary>Routing-only numbers (CMS, pilots) - suppressed when 0 duration</summary>
        IList<string> RoutingNumbers { get; }
        
        /// <summary>Hunt group numbers</summary>
        IList<string> HuntGroupNumbers { get; }

        // -------------------------------------------------------------------
        // Processing Settings
        // -------------------------------------------------------------------
        
        /// <summary>Instance identifier for logging and isolation</summary>
        string InstanceId { get; }
        
        /// <summary>File pattern to match input files (e.g., "*.bf", "*.csv")</summary>
        string FilePattern { get; }
        
        /// <summary>Hours to retain incomplete calls before orphaning</summary>
        int IncompleteRetentionHours { get; }
        
        /// <summary>Max pending calls in memory before eviction</summary>
        int MaxPendingQueueSize { get; }

        // -------------------------------------------------------------------
        // Output Settings
        // -------------------------------------------------------------------
        
        /// <summary>Write decoded CDR files for debugging</summary>
        bool WriteDecodedCdrs { get; }

        /// <summary>Delete input files after processing</summary>        
        bool DeleteInputFiles { get; }


    }
}



