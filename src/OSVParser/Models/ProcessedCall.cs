using System.Collections.Generic;

namespace Pipeline.Components.OSVParser.Models
{
    /// <summary>
    /// A complete call with all legs grouped by GID, ordered by InLegConnectTime.
    /// </summary>
    public class ProcessedCall
    {
        public string GlobalCallId { get; set; }
        public CallDirection CallDirection { get; set; }
        public int TotalLegs { get; set; }
        public bool IsAnswered { get; set; }
        public int TotalDuration { get; set; }

        // Originating parties (from first leg)
        public string CallerExtension { get; set; }
        public string CallerExternal { get; set; }
        public string DialedNumber { get; set; }
        public string OriginalDialedDigits { get; set; }

        public string Extension { get; set; }
        public string HuntGroupNumber { get; set; }
        public string ThreadId { get; set; }

        // All legs ordered by InLegConnectTime
        public List<ProcessedLeg> Legs { get; set; } = new List<ProcessedLeg>();
    }
}

