using System.Collections.Generic;

namespace Pipeline.Components.OSVParser.Models
{
    /// <summary>
    /// Human-readable interpretations for numeric CDR field values.
    /// Based on OpenScape Voice V7 CDR manual field definitions.
    /// </summary>
    public static class FieldMappings
    {
        public static readonly Dictionary<int, string> ReleaseCause = new Dictionary<int, string>
        {
            { 0, "Not set" },
            { 1, "Unassigned number" },
            { 16, "Normal clearing" },
            { 17, "User busy" },
            { 18, "No user responding" },
            { 19, "No answer" },
            { 20, "Subscriber absent" },
            { 21, "Call rejected" },
            { 23, "Redirect" },
            { 25, "Routing error" },
            { 27, "Destination out of order" },
            { 28, "Invalid format" },
            { 31, "Normal unspecified" },
            { 34, "No circuit" },
            { 41, "Temporary failure" },
            { 79, "Not implemented" },
            { 86, "Call cleared" },
            { 102, "Timer expiry" },
            { 128, "Session timer expired" },
        };

        public static readonly Dictionary<int, string> AttemptIndicator = new Dictionary<int, string>
        {
            { 0, "Completed" },
            { 1, "Busy" },
            { 2, "Invalid number" },
            { 3, "No lines" },
            { 4, "Caller abort" },
            { 5, "No answer" },
            { 6, "Network problem" },
            { 7, "Unknown" },
            { 8, "No account" },
            { 9, "Unauthorized" },
        };

        public static readonly Dictionary<int, string> PartyIdentifier = new Dictionary<int, string>
        {
            { 900, "On OpenScape (Originating)" },
            { 901, "Not on OpenScape (Incoming)" },
            { 902, "On OpenScape (Terminating)" },
            { 903, "Not on OpenScape (Outgoing)" },
            { 999, "Unknown" },
        };

        // Per Call Feature (spec field 64) - bitmask
        public static readonly Dictionary<long, string> PerCallFeatureValues = new Dictionary<long, string>
        {
            { 2, "CF Busy" },
            { 4, "CF No Answer" },
            { 8, "CF Unconditional" },
            { 16, "CLIR" },
            { 128, "CLIP" },
            { 1048576, "Malicious Call Trace" },
        };

        // Per Call Feature Extension (spec field 106) - bitmask
        public static readonly Dictionary<long, string> PerCallFeatureExtValues = new Dictionary<long, string>
        {
            { 64, "CF to Voicemail" },
            { 1024, "Call to MLHG" },
            { 2048, "Call Pickup" },
            { 4096, "Directed Call Pickup" },
            { 8192, "E911" },
            { 16384, "Silent Monitor" },
            { 1048576, "Private Call" },
            { 2097152, "Business Call" },
        };

        // Call Event Indicator (spec field 107) - bitmask
        public static readonly Dictionary<long, string> CallEventIndicatorValues = new Dictionary<long, string>
        {
            { 128, "MLHG Advance No Answer" },
            { 256, "MLHG Overflow" },
            { 512, "MLHG Night Service" },
            { 1024, "Forwarded from MLHG" },
            { 2048, "Held Party Hung Up" },
            { 4096, "Holding Party Hung Up" },
            { 8192, "Call Picked Up" },
            { 65536, "CSTA Deflect" },
            { 1048576, "Feature Activation" },
        };

        public static string GetReleaseCauseText(int code)
        {
            string text;
            return ReleaseCause.TryGetValue(code, out text) ? text : $"Unknown ({code})";
        }

        public static string GetAttemptIndicatorText(int value)
        {
            string text;
            return AttemptIndicator.TryGetValue(value, out text) ? text : $"Unknown ({value})";
        }

        public static string GetPartyIdText(int value)
        {
            string text;
            return PartyIdentifier.TryGetValue(value, out text) ? text : $"Unknown ({value})";
        }

        public static string GetPerCallFeatureText(long value)
        {
            if (value == 0) return "None";
            return DecodeBitmask(value, PerCallFeatureValues);
        }

        public static string GetPerCallFeatureExtText(long value)
        {
            if (value == 0) return "None";
            return DecodeBitmask(value, PerCallFeatureExtValues);
        }

        public static string GetCallEventIndicatorText(long value)
        {
            if (value == 0) return "None";
            return DecodeBitmask(value, CallEventIndicatorValues);
        }

        private static string DecodeBitmask(long value, Dictionary<long, string> bitValues)
        {
            var parts = new List<string>();
            foreach (var kvp in bitValues)
            {
                if ((value & kvp.Key) == kvp.Key)
                {
                    parts.Add(kvp.Value);
                }
            }
            return parts.Count > 0 ? string.Join(", ", parts) : $"0x{value:X}";
        }
    }
}

