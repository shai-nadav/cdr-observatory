using System.Collections.Generic;

namespace Pipeline.Components.OSVParser.Models
{
    /// <summary>
    /// A number that appears to be an internal extension but is not in the configured range.
    /// </summary>
    public class CandidateExtension
    {
        public string Number { get; set; }
        public int Occurrences { get; set; }
        public List<string> Reasons { get; set; } = new List<string>();
    }
}

