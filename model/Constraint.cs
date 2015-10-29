using System.Collections.Generic;
using System.Linq;

namespace SchemaZen.model {
	public class Constraint {
		public bool Clustered;
		public List<string> Columns = new List<string>();
		public List<string> IncludedColumns = new List<string>();
		public string Name;
		public Table Table;
		public string Type;
		public bool Unique;
    public bool IgnoreDupKey;
    public byte FillFactor;
	  public bool IsPadded;
	  public bool AllowRowLocks;
	  public bool AllowPageLocks;
	  public bool StatisticsNoRecompute;

	  public Constraint(string name,
	    string type,
	    string columns,
	    bool ignoreDupKey,
      byte fillFactor,
      bool isPadded,
      bool allowRowLocks,
      bool allowPageLocks,
      bool statisticsNoRecompute) {
	    Name = name;
	    Type = type;
	    IgnoreDupKey = ignoreDupKey;
	    FillFactor = fillFactor;
      IsPadded = isPadded;
	    AllowRowLocks = allowRowLocks;
	    AllowPageLocks = allowPageLocks;
      StatisticsNoRecompute = statisticsNoRecompute;
	    if (!string.IsNullOrEmpty(columns)) {
	      Columns = new List<string>(columns.Split(','));
	    }
	  }

	  public string ClusteredText {
			get { return !Clustered ? "NONCLUSTERED" : "CLUSTERED"; }
		}

		public string UniqueText {
			get { return !Unique ? "" : "UNIQUE"; }
		}

		public string Script() {
		  string flags;
		  {
        var flagsBuilder = new System.Text.StringBuilder();
        AddFlag(flagsBuilder, "IGNORE_DUP_KEY", IgnoreDupKey);
        AddFlag(flagsBuilder, "FILLFACTOR", FillFactor);
        AddFlag(flagsBuilder, "PAD_INDEX", IsPadded);
        AddFlag(flagsBuilder, "ALLOW_ROW_LOCKS", AllowRowLocks);
        AddFlag(flagsBuilder, "ALLOW_PAGE_LOCKS", AllowPageLocks);
        AddFlag(flagsBuilder, "STATISTICS_NORECOMPUTE", StatisticsNoRecompute);
		    flags = flagsBuilder.ToString();
		  }
			if (Type == "INDEX") {
        string sql = string.Format("CREATE {0} {1} INDEX [{2}] ON [{3}].[{4}] ([{5}])" + System.Environment.NewLine + "  WITH({6})", UniqueText, ClusteredText, Name,
					Table.Owner, Table.Name,
					string.Join("], [", Columns.ToArray()),
          flags);
				if (IncludedColumns.Count > 0) {
					sql += string.Format(" INCLUDE ([{0}])", string.Join("], [", IncludedColumns.ToArray()));
				}
				return sql;
			}
      return string.Format("CONSTRAINT [{0}] {1} {2} ([{3}])" + System.Environment.NewLine + "  WITH({4})", Name, Type, ClusteredText,
				string.Join("], [", Columns.ToArray()), flags);
		}

    private void AddFlag(System.Text.StringBuilder builder, string name, bool value)
    {
      if (builder.Length > 0) {
        builder.Append(", ");
      }
      builder.Append(name);
      builder.Append(" = ");
      builder.Append(value ? "ON" : "OFF");
    }
    private void AddFlag(System.Text.StringBuilder builder, string name, byte value)
    {
      if (builder.Length > 0)
      {
        builder.Append(", ");
      }
      builder.Append(name);
      builder.Append(" = ");
      builder.Append(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }
  }
}
