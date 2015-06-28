﻿using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace model {
	public class Routine {
		public enum RoutineKind {
			Procedure,
			Function,
			Trigger,
			View,
			XmlSchemaCollection
		}

		public bool AnsiNull;
		public string Name;
		public bool QuotedId;
		public RoutineKind RoutineType;
		public string Schema;
		public string Text;

		public Routine(string schema, string name) {
			Schema = schema;
			Name = name;
		}

		private string ScriptBase(Database db)
		{
			string script = "";
			bool defaultQuotedId = !QuotedId;
			if (db != null && db.FindProp("QUOTED_IDENTIFIER") != null) {
				defaultQuotedId = db.FindProp("QUOTED_IDENTIFIER").Value == "ON";
			}
			if (defaultQuotedId != QuotedId) {
				script += string.Format(@"SET QUOTED_IDENTIFIER {0} {1}GO{1}",
					(QuotedId ? "ON" : "OFF"), Environment.NewLine);
			}
			bool defaultAnsiNulls = !AnsiNull;
			if (db != null && db.FindProp("ANSI_NULLS") != null) {
				defaultAnsiNulls = db.FindProp("ANSI_NULLS").Value == "ON";
			}
			if (defaultAnsiNulls != AnsiNull) {
				script += string.Format(@"SET ANSI_NULLS {0} {1}GO{1}",
					(AnsiNull ? "ON" : "OFF"), Environment.NewLine);
			}
			// TODO: script back to default afterwards for next query in batch
			return script;
		}

		public string ScriptCreate(Database db) {
			return ScriptBase(db) + Text;
		}

		public string GetSQLType() {
			string text = RoutineType.ToString();
			return string.Join(string.Empty, text.AsEnumerable().Select(
				(c, i) => ((char.IsUpper(c) || i == 0) ? " " + char.ToUpper(c).ToString() : c.ToString())
				).ToArray()).Trim();
		}

		public string ScriptDrop() {
			return string.Format("DROP {0} [{1}].[{2}]", GetSQLType(), Schema, Name);
		}

		public string ScriptAlter(Database db) {
			var regex = new Regex(@"\A(?:--.*?(?:\r|\n)|/\*.*?\*/|\s*?)+?(CREATE\s+?)", RegexOptions.IgnoreCase);
			var match = regex.Match(Text);
			var group = match.Groups[1];
			if (group.Success) {
				return ScriptBase(db) + Text.Substring(0, group.Index) + "ALTER " + Text.Substring(group.Index + group.Length);
			} else {
				throw new Exception(string.Format("Unable to script routine {0} {1}.{2} as ALTER", RoutineType, Schema, Name));
			}
		}
	}
}
