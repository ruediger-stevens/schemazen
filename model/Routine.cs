﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SchemaZen.model {
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

		private const string SqlCreateRegex = @"\A" + Database.SqlWhitespaceOrCommentRegex + @"*?(CREATE)" + Database.SqlWhitespaceOrCommentRegex;
		private const string SqlCreateWithNameRegex = SqlCreateRegex + @"+{0}" + Database.SqlWhitespaceOrCommentRegex + @"+?(?:(?:(" + Database.SqlEnclosedIdentifierRegex + @"|" + Database.SqlRegularIdentifierRegex + @")\.)?(" + Database.SqlEnclosedIdentifierRegex + @"|" + Database.SqlRegularIdentifierRegex + @"))(?:\(|" + Database.SqlWhitespaceOrCommentRegex + @")";

		public Routine(string schema, string name) {
			Schema = schema;
			Name = name;
		}

		private string ScriptQuotedIdAndAnsiNulls(Database db, bool databaseDefaults)
		{
			string script = "";
			bool defaultQuotedId = !QuotedId;
			if (db != null && db.FindProp("QUOTED_IDENTIFIER") != null) {
				defaultQuotedId = db.FindProp("QUOTED_IDENTIFIER").Value == "ON";
			}
			if (defaultQuotedId != QuotedId) {
				script += string.Format(@"SET QUOTED_IDENTIFIER {0} {1}GO{1}",
					((databaseDefaults ? defaultQuotedId : QuotedId) ? "ON" : "OFF"), Environment.NewLine);
			}
			bool defaultAnsiNulls = !AnsiNull;
			if (db != null && db.FindProp("ANSI_NULLS") != null) {
				defaultAnsiNulls = db.FindProp("ANSI_NULLS").Value == "ON";
			}
			if (defaultAnsiNulls != AnsiNull) {
				script += string.Format(@"SET ANSI_NULLS {0} {1}GO{1}",
					((databaseDefaults ? defaultAnsiNulls : AnsiNull) ? "ON" : "OFF"), Environment.NewLine);
			}
			return script;
		}

		private string ScriptBase(Database db, string definition)
		{
			var before = ScriptQuotedIdAndAnsiNulls(db, false);
			var after = ScriptQuotedIdAndAnsiNulls(db, true);
			if (after != string.Empty)
				after = Environment.NewLine + "GO" + Environment.NewLine + after;
			
			// correct the name if it is incorrect
			var identifierEnd = new[] {TSqlTokenType.As, TSqlTokenType.On, TSqlTokenType.Variable, TSqlTokenType.LeftParenthesis};
			var identifier = new[] {TSqlTokenType.Identifier, TSqlTokenType.QuotedIdentifier, TSqlTokenType.Dot};
			var commentOrWhitespace = new[] {TSqlTokenType.MultilineComment, TSqlTokenType.SingleLineComment,TSqlTokenType.WhiteSpace};
			IList<ParseError> errors;
			TSqlFragment script = new TSql120Parser(initialQuotedIdentifiers: QuotedId).Parse(new StringReader(definition), out errors);
			var id =
				script.ScriptTokenStream.SkipWhile(t => !identifier.Contains(t.TokenType))
					.TakeWhile(t => identifier.Contains(t.TokenType) || commentOrWhitespace.Contains(t.TokenType))
					.Where(t => identifier.Contains(t.TokenType)).ToArray();
			var replaced = false;
			definition = string.Join(string.Empty, script.ScriptTokenStream.Select(t => {
				if (id.Contains(t)) {
					if (replaced)
						return string.Empty;
					else {
						replaced = true;
						return string.Format("[{0}].[{1}]", Schema, Name);
					}
				} else {
					return t.Text;
				}
			}));
			return before + definition + after;
		}

		public string ScriptCreate(Database db) {
			return ScriptBase(db, Text);
		}

		public string GetSQLTypeForRegEx() {
			var text = GetSQLType();
			if (RoutineType == RoutineKind.Procedure) // support shorthand - PROC
				return "(?:" + text + "|" + text.Substring(0, 4) + ")";
			else
				return text;
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
			bool replaced = false;
			string alter = null;
			if (RoutineType != RoutineKind.XmlSchemaCollection) {
				IList<ParseError> errors;
				TSqlFragment script = new TSql120Parser(initialQuotedIdentifiers: QuotedId).Parse(new StringReader(Text), out errors);
				
				alter = ScriptBase(db, string.Join(string.Empty, script.ScriptTokenStream.Select(t =>
				{
					if (t.TokenType == TSqlTokenType.Create && !replaced)
					{
						replaced = true;
						return "ALTER";
					}
					else
					{
						return t.Text;
					}
				})));
			}
			if (replaced)
				return alter;
			else
				throw new Exception(string.Format("Unable to script routine {0} {1}.{2} as ALTER", RoutineType, Schema, Name));
		}

		public IEnumerable<string> Warnings () {
			// check if the name is correct
			var regex = new Regex(string.Format(SqlCreateWithNameRegex, GetSQLTypeForRegEx()), RegexOptions.IgnoreCase | RegexOptions.Singleline);
			var match = regex.Match(Text);

			// the schema is captured in group index 2, and the name in 3

			var nameGroup = match.Groups[3];
			if (nameGroup.Success) {
				var name = nameGroup.Value;
				if (name.StartsWith("[") && name.EndsWith("]"))
					name = name.Substring(1, name.Length - 2);

				if (string.Compare(Name, name, StringComparison.InvariantCultureIgnoreCase) != 0) {
					yield return string.Format("Name from script definition '{0}' does not match expected name '{1}'", name, Name);
				}
			}
		} 
	}

}

