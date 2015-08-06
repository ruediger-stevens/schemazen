﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SchemaZen.model
{
	public class ZenVisitor : TSqlFragmentVisitor {
		private Database _db;

		public ZenVisitor (Database db) {
			_db = db;
		}

		public override void ExplicitVisit (CreateTableStatement node) {
			base.ExplicitVisit(node);

			string schema = node.SchemaObjectName.SchemaIdentifier != null ? node.SchemaObjectName.SchemaIdentifier.Value : Database.DefaultSchema;

			Table t = new Table(schema, node.SchemaObjectName.BaseIdentifier.Value);
			var pos = 1;
			foreach (var col in node.Definition.ColumnDefinitions) {
				var nullable = !col.Constraints.OfType<NullableConstraintDefinition>().Any(c => !c.Nullable);
				var defaultExpression = col.DefaultConstraint != null ? new Default(col.DefaultConstraint.ConstraintIdentifier.Value, col.DefaultConstraint.Expression.ToString()) : null;
				Column column;
				if (col.DataType is SqlDataTypeReference) {
					var p = ((SqlDataTypeReference) col.DataType).Parameters;
					if (p.Count == 1)
						column = new Column(col.ColumnIdentifier.Value, col.DataType.Name.BaseIdentifier.Value, int.Parse(p[0].Value), nullable, defaultExpression);
					else if (p.Count > 1)
						column = new Column(col.ColumnIdentifier.Value, col.DataType.Name.BaseIdentifier.Value, byte.Parse(p[0].Value), int.Parse(p[1].Value), nullable, defaultExpression);
					else
						column = new Column(col.ColumnIdentifier.Value, col.DataType.Name.BaseIdentifier.Value, nullable, defaultExpression);
				} else 
					throw new NotImplementedException(string.Format("Unable to parse DataType {0}", col.DataType.Name));
				column.Position = pos;
				column.IsRowGuidCol = col.IsRowGuidCol;
				column.ComputedDefinition = col.ComputedColumnExpression == null ? null : col.ComputedColumnExpression.ToString();
				if (col.IdentityOptions != null)
					column.Identity = new Identity(int.Parse(((IntegerLiteral) col.IdentityOptions.IdentitySeed).Value), int.Parse(((IntegerLiteral) col.IdentityOptions.IdentityIncrement).Value));
				
				t.Columns.Add(column);
				pos++;
				
			}
			foreach (var con in node.Definition.TableConstraints) {
				if (con is UniqueConstraintDefinition) {
					var unique = (UniqueConstraintDefinition)con;

					t.Constraints.Add(new Constraint(unique.ConstraintIdentifier.Value, unique.IsPrimaryKey ? "PRIMARY KEY" : "UNIQUE", string.Join(",", unique.Columns.Select(c => c.Column.MultiPartIdentifier.Identifiers.Select(i => i.Value).First()))) {
																																																																 Clustered = unique.Clustered ?? false,
																																																																 Unique = true
																																																															 });

				}
			}

			_db.Tables.Add(t);
		}

		private static IEnumerable<TSqlParserToken> GetNodesInFragment(TSqlFragment fragment) {
			return Enumerable.Range(fragment.FirstTokenIndex, fragment.LastTokenIndex - fragment.FirstTokenIndex).Select(x => fragment.ScriptTokenStream[x]);
		}

		private static string GetNodeTokenText(TSqlFragment fragment)
		{
			StringBuilder tokenText = new StringBuilder();
			foreach (var token in GetNodesInFragment(fragment).Select(f => f.Text))
			{
				tokenText.Append(token);
			}

			return tokenText.ToString();
		}

		private void AddRoutine (Routine.RoutineKind kind, SchemaObjectName name, TSqlFragment node) {
			AddRoutine(kind, name.SchemaIdentifier != null ? name.SchemaIdentifier.Value : Database.DefaultSchema, name.BaseIdentifier.Value, node);
		}

		private void AddRoutine(Routine.RoutineKind kind, string schema, string name, TSqlFragment node)
		{
			Routine r = new Routine(schema, name)
			{
				RoutineType = kind,
				Text = GetNodeTokenText(node)
			};

			_db.Routines.Add(r);
		}

		public override void ExplicitVisit (CreateProcedureStatement node) {
			base.ExplicitVisit(node);
			AddRoutine(Routine.RoutineKind.Procedure, node.ProcedureReference.Name, node);
		}

		public override void ExplicitVisit(CreateFunctionStatement node)
		{
			base.ExplicitVisit(node);
			AddRoutine(Routine.RoutineKind.Function, node.Name, node);
		}

		public override void ExplicitVisit(CreateViewStatement node)
		{
			base.ExplicitVisit(node);
			AddRoutine(Routine.RoutineKind.View, node.SchemaObjectName, node);
		}
	}
}
