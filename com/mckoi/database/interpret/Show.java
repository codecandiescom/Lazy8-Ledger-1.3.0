/**
 * com.mckoi.database.interpret.Show  13 Sep 2001
 *
 * Mckoi SQL Database ( http://www.mckoi.com/database )
 * Copyright (C) 2000, 2001  Diehl and Associates, Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * Version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License Version 2 for more details.
 *
 * You should have received a copy of the GNU General Public License
 * Version 2 along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 * Change Log:
 * 
 * 
 */

package com.mckoi.database.interpret;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Arrays;
import java.util.Collections;
import java.sql.SQLException;
import java.math.BigDecimal;
import com.mckoi.database.*;
import com.mckoi.database.sql.ParseException;
import com.mckoi.util.Stats;
import com.mckoi.database.global.Types;
import com.mckoi.database.global.NullObject;
import com.mckoi.database.global.StandardMessages;
import com.mckoi.database.global.SQLTypes;
import com.mckoi.database.jdbc.SQLQuery;

/**
 * Statement that handles SHOW and DESCRIBE sql commands.
 *
 * @author Tobias Downer
 */

public class Show extends Statement {

  // Various show statics,
  static final int TABLES          = 1;
  static final int STATUS          = 2;
  static final int DESCRIBE_TABLE  = 3;
  static final int CONNECTIONS     = 4;
  static final int PRODUCT         = 5;
  static final int CONNECTION_INFO = 6;

  /**
   * The name the table that we are to update.
   */
  String table_name;

  /**
   * The type of information that we are to show.
   */
  String show_type;

  /**
   * Arguments of the show statement.
   */
  Expression[] args;

  /**
   * The search expression for the show statement (where clause).
   */
  SearchExpression where_clause = new SearchExpression();

  /**
   * Convenience, creates an empty table with the given column names.
   */
  TemporaryTable createEmptyTable(Database d, String name, String[] cols)
                                                   throws DatabaseException {
    // Describe the given table...
    TableField[] fields = new TableField[cols.length];
    for (int i = 0; i < cols.length; ++i) {
      fields[i] = new TableField(cols[i], Types.DB_STRING,
                                 Integer.MAX_VALUE, false);
    }
    TemporaryTable temp_table = new TemporaryTable(d, name, fields);
    // No entries...
    temp_table.setupAllSelectableSchemes();
    return temp_table;
  }

  // ---------- Implemented from Statement ----------

  public void prepare() throws DatabaseException {
    // Get the show variables from the query model
    show_type = (String) cmd.getObject("show");
    show_type = show_type.toLowerCase();
    table_name = (String) cmd.getObject("table_name");
    args = (Expression[]) cmd.getObject("args");
    where_clause = (SearchExpression) cmd.getObject("where_clause");
  }

  public Table evaluate() throws DatabaseException {

    DatabaseQueryContext context = new DatabaseQueryContext(database);
    Database d = database.getDatabase();

    // Construct an executor for interpreting SQL queries inside here.
    SQLQueryExecutor executor = new SQLQueryExecutor();

    // The table we are showing,
    TemporaryTable show_table;

    try {

      // How we order the result set
      int[] order_set = null;

      if (show_type.equals("schema")) {

        SQLQuery query = new SQLQuery(
           "  SELECT \"name\" AS \"schema_name\", " +
           "         \"type\", " +
           "         \"other\" AS \"notes\" " +
           "    FROM SYS_INFO.sUSRSchemaInfo " +
           "ORDER BY schema_name");
        return executor.execute(database, query);

      }
      else if (show_type.equals("tables")) {

        String current_schema = database.getCurrentSchema();

        SQLQuery query = new SQLQuery(
           "  SELECT \"name\" AS \"table_name\", " +
           "         'SELECT INSERT UPDATE DELETE ALTER DROP ' AS \"user_privs\" " +
           "    FROM SYS_INFO.sUSRTableInfo " +
           "   WHERE \"schema\" = ? " +
           "ORDER BY \"table_name\"");
        query.addVar(current_schema);

        return executor.execute(database, query);

//        // Return a list of all tables in the database,
//        TableName[] all_tables = database.getTableList();
//
//        ArrayList table_list = new ArrayList();
//        for (int i = 0; i < all_tables.length; ++i) {
//          TableName tn = all_tables[i];
//          if (all_tables[i].getSchema().equals(current_schema)) {
//            table_list.add(tn);
//          }
//        }
//        Collections.sort(table_list);
//
//        TableField[] fields = new TableField[2];
//        fields[0] = new TableField("table_name", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[1] = new TableField("user_privs", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        TemporaryTable temp_table = new TemporaryTable(d, "AllTables", fields);
//
//        DataCell cell;
//        int sz = table_list.size();
//        for (int i = 0; i < sz; ++i) {
//          temp_table.newRow();
//
//          TableName table_name = (TableName) table_list.get(i);
//          cell = DataCellFactory.generateDataCell(fields[0],
//                                                  table_name.getName());
//          temp_table.setRowCell(cell, 0, i);
//
//          // Check privs on this table,
//          StringBuffer priv_string = new StringBuffer();
//          if (user.canSelectFromTable(database, table_name, null)) {
//            priv_string.append("SELECT ");
//          }
//          if (user.canInsertIntoTable(database, table_name, null)) {
//            priv_string.append("INSERT ");
//          }
//          if (user.canUpdateTable(database, table_name, null)) {
//            priv_string.append("UPDATE ");
//          }
//          if (user.canDeleteFromTable(database, table_name)) {
//            priv_string.append("DELETE ");
//          }
//          if (user.canAlterTable(database, table_name)) {
//            priv_string.append("ALTER ");
//          }
//          if (user.canDropTable(database, table_name)) {
//            priv_string.append("DROP ");
//          }
//
//          cell = DataCellFactory.generateDataCell(fields[1],
//                                                  new String(priv_string));
//          temp_table.setRowCell(cell, 1, i);
//
//        }
//
//        temp_table.setupAllSelectableSchemes();
//        show_table = temp_table;

      }
      else if (show_type.equals("status")) {

        SQLQuery query = new SQLQuery(
           "  SELECT \"stat_name\" AS \"name\", " +
           "         \"value\" " +
           "    FROM SYS_INFO.sUSRDatabaseStatistics ");

        return executor.execute(database, query);

//        // Package up the database stats information in a temporary table
//        // and return it.
//
//        TableField[] fields = new TableField[2];
//        fields[0] = new TableField("name", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[1] = new TableField("value", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        TemporaryTable temp_table = new TemporaryTable(d, "Status", fields);
//
//        Stats stats = database.getSystem().stats();
//        stats.set((int) (Runtime.getRuntime().freeMemory() / 1024),
//                                                      "Runtime.memory.freeKB");
//        stats.set((int) (Runtime.getRuntime().totalMemory() / 1024),
//                                                      "Runtime.memory.totalKB");
//
//        String[] key_list = stats.keyList();
//
//        DataCell cell;
//        for (int i = 0; i < key_list.length; ++i) {
//          temp_table.newRow();
//
//          String key = key_list[i];
//          cell = DataCellFactory.generateDataCell(fields[0], key);
//          temp_table.setRowCell(cell, 0, i);
//
//          cell = DataCellFactory.generateDataCell(fields[1],
//                                                  stats.statString(key));
//          temp_table.setRowCell(cell, 1, i);
//
//        }
//
//  //      // HACK:
//  //      //   When a 'show status' command is executed, we encourage the VM to
//  //      //   do a garbage sweep.  This will hopefully refresh the free memory
//  //      //   counter for if we query the memory again.
//  //      System.gc();
//
//        temp_table.setupAllSelectableSchemes();
//        show_table = temp_table;

      }
      else if (show_type.equals("describe_table")) {

        TableName tname = resolveTableName(table_name, database);
        if (!database.tableExists(tname)) {
          throw new StatementException(
                              "Unable to find table '" + table_name + "'");
        }

        SQLQuery query = new SQLQuery(
          "  SELECT \"column\" AS \"name\", " +
          "         i_sql_type(\"type_desc\", \"size\", \"scale\") AS \"type\", " +
          "         \"not_null\", " +
          "         \"index_str\" AS \"index\", " +
          "         \"default\" " +
          "    FROM SYS_INFO.sUSRTableColumns " +
          "   WHERE \"schema\" = ? " +
          "     AND \"table\" = ? " +
          "ORDER BY \"seq_no\" ");
        query.addVar(tname.getSchema());
        query.addVar(tname.getName());

        return executor.execute(database, query);

//        // Describe the given table...
//        TableField[] fields = new TableField[5];
//        fields[0] = new TableField("name", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[1] = new TableField("type", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[2] = new TableField("not_null", Types.DB_BOOLEAN, false);
//        fields[3] = new TableField("index", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[4] = new TableField("default", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//
//        TemporaryTable temp_table = new TemporaryTable(d, "Column", fields);
//
//        // Fill in the table with info...
//        TableName tname = resolveTableName(table_name, database);
//        DataTableDef def = database.getTable(tname).getDataTableDef();
//        for (int i = 0; i < def.columnCount(); ++i) {
//          DataTableColumnDef col = def.columnAt(i);
//          temp_table.newRow();
//          temp_table.setRowObject(col.getName(), "Column.name");
//          String type = col.getSQLTypeString();
//          int size = col.getSize();
//          int scale = col.getScale();
//          if (col.getSQLType() == SQLTypes.JAVA_OBJECT) {
//            type += "(" + col.getClassConstraint() + ")";
//          }
//          else {
//            if (size >= 0 && scale == -1) {
//              type += "(" + col.getSize() + ")";
//            }
//            else if (size >= 0 && scale >= 0) {
//              type += "(" + col.getSize() + ", " + col.getScale() + ")";
//            }
//          }
//          temp_table.setRowObject(type, "Column.type");
//          temp_table.setRowObject(new Boolean(col.isNotNull()),
//                                  "Column.not_null");
//          temp_table.setRowObject(col.getIndexScheme(), "Column.index");
//          String exp_text = "NONE";
//          Expression e = col.getDefaultExpression(context.getSystem());
//          if (e != null) {
//            exp_text = new String(e.text());
//          }
//          temp_table.setRowObject(exp_text, "Column.default");
//        }
//
//        temp_table.setupAllSelectableSchemes();
//        show_table = temp_table;

      }
      else if (show_type.equals("connections")) {

        SQLQuery query = new SQLQuery(
           "SELECT * FROM SYS_INFO.sUSRCurrentConnections");

        return executor.execute(database, query);

//        // Describe the given table...
//        TableField[] fields = new TableField[4];
//        fields[0] = new TableField("username", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[1] = new TableField("host_string", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[2] = new TableField("last_command", Types.DB_TIME, false);
//        fields[3] = new TableField("time_connected", Types.DB_TIME, false);
//
//        TemporaryTable temp_table = new TemporaryTable(d, "Connection", fields);
//
//        UserManager user_manager = d.getUserManager();
//        ArrayList user_data = new ArrayList();
//        // Synchronize over the user manager while we inspect the information,
//        synchronized (user_manager) {
//          for (int i = 0; i < user_manager.userCount(); ++i) {
//            User user = user_manager.userAt(i);
//            user_data.add(user.getUserName());
//            user_data.add(user.getConnectionString());
//            user_data.add(new Date(user.getLastCommandTime()));
//            user_data.add(new Date(user.getTimeConnected()));
//          }
//        }
//
//        // Add the data to the temporary table
//        for (int i = 0; i < user_data.size(); i += 4) {
//          temp_table.newRow();
//          temp_table.setRowObject(user_data.get(i), "Connection.username");
//          temp_table.setRowObject(user_data.get(i + 1),
//                                  "Connection.host_string");
//          temp_table.setRowObject(user_data.get(i + 2),
//                                  "Connection.last_command");
//          temp_table.setRowObject(user_data.get(i + 3),
//                                  "Connection.time_connected");
//        }
//
//        temp_table.setupAllSelectableSchemes();
//        show_table = temp_table;

      }
      else if (show_type.equals("product")) {

        SQLQuery query = new SQLQuery(
           "SELECT \"name\", \"version\" FROM " +
           "  ( SELECT \"value\" AS \"name\" FROM SYS_INFO.sUSRProductInfo " +
           "     WHERE \"var\" = 'name' ), " +
           "  ( SELECT \"value\" AS \"version\" FROM SYS_INFO.sUSRProductInfo " +
           "     WHERE \"var\" = 'version' ) "
        );

        return executor.execute(database, query);

//        // Product information,
//
//        // Describe the given table...
//        TableField[] fields = new TableField[2];
//        fields[0] = new TableField("name", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[1] = new TableField("version", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//
//        TemporaryTable temp_table = new TemporaryTable(d, "Product", fields);
//
//        temp_table.newRow();
//        temp_table.setRowObject(StandardMessages.NAME, "Product.name");
//        temp_table.setRowObject(StandardMessages.VERSION, "Product.version");
//
//        temp_table.setupAllSelectableSchemes();
//        show_table = temp_table;

      }
      else if (show_type.equals("connection_info")) {

        SQLQuery query = new SQLQuery(
           "SELECT * FROM SYS_INFO.sUSRConnectionInfo"
        );

        return executor.execute(database, query);

//        // Information about the connection,
//
//        // Describe the given table...
//        TableField[] fields = new TableField[2];
//        fields[0] = new TableField("var", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[1] = new TableField("value", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//
//        TemporaryTable temp_table = new TemporaryTable(d, "ConnectionInfo", fields);
//
//        temp_table.newRow();
//        temp_table.setRowObject("auto_commit", "ConnectionInfo.var");
//        temp_table.setRowObject(database.getAutoCommit() ? "true" : "false",
//                                "ConnectionInfo.value");
//        temp_table.newRow();
//        temp_table.setRowObject("isolation_level", "ConnectionInfo.var");
//        temp_table.setRowObject(database.getTransactionIsolationAsString(),
//                                "ConnectionInfo.value");
//        temp_table.newRow();
//        temp_table.setRowObject("user", "ConnectionInfo.var");
//        temp_table.setRowObject(database.getUser().getUserName(),
//                                "ConnectionInfo.value");
//        temp_table.newRow();
//        temp_table.setRowObject("time_connection", "ConnectionInfo.var");
//        temp_table.setRowObject(
//                   new Date(database.getUser().getTimeConnected()).toString(),
//                   "ConnectionInfo.value");
//        temp_table.newRow();
//        temp_table.setRowObject("connection_string", "ConnectionInfo.var");
//        temp_table.setRowObject(database.getUser().getConnectionString(),
//                                "ConnectionInfo.value");
//        temp_table.newRow();
//        temp_table.setRowObject("current_schema", "ConnectionInfo.var");
//        temp_table.setRowObject(database.getCurrentSchema(),
//                                "ConnectionInfo.value");
//
//        temp_table.newRow();
//        temp_table.setRowObject("case_insensitive_identifiers",
//                                "ConnectionInfo.var");
//        temp_table.setRowObject(database.isInCaseInsensitiveMode() ?
//                                "true" : "false", "ConnectionInfo.value");
//
//        temp_table.setupAllSelectableSchemes();
//        show_table = temp_table;

      }

      else if (show_type.equals("jdbc_procedures")) {
        // Need implementing?
        show_table = createEmptyTable(d, "JDBCProcedures",
            new String[] { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                           "R1", "R2", "R3", "REMARKS", "PROCEDURE_TYPE" });
      }

      else if (show_type.equals("jdbc_procedure_columns")) {
        // Need implementing?
        show_table = createEmptyTable(d, "JDBCProcedureColumns",
            new String[] { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                           "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
                           "TYPE_NAME", "PRECISION", "LENGTH", "SCALE",
                           "RADIX", "NULLABLE", "REMARKS" });
      }

      else if (show_type.equals("jdbc_catalogs")) {
        // Need implementing?
        show_table = createEmptyTable(d, "JDBCCatalogs",
                                      new String[] { "TABLE_CAT" });
      }

      else if (show_type.equals("jdbc_table_types")) {
        // Describe the given table...
        TableField[] fields = new TableField[1];
        fields[0] = new TableField("TABLE_TYPE", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        TemporaryTable temp_table = new TemporaryTable(d, "JDBCTableTypes",
                                                       fields);
        String[] supported_types = { "TABLE", "VIEW", "SYSTEM TABLE" };
        for (int i = 0; i < supported_types.length; ++i) {
          temp_table.newRow();
          temp_table.setRowObject(supported_types[i],
                                  "JDBCTableTypes.TABLE_TYPE");
        }
        temp_table.setupAllSelectableSchemes();
        show_table = temp_table;
        order_set = new int[] { 0 };
      }

      else if (show_type.equals("jdbc_column_privileges")) {

        // This is for 'DatabaseMetaData.getColumnPrivileges' method.

        // NOTE: Currently no support for 'catalog' and 'schema' args,
        Expression catalog = args[0];
        Expression schema = args[1];
        Expression table = args[2];
        Expression columnNamePattern = args[3];

        Object ob = table.evaluate(null, null);
        String tablen = null;
        if (!(ob instanceof NullObject)) {
          tablen = ob.toString();
        }
        ob = columnNamePattern.evaluate(null, null);
        String column_pattern = null;
        if (!(ob instanceof NullObject)) {
          column_pattern = ob.toString();
        }

        // Describe the given table...
        TableField[] fields = new TableField[8];
        fields[0] = new TableField("TABLE_CAT", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[1] = new TableField("TABLE_SCHEM", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[2] = new TableField("TABLE_NAME", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[3] = new TableField("COLUMN_NAME", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[4] = new TableField("GRANTOR", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[5] = new TableField("GRANTEE", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[6] = new TableField("PRIVILEGE", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[7] = new TableField("IS_GRANTABLE", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);

        TemporaryTable temp_table =
                              new TemporaryTable(d, "JDBCColumnPrivs",fields);

        // The list of all tables visible within the transaction,
        TableName[] table_list = database.getTableList();
        // For each table,
        for (int i = 0; i < table_list.length; ++i) {
          String cur_table = table_list[i].getName();
          String cur_schema = table_list[i].getSchema();
          if (tablen == null || cur_table.equals(tablen)) {
            // We have a match so get the columns,
            DataTableDef def = database.getTable(cur_table).getDataTableDef();

            for (int n = 0; n < def.columnCount(); ++n) {
              DataTableColumnDef col = def.columnAt(n);
              String cur_column = col.getName();
              if (column_pattern != null &&
                  !PatternSearch.fullPatternMatch(column_pattern,
                                                  cur_column, '\\')) {
                continue;
              }

              String[] privs = { "SELECT", "INSERT", "UPDATE", "DELETE" };
              for (int p = 0; p < privs.length; ++p) {
                temp_table.newRow();
                temp_table.setRowObject(NullObject.NULL_OBJ,
                                        "JDBCColumnPrivs.TABLE_CAT");
                temp_table.setRowObject(cur_schema,
                                        "JDBCColumnPrivs.TABLE_SCHEM");
                temp_table.setRowObject(cur_table, "JDBCColumnPrivs.TABLE_NAME");
                temp_table.setRowObject(cur_column,
                                        "JDBCColumnPrivs.COLUMN_NAME");
                temp_table.setRowObject(NullObject.NULL_OBJ,
                                        "JDBCColumnPrivs.GRANTOR");
                temp_table.setRowObject("",
                                        "JDBCColumnPrivs.GRANTEE");
                temp_table.setRowObject(privs[p], "JDBCColumnPrivs.PRIVILEGE");
                temp_table.setRowObject(NullObject.NULL_OBJ,
                                        "JDBCColumnPrivs.IS_GRANTABLE");
              }

            }
          }
        }

        temp_table.setupAllSelectableSchemes();
        show_table = temp_table;
        order_set = new int[] { 6, 3 };

      }

      else if (show_type.equals("jdbc_table_privileges")) {
        // This is for 'DatabaseMetaData.getTablePrivileges' method.

        // Describe the given table...
        TableField[] fields = new TableField[7];
        fields[0] = new TableField("TABLE_CAT", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[1] = new TableField("TABLE_SCHEM", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[2] = new TableField("TABLE_NAME", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[3] = new TableField("GRANTOR", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[4] = new TableField("GRANTEE", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[5] = new TableField("PRIVILEGE", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);
        fields[6] = new TableField("IS_GRANTABLE", Types.DB_STRING,
                                   Integer.MAX_VALUE, false);

        TemporaryTable temp_table =
                               new TemporaryTable(d, "JDBCTablePrivs", fields);

        // The list of all tables visible within the transaction,
        TableName[] table_list = database.getTableList();

        for (int i = 0; i < table_list.length; ++i) {
          String[] privs = { "SELECT", "INSERT", "UPDATE", "DELETE" };
          for (int p = 0; p < privs.length; ++p) {
            temp_table.newRow();
            temp_table.setRowObject(NullObject.NULL_OBJ,
                                    "JDBCTablePrivs.TABLE_CAT");
            temp_table.setRowObject(table_list[i].getSchema(),
                                    "JDBCTablePrivs.TABLE_SCHEM");
            temp_table.setRowObject(table_list[i].getName(),
                                    "JDBCTablePrivs.TABLE_NAME");
            temp_table.setRowObject(NullObject.NULL_OBJ,
                                    "JDBCTablePrivs.GRANTOR");
            temp_table.setRowObject("", "JDBCTablePrivs.GRANTEE");
            temp_table.setRowObject(privs[p], "JDBCTablePrivs.PRIVILEGE");
            temp_table.setRowObject(NullObject.NULL_OBJ,
                                    "JDBCTablePrivs.IS_GRANTABLE");
          }
        }

        temp_table.setupAllSelectableSchemes();
        show_table = temp_table;
        order_set = new int[] { 5, 2, 1 };

      }

      else if (show_type.equals("jdbc_best_row_identifier")) {
        // Need implementing?
        show_table = createEmptyTable(d, "JDBCBestRowIdentifier",
              new String[] { "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                "PSEUDO_COLUMN" });
      }

      else if (show_type.equals("jdbc_version_columns")) {
        // Need implementing?
        show_table = createEmptyTable(d, "JDBCVersionColumn",
              new String[] { "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                "PSEUDO_COLUMN" });
      }

//      else if (show_type.equals("jdbc_type_info")) {
//
//        // Describe the given table...
//        TableField[] fields = new TableField[18];
//        fields[0] = new TableField("TYPE_NAME", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[1] = new TableField("DATA_TYPE", Types.DB_NUMERIC, false);
//        fields[2] = new TableField("PRECISION", Types.DB_NUMERIC, false);
//        fields[3] = new TableField("LITERAL_PREFIX", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[4] = new TableField("LITERAL_SUFFIX", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[5] = new TableField("CREATE_PARAMS", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[6] = new TableField("NULLABLE", Types.DB_NUMERIC, false);
//        fields[7] = new TableField("CASE_SENSITIVE", Types.DB_BOOLEAN, false);
//        fields[8] = new TableField("SEARCHABLE", Types.DB_NUMERIC, false);
//        fields[9] = new TableField("UNSIGNED_ATTRIBUTE", Types.DB_BOOLEAN,false);
//        fields[10] = new TableField("FIXED_PREC_SCALE", Types.DB_BOOLEAN, false);
//        fields[11] = new TableField("AUTO_INCREMENT", Types.DB_BOOLEAN, false);
//        fields[12] = new TableField("LOCAL_TYPE_NAME", Types.DB_STRING,
//                                   Integer.MAX_VALUE, false);
//        fields[13] = new TableField("MINIMUM_SCALE", Types.DB_NUMERIC, false);
//        fields[14] = new TableField("MAXIMUM_SCALE", Types.DB_NUMERIC, false);
//        fields[15] = new TableField("SQL_DATA_TYPE", Types.DB_NUMERIC, false);
//        fields[16] = new TableField("SQL_DATETIME_SUB", Types.DB_NUMERIC, false);
//        fields[17] = new TableField("NUM_PREC_RADIX", Types.DB_NUMERIC, false);
//
//        TemporaryTable temp_table =
//                             new TemporaryTable(d, "JDBCTypes", fields);
//
//        for (int i = 0; i < type_list.size(); ++i) {
//          TypeDesc desc = (TypeDesc) type_list.get(i);
//          temp_table.newRow();
//          desc.insertTo(temp_table);
//        }
//
//        temp_table.setupAllSelectableSchemes();
//        show_table = temp_table;
//        order_set = new int[] { 1 };
//
//      }

      else if (show_type.equals("jdbc_index_info")) {
        // Need implementing?
        show_table = createEmptyTable(d, "JDBCIndexInfo",
              new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
                "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"
              });
      }

      else {
        throw new StatementException("Unknown SHOW identifier: " + show_type);
      }

    }
    catch (SQLException e) {
      throw new DatabaseException("SQL Error: " + e.getMessage());
    }
    catch (ParseException e) {
      throw new DatabaseException("Parse Error: " + e.getMessage());
    }
    catch (TransactionException e) {
      throw new DatabaseException("Transaction Error: " + e.getMessage());
    }


    return show_table;

  }

  public boolean isExclusive() {
    // No show operations are exclusive,
    return false;
  }

  public List readsFromTables() {
    // The information we are retrieving doesn't require write or read locks.
    ArrayList read_list = new ArrayList();
    return read_list;
  }

  public List writesToTables() {
    // The information we are retrieving doesn't require write or read locks.
    ArrayList write_list = new ArrayList();
    return write_list;
  }


//  // ---------- Some static constants ----------
//
//  private static ArrayList type_list = new ArrayList();
//
//  static {
//
//    type_list.add(new TypeDesc(
//            "BIT", SQLTypes.BIT, 1, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "BOOLEAN", SQLTypes.BIT, 1, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "TINYINT", SQLTypes.TINYINT, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "SMALLINT", SQLTypes.SMALLINT, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "INTEGER", SQLTypes.INTEGER, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "BIGINT", SQLTypes.BIGINT, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "FLOAT", SQLTypes.FLOAT, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "REAL", SQLTypes.REAL, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "DOUBLE", SQLTypes.DOUBLE, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "NUMERIC", SQLTypes.NUMERIC, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "DECIMAL", SQLTypes.DECIMAL, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "CHAR", SQLTypes.CHAR, 9, "'", "'", null, true));
//    type_list.add(new TypeDesc(
//            "VARCHAR", SQLTypes.VARCHAR, 9, "'", "'", null, true));
//    type_list.add(new TypeDesc(
//            "LONGVARCHAR", SQLTypes.LONGVARCHAR, 9, "'", "'", null, true));
//    type_list.add(new TypeDesc(
//            "DATE", SQLTypes.DATE, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "TIME", SQLTypes.TIME, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "TIMESTAMP", SQLTypes.TIMESTAMP, 9, null, null, null, true));
//    type_list.add(new TypeDesc(
//            "BINARY", SQLTypes.BINARY, 9, null, null, null, false));
//    type_list.add(new TypeDesc(
//            "VARBINARY", SQLTypes.VARBINARY, 9, null, null, null, false));
//    type_list.add(new TypeDesc(
//            "LONGVARBINARY", SQLTypes.LONGVARBINARY,
//                             9, null, null, null, false));
//    type_list.add(new TypeDesc(
//            "JAVA_OBJECT", SQLTypes.JAVA_OBJECT, 9, null, null, null, false));
//
//  }
//
//  private static class TypeDesc {
//    String name, prefix, suffix;
//    int type, precision;
//    boolean searchable;
//    public TypeDesc(String name, int type, int precision,
//                    String prefix, String suffix, String oops,
//                    boolean searchable) {
//      this.name = name;
//      this.type = type;
//      this.precision = precision;
//      this.prefix = prefix;
//      this.suffix = suffix;
//      this.searchable = searchable;
//    }
//
//    void insertTo(TemporaryTable table) {
//      table.setRowObject(name,
//                         "JDBCTypes.TYPE_NAME");
//      table.setRowObject(new BigDecimal(type),
//                         "JDBCTypes.DATA_TYPE");
//      table.setRowObject(new BigDecimal(precision),
//                         "JDBCTypes.PRECISION");
//      table.setRowObject(prefix,
//                         "JDBCTypes.LITERAL_PREFIX");
//      table.setRowObject(suffix,
//                         "JDBCTypes.LITERAL_SUFFIX");
//      table.setRowObject(NullObject.NULL_OBJ,
//                         "JDBCTypes.CREATE_PARAMS");
//      table.setRowObject(Boolean.TRUE,
//                         "JDBCTypes.NULLABLE");
//      table.setRowObject(Boolean.TRUE,
//                         "JDBCTypes.CASE_SENSITIVE");
//      table.setRowObject(searchable ? new BigDecimal(3) : new BigDecimal(0),
//                         "JDBCTypes.SEARCHABLE");
//      table.setRowObject(Boolean.FALSE,
//                         "JDBCTypes.UNSIGNED_ATTRIBUTE");
//      table.setRowObject(Boolean.FALSE,
//                         "JDBCTypes.FIXED_PREC_SCALE");
//      table.setRowObject(Boolean.FALSE,
//                         "JDBCTypes.AUTO_INCREMENT");
//      table.setRowObject(NullObject.NULL_OBJ,
//                         "JDBCTypes.LOCAL_TYPE_NAME");
//      table.setRowObject(new BigDecimal(0),
//                         "JDBCTypes.MINIMUM_SCALE");
//      table.setRowObject(new BigDecimal(10000000),
//                         "JDBCTypes.MAXIMUM_SCALE");
//      table.setRowObject(NullObject.NULL_OBJ,
//                         "JDBCTypes.SQL_DATA_TYPE");
//      table.setRowObject(NullObject.NULL_OBJ,
//                         "JDBCTypes.SQL_DATETIME_SUB");
//      table.setRowObject(new BigDecimal(10),
//                         "JDBCTypes.NUM_PREC_RADIX");
//
//    }
//
//  }

}
