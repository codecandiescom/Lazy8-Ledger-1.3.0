/**
 * com.mckoi.database.jdbc.MDatabaseMetaData  23 Jul 2000
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

package com.mckoi.database.jdbc;

import java.sql.*;

/**
 * An implementation of JDBC's DatabaseMetaData.
 *
 * @author Tobias Downer
 */

public class MDatabaseMetaData implements DatabaseMetaData {

  /**
   * The Connection object associated with this meta data.
   */
  private MConnection connection;

  /**
   * The name and version of the database we are connected to.
   */
  private String database_name, database_version;

  /**
   * Constructor.
   */
  MDatabaseMetaData(MConnection connection) {
    this.connection = connection;
  }

  /**
   * Queries product information about the database we are connected to.
   */
  private void queryProductInformation() throws SQLException {
    if (database_name == null ||
        database_version == null) {
      Statement statement = connection.createStatement();
      ResultSet result = statement.executeQuery("SHOW PRODUCT");
      result.next();
      database_name = result.getString("name");
      database_version = result.getString("version");
      statement.close();
      result.close();
    }
  }



  //----------------------------------------------------------------------
  // First, a variety of minor information about the target database.

  public boolean allProceduresAreCallable() throws SQLException {
    // NOT SUPPORTED
    return false;
  }

  public boolean allTablesAreSelectable() throws SQLException {
    // No, only tables that the user has read access to,
    return false;
  }

  public String getURL() throws SQLException {
    return connection.getURL();
  }

  public String getUserName() throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet result_set = statement.executeQuery("SELECT USER()");
    result_set.next();
    String username = result_set.getString(1);
    result_set.close();
    statement.close();
    return username;
  }

  public boolean isReadOnly() throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet result_set = statement.executeQuery(
       " SELECT * FROM SYS_INFO.sUSRDatabaseStatistics " +
       "  WHERE \"stat_name\" = 'DatabaseSystem.read_only' ");
    boolean read_only = result_set.next();
    result_set.close();
    statement.close();
    return read_only;
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return true;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  public String getDatabaseProductName() throws SQLException {
    queryProductInformation();
    return database_name;
  }

  public String getDatabaseProductVersion() throws SQLException {
    queryProductInformation();
    return database_version;
  }

  public String getDriverName() throws SQLException {
    return MDriver.DRIVER_NAME;
  }

  public String getDriverVersion() throws SQLException {
    return MDriver.DRIVER_VERSION;
  }

  public int getDriverMajorVersion() {
    return MDriver.DRIVER_MAJOR_VERSION;
  }

  public int getDriverMinorVersion() {
    return MDriver.DRIVER_MINOR_VERSION;
  }

  public boolean usesLocalFiles() throws SQLException {
    // Depends if we are embedded or not,
    // ISSUE: We need to keep an eye on this for future enhancements to the
    //   Mckoi URL spec.
    if (getURL().toLowerCase().startsWith(":jdbc:mckoi:local://")) {
      return true;
    }
    else {
      return false;
    }
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    // Actually uses 3 files per table.  Why would a developer need this info?
    // Returning false,
    return false;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  public String getSQLKeywords() throws SQLException {
    // not implemented,
    return "show";
  }

  public String getNumericFunctions() throws SQLException {
    // not implemented,
    // We should put this as a query to the database.  It will need to
    // inspect all user defined functions also.
    return "";
  }

  public String getStringFunctions() throws SQLException {
    // not implemented,
    // We should put this as a query to the database.  It will need to
    // inspect all user defined functions also.
    return "";
  }

  public String getSystemFunctions() throws SQLException {
    // not implemented,
    return "";
  }

  public String getTimeDateFunctions() throws SQLException {
    // not implemented,
    // We should put this as a query to the database.  It will need to
    // inspect all user defined functions also.
    return "";
  }

  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

    //--------------------------------------------------------------------
    // Functions describing which features are supported.

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return true;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    return false;
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    // Is this, for example,  "select * from Part P, Customer as C where ... '
    // If it is then yes.
    return true;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    // This is easily tested as,
    //   SELECT * FROM Test1 AS Test2;
    // where 'Test2' is a valid table in the database.
    return false;
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    // This doesn't make sense - returning false to be safe,
    return false;
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    // Of course... :-)
    return true;
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    // I need to check this...
    // What's minimum SQL as defined in ODBC?
    return false;
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    // What's core SQL as defined in ODBC?
    return false;
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    // Not yet...
    return false;
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    // ?
    return false;
  }

  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  public String getSchemaTerm() throws SQLException {
    return "Schema";
  }

  public String getProcedureTerm() throws SQLException {
    return "Procedure";
  }

  public String getCatalogTerm() throws SQLException {
    return "Catalog";
  }

  public boolean isCatalogAtStart() throws SQLException {
    // Don't support catalogs
    return false;
  }

  public String getCatalogSeparator() throws SQLException {
    return "";
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    // When we support procedures then true...
    return true;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsPositionedDelete() throws SQLException {
    // I'm guessing this comes with updatable result sets.
    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    // I'm guessing this comes with updatable result sets.
    return false;
  }

  public boolean supportsSelectForUpdate() throws SQLException {
    // I'm not sure what this is,
    return false;
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    // Not yet,
    return false;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    // No 'exists' yet,
    return false;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    // I don't think so,
    return false;
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    // Not yet,
    return false;
  }

  public boolean supportsUnion() throws SQLException {
    return false;
  }

  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    // Sort of, a result set can remain open after a commit...
    return true;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    // Sort of, a result set can remain open after a commit...
    return true;
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;
  }

    //----------------------------------------------------------------------
    // The following group of methods exposes various limitations
    // based on the target database with the current driver.
    // Unless otherwise specified, a result of zero means there is no
    // limit, or the limit is not known.

  public int getMaxBinaryLiteralLength() throws SQLException {
    // No binary literals yet,
    return 0;
  }

  public int getMaxCharLiteralLength() throws SQLException {
    // This is an educated guess...
    return 32768;
  }

  public int getMaxColumnNameLength() throws SQLException {
    // Need to work out this limitation for real.  There may be no limit.
    return 256;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    // The limit is determined by number of columns in select.
    return getMaxColumnsInSelect();
  }

  public int getMaxColumnsInIndex() throws SQLException {
    // No explicit indexing syntax,
    return 1;
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    // The limit is determined by number of columns in select.
    return getMaxColumnsInSelect();
  }

  public int getMaxColumnsInSelect() throws SQLException {
    // Probably limited only by resources...
    return 4096;
  }

  public int getMaxColumnsInTable() throws SQLException {
    // Probably limited only by resources...
    return 4096;
  }

  public int getMaxConnections() throws SQLException {
    // Maybe we need to do some benchmarks for this.  There's certainly no
    // limit with regard to licensing.
    return 8000;
  }

  public int getMaxCursorNameLength() throws SQLException {
    // Cursors not supported,
    return 0;
  }

  public int getMaxIndexLength() throws SQLException {
    // No explicit indexing syntax,
    return 0;
  }

  public int getMaxSchemaNameLength() throws SQLException {
    // Schema not supported,
    return 0;
  }

  public int getMaxProcedureNameLength() throws SQLException {
    // Procedures not supported,
    return 0;
  }

  public int getMaxCatalogNameLength() throws SQLException {
    // Catalog not supported,
    return 0;
  }

  public int getMaxRowSize() throws SQLException {
    // Only limited by resources,
    // Returning 16MB here.
    return 16 * 1024 * 1024;
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    // The size of a UTF-8 string?
    return 60000;
  }

  public int getMaxStatements() throws SQLException {
    // No coded limit,
    return 1024;
  }

  public int getMaxTableNameLength() throws SQLException {
    // This is what's in DatabaseConstants.
    // However, this limitation should no longer be applicable!
    return 50;
  }

  public int getMaxTablesInSelect() throws SQLException {
    // Should be no limit but we'll put an arbitary limit anyway...
    return 512;
  }

  public int getMaxUserNameLength() throws SQLException {
    // This is what's in DatabaseConstants.
    // However, this limitation should no longer be applicable!
    return 50;
  }

    //----------------------------------------------------------------------

  public int getDefaultTransactionIsolation() throws SQLException {
    // Currently the only supported isolation level
    return Connection.TRANSACTION_SERIALIZABLE;
  }

  public boolean supportsTransactions() throws SQLException {
    // As of version 0.88, yes!
    return true;
  }

  public boolean supportsTransactionIsolationLevel(int level)
              throws SQLException {
    return (level == Connection.TRANSACTION_SERIALIZABLE);
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions()
               throws SQLException {
    return true;
  }

  public boolean supportsDataManipulationTransactionsOnly()
              throws SQLException {
    return false;
  }

  public boolean dataDefinitionCausesTransactionCommit()
              throws SQLException {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions()
              throws SQLException {
    return false;
  }

  public ResultSet getProcedures(String catalog, String schemaPattern,
      String procedureNamePattern) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(
                            "SHOW JDBC_PROCEDURES ( ?, ?, ? )");
    statement.setString(1, catalog);
    statement.setString(2, schemaPattern);
    statement.setString(3, procedureNamePattern);

    return statement.executeQuery();
  }

  public ResultSet getProcedureColumns(String catalog,
      String schemaPattern,
      String procedureNamePattern,
      String columnNamePattern) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(
                 "SHOW JDBC_PROCEDURE_COLUMNS ( ?, ?, ?, ? )");
    statement.setString(1, catalog);
    statement.setString(2, schemaPattern);
    statement.setString(3, procedureNamePattern);
    statement.setString(4, columnNamePattern);

    return statement.executeQuery();
  }

  public ResultSet getTables(String catalog, String schemaPattern,
    String tableNamePattern, String[] types) throws SQLException {

//    System.out.println(catalog);
//    System.out.println(schemaPattern);
//    System.out.println(tableNamePattern);
//    System.out.println(types);

    // The JDBC spec actually doesn't specify this but some tools appear to
    // think passing 'null' for schema and table will include all schema in
    // the result.
    if (tableNamePattern == null) {
      tableNamePattern = "%";
    }
    if (schemaPattern == null) {
      schemaPattern = "%";
    }

    // The 'types' argument
    String type_part = "";
    int type_size = 0;
    if (types != null && types.length > 0) {
      StringBuffer buf = new StringBuffer();
      buf.append("      AND \"sUSRTableInfo.type\" IN ( ");
      for (int i = 0; i < types.length - 1; ++i) {
        buf.append("?, ");
      }
      buf.append("? ) \n");
      type_size = types.length;
      type_part = new String(buf);
    }

    // Create the statement

    PreparedStatement stmt = connection.prepareStatement(
        "   SELECT NULL AS \"TABLE_CAT\", \n" +
        "          \"schema\" AS \"TABLE_SCHEM\", \n" +
        "          \"name\" AS \"TABLE_NAME\", \n" +
        "          \"type\" AS \"TABLE_TYPE\", \n" +
        "          \"other\" AS \"REMARKS\" \n" +
        "     FROM \"SYS_INFO.sUSRTableInfo\" \n" +
        "    WHERE \"sUSRTableInfo.schema\" LIKE ? \n" +
        "      AND \"sUSRTableInfo.name\" LIKE ? \n" +
        type_part
    );
    stmt.setString(1, schemaPattern);
    stmt.setString(2, tableNamePattern);
    if (type_size > 0) {
      for (int i = 0; i < type_size; ++i) {
        stmt.setString(3 + i, types[i]);
      }
    }

    return stmt.executeQuery();

//    PreparedStatement stmt = connection.prepareStatement(
//        "   SELECT NULL AS \"TABLE_CAT\", " +
//        "          \"schema\" AS \"TABLE_SCHEM\", " +
//        "          \"table\" AS \"TABLE_NAME\", " +
//        "          'TABLE' AS \"TABLE_TYPE\", " +
//        "          '' AS \"REMARKS\" " +
//        "     FROM SYS_INFO.sUSRTableColumns " +
//        "    WHERE sUSRTableColumns.schema LIKE ? " +
//        "      AND sUSRTableColumns.table LIKE ? " +
//        " GROUP BY \"TABLE_SCHEM\", \"TABLE_NAME\" " +
//        " ORDER BY \"TABLE_TYPE\", \"TABLE_SCHEM\", \"TABLE_NAME\" "
//    );
//    stmt.setString(1, schemaPattern);
//    stmt.setString(2, tableNamePattern);
//    return stmt.executeQuery();

  }

  public ResultSet getSchemas() throws SQLException {
    Statement statement = connection.createStatement();
    return statement.executeQuery(
       "    SELECT \"name\" AS \"TABLE_SCHEM\" " +
       "      FROM SYS_INFO.sUSRSchemaInfo " +
       "  ORDER BY \"TABLE_SCHEM\" "
    );
//    Statement statement = connection.createStatement();
//    return statement.executeQuery("SHOW JDBC_SCHEMAS");
  }

  public ResultSet getCatalogs() throws SQLException {
    Statement statement = connection.createStatement();
    return statement.executeQuery("SHOW JDBC_CATALOGS");
  }

  public ResultSet getTableTypes() throws SQLException {
    Statement statement = connection.createStatement();
    return statement.executeQuery("SHOW JDBC_TABLE_TYPES");
  }

  public ResultSet getColumns(String catalog, String schemaPattern,
    String tableNamePattern, String columnNamePattern)
          throws SQLException {

    // The JDBC spec actually doesn't specify this but some tools appear to
    // think passing 'null' for schema and table will include all schema in
    // the result.
    if (tableNamePattern == null) {
      tableNamePattern = "%";
    }
    if (schemaPattern == null) {
      schemaPattern = "%";
    }
    if (columnNamePattern == null) {
      columnNamePattern = "%";
    }

    PreparedStatement statement = connection.prepareStatement(
"  SELECT NULL AS \"TABLE_CAT\",\n" +
"         \"schema\" AS \"TABLE_SCHEM\",\n" +
"         \"table\" AS \"TABLE_NAME\",\n" +
"         \"column\" AS \"COLUMN_NAME\",\n" +
"         \"sql_type\" AS \"DATA_TYPE\",\n" +
"         \"type_desc\" AS \"TYPE_NAME\",\n" +
"         IF(\"size\" = -1, 1024, \"size\") AS \"COLUMN_SIZE\",\n" +
"         NULL AS \"BUFFER_LENGTH\",\n" +
"         \"scale\" AS \"DECIMAL_DIGITS\",\n" +
"         IF(\"sql_type\" = -7, 2, 10) AS \"NUM_PREC_RADIX\",\n" +
"         IF(\"not_null\", 0, 1) AS \"NULLABLE\",\n" +
"         '' AS \"REMARKS\",\n" +
"         \"default\" AS \"COLUMN_DEF\",\n" +
"         NULL AS \"SQL_DATA_TYPE\",\n" +
"         NULL AS \"SQL_DATETIME_SUB\",\n" +
"         IF(\"size\" = -1, 1024, \"size\") AS \"CHAR_OCTET_LENGTH\",\n" +
"         \"seq_no\" + 1 AS \"ORDINAL_POSITION\",\n" +
"         IF(\"not_null\", 'NO', 'YES') AS \"IS_NULLABLE\"\n" +
"    FROM SYS_INFO.sUSRTableColumns\n" +
"   WHERE \"schema\" LIKE ?\n" +
"     AND \"table\" LIKE ?\n" +
"     AND \"column\" LIKE ?\n" +
"ORDER BY \"TABLE_SCHEM\", \"TABLE_NAME\", \"ORDINAL_POSITION\""
);

//    PreparedStatement statement = connection.prepareStatement(
//                                 "SHOW JDBC_COLUMNS ( ?, ?, ?, ? ) ");
//    statement.setString(1, catalog);
    statement.setString(1, schemaPattern);
    statement.setString(2, tableNamePattern);
    statement.setString(3, columnNamePattern);

//    System.out.println(statement);

    return statement.executeQuery();
  }

  public ResultSet getColumnPrivileges(String catalog, String schema,
    String table, String columnNamePattern) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(
                             "SHOW JDBC_COLUMN_PRIVILEGES ( ?, ?, ?, ? )");
    statement.setString(1, catalog);
    statement.setString(2, schema);
    statement.setString(3, table);
    statement.setString(4, columnNamePattern);

    return statement.executeQuery();
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
        String tableNamePattern) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(
                             "SHOW JDBC_TABLE_PRIVILEGES ( ?, ?, ? )");
    statement.setString(1, catalog);
    statement.setString(2, schemaPattern);
    statement.setString(3, tableNamePattern);

    return statement.executeQuery();
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema,
    String table, int scope, boolean nullable) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(
                  "SHOW JDBC_BEST_ROW_IDENTIFIER ( ?, ?, ?, ?, ? )");
    statement.setString(1, catalog);
    statement.setString(2, schema);
    statement.setString(3, table);
    statement.setInt(4, scope);
    statement.setBoolean(5, nullable);

    return statement.executeQuery();
  }

  public ResultSet getVersionColumns(String catalog, String schema,
        String table) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(
                           "SHOW JDBC_VERSION_COLUMNS ( ?, ?, ? )");
    statement.setString(1, catalog);
    statement.setString(2, schema);
    statement.setString(3, table);

    return statement.executeQuery();
  }

  public ResultSet getPrimaryKeys(String catalog, String schema,
        String table) throws SQLException {

//    StringBuffer stmt_str = new StringBuffer();
//    stmt_str.append(
//"  SELECT NULL \"TABLE_CAT\",\n" +
//"         \"schema\" \"TABLE_SCHEM\",\n" +
//"         \"table\" \"TABLE_NAME\",\n" +
//"         \"column\" \"COLUMN_NAME\",\n" +
//"         \"seq_no\" \"SEQ_NO\",\n" +
//"         \"name\" \"PK_NAME\"\n" +
//"    FROM SYS_INFO.sUSRPKeyInfo, SYS_INFO.sUSRPrimaryColumns\n" +
//"   WHERE sUSRPKeyInfo.id = sUSRPrimaryColumns.pk_id\n");
//    if (schema != null) {
//      stmt_str.append("     AND \"schema\" = ?\n");
//    }
//    stmt_str.append(
//"     AND \"table\" = ?\n" +
//"ORDER BY \"COLUMN_NAME\"\n");
//
//    PreparedStatement stmt = connection.prepareStatement(new String(stmt_str));
//    if (schema != null) {
//      stmt.setString(1, schema);
//      stmt.setString(2, table);
//    }
//    else {
//      stmt.setString(1, table);
//    }
//    return stmt.executeQuery();

    PreparedStatement stmt = connection.prepareStatement(
"  SELECT NULL \"TABLE_CAT\",\n" +
"         \"schema\" \"TABLE_SCHEM\",\n" +
"         \"table\" \"TABLE_NAME\",\n" +
"         \"column\" \"COLUMN_NAME\",\n" +
"         \"seq_no\" \"SEQ_NO\",\n" +
"         \"name\" \"PK_NAME\"\n" +
"    FROM SYS_INFO.sUSRPKeyInfo, SYS_INFO.sUSRPrimaryColumns\n" +
"   WHERE sUSRPKeyInfo.id = sUSRPrimaryColumns.pk_id\n" +
"     AND ( ? IS NULL OR \"schema\" = ? )\n" +
"     AND \"table\" = ?\n" +
"ORDER BY \"COLUMN_NAME\"\n"
    );
    stmt.setString(1, schema);
    stmt.setString(2, schema);
    stmt.setString(3, table);

    return stmt.executeQuery();

  }

  public ResultSet getImportedKeys(String catalog, String schema,
        String table) throws SQLException {

    PreparedStatement stmt = connection.prepareStatement(
"  SELECT NULL \"PKTABLE_CAT\",\n" +
"         \"sUSRFKeyInfo.ref_schema\" \"PKTABLE_SCHEM\",\n" +
"         \"sUSRFKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
"         \"sUSRForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
"         NULL \"FKTABLE_CAT\",\n" +
"         \"sUSRFKeyInfo.schema\" \"FKTABLE_SCHEM\",\n" +
"         \"sUSRFKeyInfo.table\" \"FKTABLE_NAME\",\n" +
"         \"sUSRForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
"         \"sUSRForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
"         I_FRULE_CONVERT(\"sUSRFKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
"         I_FRULE_CONVERT(\"sUSRFKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
"         \"sUSRFKeyInfo.name\" \"FK_NAME\",\n" +
"         NULL \"PK_NAME\",\n" +
"         \"sUSRFKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
"    FROM \"SYS_INFO.sUSRFKeyInfo\",\n" +
"         \"SYS_INFO.sUSRForeignColumns\"\n" +
"   WHERE ( ? IS NULL OR sUSRFKeyInfo.schema = ? )\n" +
"     AND sUSRFKeyInfo.table = ?\n" +
"     AND sUSRFKeyInfo.id = sUSRForeignColumns.fk_id\n" +
"ORDER BY \"FKTABLE_SCHEM\", \"FKTABLE_NAME\", \"KEY_SEQ\"\n"
    );
    stmt.setString(1, schema);
    stmt.setString(2, schema);
    stmt.setString(3, table);

    return stmt.executeQuery();
  }

  public ResultSet getExportedKeys(String catalog, String schema,
        String table) throws SQLException {

    PreparedStatement stmt = connection.prepareStatement(
"  SELECT NULL \"PKTABLE_CAT\",\n" +
"         \"sUSRFKeyInfo.ref_schema\" \"PKTABLE_SCHEM\",\n" +
"         \"sUSRFKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
"         \"sUSRForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
"         NULL \"FKTABLE_CAT\",\n" +
"         \"sUSRFKeyInfo.schema\" \"FKTABLE_SCHEM\",\n" +
"         \"sUSRFKeyInfo.table\" \"FKTABLE_NAME\",\n" +
"         \"sUSRForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
"         \"sUSRForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
"         I_FRULE_CONVERT(\"sUSRFKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
"         I_FRULE_CONVERT(\"sUSRFKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
"         \"sUSRFKeyInfo.name\" \"FK_NAME\",\n" +
"         NULL \"PK_NAME\",\n" +
"         \"sUSRFKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
"    FROM \"SYS_INFO.sUSRFKeyInfo\",\n" +
"         \"SYS_INFO.sUSRForeignColumns\"\n" +
"   WHERE ( ? IS NULL OR sUSRFKeyInfo.ref_schema = ? )\n" +
"     AND sUSRFKeyInfo.ref_table = ?\n" +
"     AND sUSRFKeyInfo.id = sUSRForeignColumns.fk_id\n" +
"ORDER BY \"FKTABLE_SCHEM\", \"FKTABLE_NAME\", \"KEY_SEQ\"\n"
    );
    stmt.setString(1, schema);
    stmt.setString(2, schema);
    stmt.setString(3, table);

    return stmt.executeQuery();
  }

  public ResultSet getCrossReference(
    String primaryCatalog, String primarySchema, String primaryTable,
    String foreignCatalog, String foreignSchema, String foreignTable
    ) throws SQLException {

    PreparedStatement stmt = connection.prepareStatement(
"  SELECT NULL \"PKTABLE_CAT\",\n" +
"         \"sUSRFKeyInfo.ref_schema\" \"PKTABLE_SCHEM\",\n" +
"         \"sUSRFKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
"         \"sUSRForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
"         NULL \"FKTABLE_CAT\",\n" +
"         \"sUSRFKeyInfo.schema\" \"FKTABLE_SCHEM\",\n" +
"         \"sUSRFKeyInfo.table\" \"FKTABLE_NAME\",\n" +
"         \"sUSRForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
"         \"sUSRForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
"         I_FRULE_CONVERT(\"sUSRFKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
"         I_FRULE_CONVERT(\"sUSRFKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
"         \"sUSRFKeyInfo.name\" \"FK_NAME\",\n" +
"         NULL \"PK_NAME\",\n" +
"         \"sUSRFKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
"    FROM \"SYS_INFO.sUSRFKeyInfo\",\n" +
"         \"SYS_INFO.sUSRForeignColumns\"\n" +
"   WHERE ( ? IS NULL OR sUSRFKeyInfo.ref_schema = ? )\n" +
"     AND sUSRFKeyInfo.ref_table = ?\n" +
"     AND ( ? IS NULL OR sUSRFKeyInfo.schema = ? )\n" +
"     AND sUSRFKeyInfo.table = ?\n" +
"     AND sUSRFKeyInfo.id = sUSRForeignColumns.fk_id\n" +
"ORDER BY \"FKTABLE_SCHEM\", \"FKTABLE_NAME\", \"KEY_SEQ\"\n"
    );
    stmt.setString(1, primarySchema);
    stmt.setString(2, primarySchema);
    stmt.setString(3, primaryTable);
    stmt.setString(4, foreignSchema);
    stmt.setString(5, foreignSchema);
    stmt.setString(6, foreignTable);

    return stmt.executeQuery();
  }

  public ResultSet getTypeInfo() throws SQLException {
    return connection.createStatement().executeQuery(
                                   "SELECT * FROM SYS_INFO.sUSRSQLTypeInfo");
  }

  public ResultSet getIndexInfo(String catalog, String schema, String table,
      boolean unique, boolean approximate)
          throws SQLException {

    PreparedStatement statement = connection.prepareStatement(
                     "SHOW JDBC_INDEX_INFO ( ?, ?, ?, ?, ? )");
    statement.setString(1, catalog);
    statement.setString(2, schema);
    statement.setString(3, table);
    statement.setBoolean(4, unique);
    statement.setBoolean(5, approximate);

    return statement.executeQuery();
  }

//#IFDEF(JDBC2.0)

    //--------------------------JDBC 2.0-----------------------------

  public boolean supportsResultSetType(int type) throws SQLException {
    return (type == ResultSet.TYPE_FORWARD_ONLY ||
            type == ResultSet.TYPE_SCROLL_INSENSITIVE);
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
    if (type == ResultSet.TYPE_SCROLL_INSENSITIVE &&
        concurrency == ResultSet.CONCUR_READ_ONLY) {
      return true;
    }
    return false;
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  public ResultSet getUDTs(String catalog, String schemaPattern,
          String typeNamePattern, int[] types)
      throws SQLException {

    String where_clause = "true";
    if (types != null) {
      for (int i = 0; i < types.length; ++i) {

        int t = types[i];
        String tstr = "JAVA_OBJECT";
        if (t == Types.STRUCT) {
          tstr = "STRUCT";
        }
        else if (t == Types.DISTINCT) {
          tstr = "DISTINCT";
        }

        if (i != 0) {
          where_clause += " AND";
        }
        where_clause += " DATA_TYPE = '" +
                        MckoiConnection.quote(tstr) + "'";
      }
    }

    PreparedStatement statement = connection.prepareStatement(
                 "SHOW JDBC_UDTS ( ?, ?, ? ) WHERE " + where_clause);
    statement.setString(1, catalog);
    statement.setString(2, schemaPattern);
    statement.setString(3, typeNamePattern);

    return statement.executeQuery();
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

//#ENDIF

//#IFDEF(JDBC3.0)

  // ------------------- JDBC 3.0 -------------------------

  public boolean supportsSavepoints() throws SQLException {
    // Currently no
    return false;
  }

  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                 String typeNamePattern) throws SQLException {
    throw new SQLException("Not Supported");
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern,
                                String tableNamePattern) throws SQLException {
    throw new SQLException("Not Supported");
  }

  public ResultSet getAttributes(String catalog, String schemaPattern,
                      String typeNamePattern, String attributeNamePattern)
                                                         throws SQLException {
    throw new SQLException("Not Supported");
  }

  public boolean supportsResultSetHoldability(int holdability)
                                                         throws SQLException {
    return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  public int getResultSetHoldability() throws SQLException {
    // Eh?  This is in ResultSetMetaData also?  An error in the spec or is
    // this the *default* holdability of a result set?
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    throw new SQLException("Not Supported");
  }

  public int getDatabaseMinorVersion() throws SQLException {
    throw new SQLException("Not Supported");
  }

  public int getJDBCMajorVersion() throws SQLException {
    return 3;
  }

  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  public int getSQLStateType() throws SQLException {
    // ?
    throw new SQLException("Not Supported");
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    // Doesn't matter because this is not supported.
    throw new SQLException("Not Supported");
  }

  public boolean supportsStatementPooling() throws SQLException {
    return true;
  }

//#ENDIF

}
