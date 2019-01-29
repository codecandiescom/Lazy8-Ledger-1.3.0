/**
 * com.mckoi.database.Database  02 Mar 1998
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

package com.mckoi.database;

import java.sql.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import com.mckoi.debug.*;
import com.mckoi.util.Log;
import com.mckoi.util.Stats;
import com.mckoi.database.jdbc.MSQLException;

/**
 * The representation of a single database in the system.  A database
 * is a set of schema, a set of tables and table definitions of tables in
 * the schema, and a description of the schema.
 * <p>
 * This class encapsulates the top level behaviour of a database.  That is
 * of creating itself, initializing itself, shutting itself down, deleting
 * itself, creating/dropping a table, updating a table.  It is not the
 * responsibility of this class to handle table behaviour above this.  Top
 * level table behaviour is handled by DataTable through the DatabaseConnection
 * interface.
 * <p>
 * The Database object is also responsible for various database management
 * functions such a creating, editing and removing users, triggers, functions
 * and services.
 *
 * @author Tobias Downer
 */

public final class Database implements DatabaseConstants {

  // ---------- Statics ----------

  /**
   * The name of the system schema that contains tables refering to system
   * information.
   */
  public static final String SYSTEM_SCHEMA =
                                        TableDataConglomerate.SYSTEM_SCHEMA;

  /**
   * The name of the default schema.
   */
  public static final String DEFAULT_SCHEMA = "APP";

  /**
   * The password table.
   */
  public static final TableName SYS_PASSWORD =
                               new TableName(SYSTEM_SCHEMA, "sUSRPassword");

  public static final TableName SYS_USERCONNECT =
                        new TableName(SYSTEM_SCHEMA, "sUSRUserConnectPriv");

  public static final TableName SYS_USERPRIV =
                               new TableName(SYSTEM_SCHEMA, "sUSRUserPriv");

  public static final TableName SYS_GRANTS =
                                  new TableName(SYSTEM_SCHEMA, "sUSRGrant");

  /**
   * The system internally generated 'sUSRTableColumns' table.
   */
  public static final TableName SYS_TABLE_COLUMNS =
                           new TableName(SYSTEM_SCHEMA, "sUSRTableColumns");

  /**
   * The system internally generated 'sUSRTableInfo' table.
   */
  public static final TableName SYS_TABLE_INFO =
                              new TableName(SYSTEM_SCHEMA, "sUSRTableInfo");

  /**
   * The system internally generated 'sUSRDatabaseStatistics' table.
   */
  public static final TableName SYS_DB_STATISTICS =
                     new TableName(SYSTEM_SCHEMA, "sUSRDatabaseStatistics");






  // ---------- Members ----------

  /**
   * The DatabaseSystem that this database is part of.
   */
  private DatabaseSystem system;

  /**
   * The name of this database.
   */
  private String name;

  /**
   * The path where the database is stored.
   */
  private File database_path;

  /**
   * The TableDataConglomerate that contains the conglomerate of tables for
   * this database.
   */
  private TableDataConglomerate conglomerate;

  /**
   * A flag which, when set to true, will cause the engine to delete the
   * database from the file system when it is shut down.
   */
  private boolean delete_on_shutdown;

//  /**
//   * A list of DatabaseListener that represents the listeners that are
//   * listening for general database events on this database.
//   */
//  private ArrayList listener_list;


  /**
   * The various log files.
   */
  /**
   * This log file records the DQL commands executed on the server.
   */
  private Log commands_log;

  /**
   * This is set to true when the 'init()' method is first called.
   */
  private boolean initialised = false;

  /**
   * The Constructor.  This takes a directory path in which the database is
   * stored.
   */
  public Database(DatabaseSystem system, String name, File database_path) {
    this.system = system;
    this.delete_on_shutdown = false;
    this.database_path = database_path;
    this.name = name;
    conglomerate = new TableDataConglomerate(system);
//    listener_list = new ArrayList();
  }

  /**
   * Returns the name of this database.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns true if this database is in read only mode.
   */
  public boolean isReadOnly() {
    return getSystem().readOnlyAccess();
  }

  // ---------- Log accesses ----------

  /**
   * Returns the log file where commands are recorded.
   */
  public Log getCommandsLog() {
    return commands_log;
  }

  /**
   * Returns the conglomerate for this database.
   */
  TableDataConglomerate getConglomerate() {
    return conglomerate;
  }

  /**
   * Returns a new DatabaseConnection instance that is used against this
   * database.
   * <p>
   * When a new connection is made on this database, this method is called
   * to create a new DatabaseConnection instance for the connection.  This
   * connection handles all transactional queries and modifications to the
   * database.
   */
  public DatabaseConnection createNewConnection(
                            User user, DatabaseConnection.CallBack call_back) {
    DatabaseConnection connection =
                                new DatabaseConnection(this, user, call_back);
    return connection;
  }

  /**
   * Called by the DatabaseConnection.close() method to close the given
   * connection.  This clears up any state associated with the connection in
   * this database instance.
   */
  public void connectionClosed(DatabaseConnection connection) {
  }

  // ---------- Database user management functions ----------

  /**
   * Tries to authenticate a username/password against this database.  If we
   * fail to authenticate then a 'null' object is returned, otherwise a valid
   * User object is returned.  If a valid object is returned, the user
   * will be logged into the engine via the UserManager object (in
   * DatabaseSystem).  The developer must ensure that 'close' is called before
   * the object is disposed (logs out of the system).
   * <p>
   * This method also returns null if a user exists but was denied access from
   * the given host string.  The given 'host_name' object is formatted in the
   * database host connection encoding.  This method checks all the values
   * from the sUSRUserConnectPriv table for this user for the given protocol.
   * It first checks if the user is specifically DENIED access from the given
   * host.  It then checks if the user is ALLOWED access from the given host.
   * If a host is neither allowed or denied then it is denied.
   */
  public User authenticateUser(String username, String password,
                               String connection_string) {

    // Create a temporary connection for authentication only...
    DatabaseConnection connection = createNewConnection(null, null);
    DatabaseQueryContext context = new DatabaseQueryContext(connection);
    connection.setCurrentSchema(SYSTEM_SCHEMA);
    LockingMechanism locker = connection.getLockingMechanism();
    locker.setMode(LockingMechanism.EXCLUSIVE_MODE);
    try {

      try {
        Connection jdbc = connection.getJDBCConnection();

        // Is the username/password in the database?
        PreparedStatement stmt = jdbc.prepareStatement(
            " SELECT \"UserName\" FROM \"sUSRPassword\" " +
            "  WHERE \"sUSRPassword.UserName\" = ? " +
            "    AND \"sUSRPassword.Password\" = ? ");
        stmt.setString(1, username);
        stmt.setString(2, password);
        ResultSet rs = stmt.executeQuery();
        if (!rs.next()) {
          return null;
        }
        rs.close();
        stmt.close();

        // Now check if this user is permitted to connect from the given
        // host.
        if (userAllowedAccessFromHost(context,
                                      username, connection_string)) {
          // Successfully authenticated...
          User user = new User(username, this,
                             connection_string, System.currentTimeMillis());
          // Log the authenticated user in to the engine.
          system.getUserManager().userLoggedIn(user);
          return user;
        }

        return null;

      }
      catch (SQLException e) {
        if (e instanceof MSQLException) {
          MSQLException msqle = (MSQLException) e;
          Debug().write(Lvl.ERROR, this,
                      msqle.getServerErrorStackTrace());
        }
        Debug().writeException(Lvl.ERROR, e);
        throw new RuntimeException("SQL Error: " + e.getMessage());
      }

//      Table passwords = context.getTable(SYS_PASSWORD);
//
//      // select all rows with 'UserName == username'
//      Table t1 = passwords.simpleSelect(context,
//                new Variable(SYS_PASSWORD, "UserName"), Operator.get("="),
//                new Expression(username));
////      Table t1 = passwords.select(
////                    new Condition("sUSRPassword.UserName", Condition.EQUALS,
////                                    ValueSubstitution.fromObject(username)));
//
//      if (t1.getRowCount() == 1) {
//        // select all rows with 'Password == password'
//        Table t2 = t1.exhaustiveSelect(context, Expression.simple(
//              new Variable(SYS_PASSWORD, "Password"), Operator.get("="),
//              password));
////        Table t2 = t1.select(
////                    new Condition("sUSRPassword.Password", Condition.EQUALS,
////                                    ValueSubstitution.fromObject(password)));
//        if (t2.getRowCount() != 1) {
//          return null;
//        }
//
//        // Now check if this user is permitted to connect from the given
//        // host.
//        if (userAllowedAccessFromHost(context,
//                                      username, connection_string)) {
//          // Successfully authenticated...
//          User user = new User(username, this,
//                             connection_string, System.currentTimeMillis());
//          // Log the authenticated user in to the engine.
//          DatabaseSystem.getUserManager().userLoggedIn(user);
//          return user;
//        }
//
//        return null;
//      }
//      else {
//        return null;
//      }

    }
    finally {
      try {
        // Make sure we commit the connection.
        connection.commit();
      }
      catch (TransactionException e) {
        // Just issue a warning...
        Debug().writeException(Lvl.WARNING, e);
      }
      finally {
        // Guarentee that we unluck from EXCLUSIVE
        locker.finishMode(LockingMechanism.EXCLUSIVE_MODE);
      }
      // And make sure we close (dispose) of the temporary connection.
      connection.close();
    }

  }

  /**
   * Performs check to determine if user is allowed access from the given
   * host.  See the comments of 'authenticateUser' for a description of
   * how this is determined.
   */
  private boolean userAllowedAccessFromHost(DatabaseQueryContext context,
                                  String username, String connection_string) {

    // We always allow access from 'Internal/*' (connections from the
    // 'getConnection' method of a com.mckoi.database.control.DBSystem object)
    // ISSUE: Should we add this as a rule?
    if (connection_string.startsWith("Internal/")) {
      return true;
    }

    // What's the protocol?
    int protocol_host_deliminator = connection_string.indexOf("/");
    String protocol =
                    connection_string.substring(0, protocol_host_deliminator);
    String host = connection_string.substring(protocol_host_deliminator + 1);

    if (Debug().isInterestedIn(Lvl.INFORMATION)) {
      Debug().write(Lvl.INFORMATION, this,
                  "Checking host: protocol = " + protocol +
                  ", host = " + host);
    }

    // The table to check
    DataTable connect_priv = context.getTable(SYS_USERCONNECT);
    Variable un_col = connect_priv.getResolvedVariable(0);
    Variable proto_col = connect_priv.getResolvedVariable(1);
    Variable host_col = connect_priv.getResolvedVariable(2);
    Variable access_col = connect_priv.getResolvedVariable(3);
    // Query: where UserName = %username%
    Table t = connect_priv.simpleSelect(context, un_col, Operator.get("="),
                                        new Expression(username));
    // Query: where %protocol% like Protocol
    Expression exp = Expression.simple(protocol, Operator.get("like"),
                                       proto_col);
    t = t.exhaustiveSelect(context, exp);
    // Query: where %host% like Host
    exp = Expression.simple(host, Operator.get("like"),
                            host_col);
    t = t.exhaustiveSelect(context, exp);

    // Those that are DENY
    Table t2 = t.simpleSelect(context, access_col, Operator.get("="),
                              new Expression("DENY"));
    if (t2.getRowCount() > 0) {
      return false;
    }
    // Those that are ALLOW
    Table t3 = t.simpleSelect(context, access_col, Operator.get("="),
                              new Expression("ALLOW"));
    if (t3.getRowCount() > 0) {
      return true;
    }
    // No DENY or ALLOW entries for this host so deny access.
    return false;

  }

  /**
   * Returns true if a user exists in this database, otherwise returns
   * false.
   * <p>
   * NOTE: Assumes exclusive lock on DatabaseConnection.
   */
  public boolean userExists(DatabaseQueryContext context, String username)
                                                    throws DatabaseException {
    DataTable table = context.getTable(SYS_PASSWORD);
    Variable c1 = table.getResolvedVariable(0);
    // All sUSRPassword where UserName = %username%
    Table t = table.simpleSelect(context, c1, Operator.get("="),
                                 new Expression(username));
    return t.getRowCount() > 0;
  }

  /**
   * Creates and adds a new user to this database.  The User object for
   * the user is returned.
   * <p>
   * If the user is already defined by the database then an error is generated.
   * <p>
   * NOTE: Assumes exclusive lock on DatabaseConnection.
   */
  public User createUser(DatabaseQueryContext context,
                         String username, String password)
                                                    throws DatabaseException {

    // Check the user doesn't already exist
    if (userExists(context, username)) {
      throw new Error("User '" + username + "' already exists.");
    }

    // Some usernames are reserved words
    if (username.equalsIgnoreCase("public")) {
      throw new Error("User '" + username + "' not allowed - reserved.");
    }

    // Usernames starting with @, &, # and $ are reserved for system
    // identifiers
    char c = username.charAt(0);
    if (c == '@' || c == '&' || c == '#' || c == '$') {
      throw new Error("User name can not start with '" + c + "' character.");
    }

    // Add this user to the password table.
    DataTable table = context.getTable(SYS_PASSWORD);
    RowData rdat = new RowData(table);
    rdat.setColumnDataFromObject(0, username);
    rdat.setColumnDataFromObject(1, password);
    table.add(rdat);

    // Return a handle for this user.
    return new User(username, this,
                    "Local/[During com.mckoi.database.Database]",
                    System.currentTimeMillis());
  }

  /**
   * Returns true if the given user belongs to the given group otherwise
   * returns false.
   * <p>
   * NOTE: Assumes exclusive lock on DatabaseConnection.
   */
  public boolean userBelongsToGroup(DatabaseQueryContext context,
                                    String username, String group)
                                                    throws DatabaseException {

    DataTable table = context.getTable(SYS_USERPRIV);
    Variable c1 = table.getResolvedVariable(0);
    Variable c2 = table.getResolvedVariable(1);
    // All sUSRUserPriv where UserName = %username%
    Table t = table.simpleSelect(context, c1, Operator.get("="),
                                 new Expression(username));
    // All from this set where PrivGroupName = %group%
    t = t.simpleSelect(context, c2, Operator.get("="),
                       new Expression(group));
    return t.getRowCount() > 0;
  }

  /**
   * Adds the user to the given group.  This makes an entry in the sUSRUserPriv
   * for this user and the given group.  If the user already belongs to the
   * group then no changes are made.
   * <p>
   * It is important that any security checks for ensuring the grantee is
   * allowed to give the user these privs are preformed before this method is
   * called.
   * <p>
   * NOTE: Assumes exclusive lock on DatabaseConnection.
   */
  public void addUserToGroup(DatabaseQueryContext context,
                             User user, String group)
                                                    throws DatabaseException {
    String username = user.getUserName();
    // Check the user doesn't belong to the group
    if (!userBelongsToGroup(context, username, group)) {
      // The user priv table
      DataTable table = context.getTable(SYS_USERPRIV);
      // Add this user to the 'secure access' priv group.
      RowData rdat = new RowData(table);
      rdat.setColumnDataFromObject(0, username);
      rdat.setColumnDataFromObject(1, group);
      table.add(rdat);
    }
    // NOTE: we silently ignore the case when a user already belongs to the
    //   group.
  }

  /**
   * Grants the given user access to connect to the database from the
   * given host address.  The 'protocol' string is the connecting protocol
   * which can be either 'TCP' or 'Local'.  The 'host' string is the actual
   * host that is connecting.  For example, if the protocol was TCP then
   * the client host may be '127.0.0.1' for localhost.
   * <p>
   * NOTE: Assumes exclusive lock on DatabaseConnection.
   */
  public void grantHostAccessToUser(DatabaseConnection connection,
                                    String user, String protocol, String host)
                                                    throws DatabaseException {
    // The user connect priv table
    DataTable table = connection.getTable(SYS_USERCONNECT);
    // Add the protocol and host to the table
    RowData rdat = new RowData(table);
    rdat.setColumnDataFromObject(0, user);
    rdat.setColumnDataFromObject(1, protocol);
    rdat.setColumnDataFromObject(2, host);
    rdat.setColumnDataFromObject(3, "ALLOW");
    table.add(rdat);

  }










  // ---------- Schema management ----------






  /**
   * Creates the schema information tables introducted in version 0.90.  The
   * schema information tables are;
   * <p>
   * <pre>
   * sUSRPKeyInfo - Primary key constraint information.
   * sUSRFKeyInfo - Foreign key constraint information.
   * sUSRUniqueInfo - Unique set constraint information.
   * sUSRCheckInfo  - Check constraint information.
   * sUSRPrimaryColumns - Primary columns information (refers to PKeyInfo)
   * sUSRUniqueColumns  - Unique columns information (refers to UniqueInfo)
   * sUSRForeignColumns1 - Foreign column information (refers to FKeyInfo)
   * sUSRForeignColumns2 - Secondary Foreign column information (refers to
   *                       FKeyInfo).
   * </pre>
   * These tables handle data for referential integrity.  There are also some
   * additional tables containing general table information.
   * <pre>
   * sUSRTableColumnInfo - All table and column information.
   * </pre>
   * The design is fairly elegant in that we are using the database to store
   * information to maintain referential integrity.
   */
  void createSchemaInfoTables(DatabaseConnection connection)
                                                  throws DatabaseException {

    connection.createSchema(DEFAULT_SCHEMA, "DEFAULT");

    /**
     * The schema layout for these tables;
     *
     *  CREATE TABLE sUSRPKeyInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the primary key constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id),
     *    UNIQUE (schema, table)
     *  );
     *  CREATE TABLE sUSRFKeyInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the foreign key constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    ref_schema  TEXT NOT NULL,  // The name of the schema referenced
     *    ref_table   TEXT NOT NULL,  // The name of the table referenced
     *    update_rule TEXT NOT NULL,  // The rule for updating to table
     *    delete_rule TEXT NOT NULL,  // The rule for deleting from table
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id)
     *  );
     *  CREATE TABLE sUSRUniqueInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the unique constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id)
     *  );
     *  CREATE TABLE sUSRCheckInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the check constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    expression  TEXT NOT NULL,  // The check expression
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id)
     *  );
     *  CREATE TABLE sUSRPrimaryColumns (
     *    pk_id   NUMERIC NOT NULL, // The primary key constraint id
     *    column  TEXT NOT NULL,    // The name of the primary
     *    seq_no  INTEGER NOT NULL, // The sequence number of this constraint
     *    FOREIGN KEY pk_id REFERENCES sUSRPKeyInfo
     *  );
     *  CREATE TABLE sUSRUniqueColumns (
     *    un_id   NUMERIC NOT NULL, // The unique constraint id
     *    column  TEXT NOT NULL,    // The column that is unique
     *    seq_no  INTEGER NOT NULL, // The sequence number of this constraint
     *    FOREIGN KEY un_id REFERENCES sUSRUniqueInfo
     *  );
     *  CREATE TABLE sUSRForeignColumns (
     *    fk_id   NUMERIC NOT NULL, // The foreign key constraint id
     *    fcolumn TEXT NOT NULL,    // The column in the foreign key
     *    pcolumn TEXT NOT NULL,    // The column in the primary key
     *                              // (referenced)
     *    seq_no  INTEGER NOT NULL, // The sequence number of this constraint
     *    FOREIGN KEY fk_id REFERENCES sUSRFKeyInfo
     *  );
     *  CREATE TABLE sUSRSchemaInfo (
     *    id     NUMERIC NOT NULL,
     *    name   TEXT NOT NULL,
     *    type   TEXT,              // Schema type (system, etc)
     *    other  TEXT,
     *
     *    UNIQUE ( name )
     *  );
     *  CREATE TABLE sUSRTableInfo (
     *    id     NUMERIC NOT NULL,
     *    name   TEXT NOT NULL,     // The name of the table
     *    schema TEXT NOT NULL,     // The name of the schema of this table
     *    type   TEXT,              // Table type (temporary, system, etc)
     *    other  TEXT,              // Notes, etc
     *
     *    UNIQUE ( name )
     *  );
     *  CREATE TABLE sUSRColumnColumns (
     *    t_id    NUMERIC NOT NULL,  // Foreign key to sUSRTableInfo
     *    column  TEXT NOT NULL,     // The column name
     *    seq_no  INTEGER NOT NULL,  // The sequence in the table
     *    type    TEXT NOT NULL,     // The SQL type of this column
     *    size    NUMERIC,           // The size of the column if applicable
     *    scale   NUMERIC,           // The scale of the column if applicable
     *    default TEXT NOT NULL,     // The default expression
     *    constraints TEXT NOT NULL, // The constraints of this column
     *    other   TEXT,              // Notes, etc
     *
     *    FOREIGN KEY t_id REFERENCES sUSRTableInfo,
     *    UNIQUE ( t_id, column )
     *  );
     */
//    DataTableDef table;
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRPKeyInfo");
//    table.addColumn(createNumericColumn("id"));
//    table.addColumn(createStringColumn("name"));
//    table.addColumn(createStringColumn("schema"));
//    table.addColumn(createStringColumn("table"));
//    table.addColumn(createNumericColumn("deferred"));
//    connection.alterCreateTable(table, 187, 128);
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRFKeyInfo");
//    table.addColumn(createNumericColumn("id"));
//    table.addColumn(createStringColumn("name"));
//    table.addColumn(createStringColumn("schema"));
//    table.addColumn(createStringColumn("table"));
//    table.addColumn(createStringColumn("ref_schema"));
//    table.addColumn(createStringColumn("ref_table"));
//    table.addColumn(createStringColumn("update_rule"));
//    table.addColumn(createStringColumn("delete_rule"));
//    table.addColumn(createNumericColumn("deferred"));
//    connection.alterCreateTable(table, 187, 128);
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRUniqueInfo");
//    table.addColumn(createNumericColumn("id"));
//    table.addColumn(createStringColumn("name"));
//    table.addColumn(createStringColumn("schema"));
//    table.addColumn(createStringColumn("table"));
//    table.addColumn(createNumericColumn("deferred"));
//    connection.alterCreateTable(table, 187, 128);
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRCheckInfo");
//    table.addColumn(createNumericColumn("id"));
//    table.addColumn(createStringColumn("name"));
//    table.addColumn(createStringColumn("schema"));
//    table.addColumn(createStringColumn("table"));
//    table.addColumn(createStringColumn("expression"));
//    table.addColumn(createNumericColumn("deferred"));
//    connection.alterCreateTable(table, 187, 128);
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRPrimaryColumns");
//    table.addColumn(createNumericColumn("pk_id"));
//    table.addColumn(createStringColumn("column"));
//    table.addColumn(createNumericColumn("seq_no"));
//    connection.alterCreateTable(table, 91, 128);
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRUniqueColumns");
//    table.addColumn(createNumericColumn("un_id"));
//    table.addColumn(createStringColumn("column"));
//    table.addColumn(createNumericColumn("seq_no"));
//    connection.alterCreateTable(table, 91, 128);
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRForeignColumns");
//    table.addColumn(createNumericColumn("fk_id"));
//    table.addColumn(createStringColumn("fcolumn"));
//    table.addColumn(createStringColumn("pcolumn"));
//    table.addColumn(createNumericColumn("seq_no"));
//    connection.alterCreateTable(table, 91, 128);
//
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRSchemaInfo");
//    table.addColumn(createNumericColumn("id"));
//    table.addColumn(createStringColumn("name"));
//    table.addColumn(createStringColumn("type"));
//    table.addColumn(createStringColumn("other"));
//    connection.alterCreateTable(table, 91, 128);
//
////    table = new DataTableDef();
////    table.setSchema(SYSTEM_SCHEMA);
////    table.setName("sUSRTableInfo");
//////    table.addColumn(createNumericColumn("id"));
////    table.addColumn(createStringColumn("schema"));
////    table.addColumn(createStringColumn("name"));
////    table.addColumn(createStringColumn("type"));
////    table.addColumn(createStringColumn("other"));
////    connection.alterCreateTable(table);
//
////    table = new DataTableDef();
////    table.setSchema(SYSTEM_SCHEMA);
////    table.setName("sUSRColumnColumns");
////    table.addColumn(createNumericColumn("t_id"));
////    table.addColumn(createStringColumn("column"));
////    table.addColumn(createNumericColumn("seq_no"));
////    table.addColumn(createStringColumn("type"));
////    table.addColumn(createNumericColumn("size"));
////    table.addColumn(createNumericColumn("scale"));
////    table.addColumn(createStringColumn("default"));
////    table.addColumn(createStringColumn("constraints"));
////    table.addColumn(createStringColumn("other"));
////    connection.alterCreateTable(table);
//
//
//    // Stores misc variables of the database,
//    table = new DataTableDef();
//    table.setSchema(SYSTEM_SCHEMA);
//    table.setName("sUSRDatabaseVars");
//    table.addColumn(createStringColumn("variable"));
//    table.addColumn(createStringColumn("value"));
//    connection.alterCreateTable(table, 91, 128);
//
//    // Insert unique constraints on these tables,
//    // ( I like the elegance of this - the integrity constraint tables define
//    //   constraints for themselves )
//
//    SchemaManager manager = connection.getSchemaManager();
//
//    // Insert the two default schema names,
//    manager.createSchema(SYSTEM_SCHEMA, "SYSTEM");
//    manager.createSchema(DEFAULT_SCHEMA, "DEFAULT");
//
//    // -- Primary Keys --
//    // The 'id' columns are primary keys on all the system tables,
//    final String[] id_col = new String[] { "id" };
//    manager.addPrimaryKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_PK_PK",
//                                    "sUSRPKeyInfo", id_col,
//                                    SchemaManager.INITIALLY_IMMEDIATE);
//    manager.addPrimaryKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_FK_PK",
//                                    "sUSRFKeyInfo", id_col,
//                                    SchemaManager.INITIALLY_IMMEDIATE);
//    manager.addPrimaryKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_UNIQUE_PK",
//                                    "sUSRUniqueInfo", id_col,
//                                    SchemaManager.INITIALLY_IMMEDIATE);
//    manager.addPrimaryKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_CHECK_PK",
//                                    "sUSRCheckInfo", id_col,
//                                    SchemaManager.INITIALLY_IMMEDIATE);
//    manager.addPrimaryKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_SCHEMA_PK",
//                                    "sUSRSchemaInfo", id_col,
//                                    SchemaManager.INITIALLY_IMMEDIATE);
//
//    // -- Foreign Keys --
//    // Create the foreign key references,
//    final String[] fk_col = new String[1];
//    final String[] fk_ref_col = new String[] { "id" };
//    fk_col[0] = "pk_id";
//    manager.addForeignKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_PK_FK",
//                            "sUSRPrimaryColumns", fk_col,
//                            SYSTEM_SCHEMA, "sUSRPKeyInfo", fk_ref_col,
//                            SchemaManager.NO_ACTION, SchemaManager.NO_ACTION,
//                            SchemaManager.INITIALLY_IMMEDIATE);
//    fk_col[0] = "fk_id";
//    manager.addForeignKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_FK_FK",
//                            "sUSRForeignColumns", fk_col,
//                            SYSTEM_SCHEMA, "sUSRFKeyInfo", fk_ref_col,
//                            SchemaManager.NO_ACTION, SchemaManager.NO_ACTION,
//                            SchemaManager.INITIALLY_IMMEDIATE);
//    fk_col[0] = "un_id";
//    manager.addForeignKeyConstraint(SYSTEM_SCHEMA, "SYSTEM_UNIQUE_FK",
//                            "sUSRUniqueColumns", fk_col,
//                            SYSTEM_SCHEMA, "sUSRUniqueInfo", fk_ref_col,
//                            SchemaManager.NO_ACTION, SchemaManager.NO_ACTION,
//                            SchemaManager.INITIALLY_IMMEDIATE);
//
//    // sUSRPKeyInfo 'schema', 'table' column is a unique set,
//    // (You are only allowed one primary key per table).
//    String[] columns = new String[] { "schema", "table" };
//    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_PKEY_ST_UNIQUE",
//                                "sUSRPKeyInfo", columns,
//                                SchemaManager.INITIALLY_IMMEDIATE);
////    // sUSRTableInfo also requires that schema/table is unique,
////    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_TABLE_ST_UNIQUE",
////                                "sUSRTableInfo", columns,
////                                SchemaManager.INITIALLY_IMMEDIATE);
//
//    // sUSRSchemaInfo 'name' column is a unique column,
//    columns = new String[] { "name" };
//    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_SCHEMA_UNIQUE",
//                                "sUSRSchemaInfo", columns,
//                                SchemaManager.INITIALLY_IMMEDIATE);
//    // sUSRPKeyInfo 'name' column is a unique column,
//    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_PKEY_UNIQUE",
//                                "sUSRPKeyInfo", columns,
//                                SchemaManager.INITIALLY_IMMEDIATE);
//    // sUSRFKeyInfo 'name' column is a unique column,
//    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_FKEY_UNIQUE",
//                                "sUSRFKeyInfo", columns,
//                                SchemaManager.INITIALLY_IMMEDIATE);
//    // sUSRUniqueInfo 'name' column is a unique column,
//    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_UNIQUE_UNIQUE",
//                                "sUSRUniqueInfo", columns,
//                                SchemaManager.INITIALLY_IMMEDIATE);
//    // sUSRCheckInfo 'name' column is a unique column,
//    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_CHECK_UNIQUE",
//                                "sUSRCheckInfo", columns,
//                                SchemaManager.INITIALLY_IMMEDIATE);
//
//    // sUSRDatabaseVars 'variable' is unique
//    columns = new String[] { "variable" };
//    manager.addUniqueConstraint(SYSTEM_SCHEMA, "SYSTEM_DATABASEVARS_UNIQUE",
//                                "sUSRDatabaseVars", columns,
//                                SchemaManager.INITIALLY_IMMEDIATE);
//
//
//    // Insert the version number of the database
//    manager.setDatabaseVar("database.version", "1.1");

  }

  /**
   * Creates all the priv/password system tables.
   */
  private void createSystemTables(DatabaseConnection connection)
                                                   throws DatabaseException {

    // --- The user management tables ---
    DataTableDef sUSRPassword = new DataTableDef();
    sUSRPassword.setSchema(SYSTEM_SCHEMA);
    sUSRPassword.setName("sUSRPassword");
    sUSRPassword.addColumn(DataTableColumnDef.createStringColumn("UserName"));
    sUSRPassword.addColumn(DataTableColumnDef.createStringColumn("Password"));

    DataTableDef sUSRUserPriv = new DataTableDef();
    sUSRUserPriv.setSchema(SYSTEM_SCHEMA);
    sUSRUserPriv.setName("sUSRUserPriv");
    sUSRUserPriv.addColumn(DataTableColumnDef.createStringColumn("UserName"));
    sUSRUserPriv.addColumn(
                      DataTableColumnDef.createStringColumn("PrivGroupName"));

    DataTableDef sUSRUserConnectPriv = new DataTableDef();
    sUSRUserConnectPriv.setSchema(SYSTEM_SCHEMA);
    sUSRUserConnectPriv.setName("sUSRUserConnectPriv");
    sUSRUserConnectPriv.addColumn(
                           DataTableColumnDef.createStringColumn("UserName"));
    sUSRUserConnectPriv.addColumn(
                           DataTableColumnDef.createStringColumn("Protocol"));
    sUSRUserConnectPriv.addColumn(
                               DataTableColumnDef.createStringColumn("Host"));
    sUSRUserConnectPriv.addColumn(
                             DataTableColumnDef.createStringColumn("Access"));

    DataTableDef sUSRGrant = new DataTableDef();
    sUSRGrant.setSchema(SYSTEM_SCHEMA);
    sUSRGrant.setName("sUSRGrant");
    sUSRGrant.addColumn(DataTableColumnDef.createStringColumn("priv"));
    sUSRGrant.addColumn(DataTableColumnDef.createNumericColumn("object"));
    sUSRGrant.addColumn(DataTableColumnDef.createStringColumn("param"));
    sUSRGrant.addColumn(DataTableColumnDef.createStringColumn("grantee"));
    sUSRGrant.addColumn(DataTableColumnDef.createStringColumn("grant_option"));
    sUSRGrant.addColumn(DataTableColumnDef.createStringColumn("granter"));

    DataTableDef sUSRService = new DataTableDef();
    sUSRService.setSchema(SYSTEM_SCHEMA);
    sUSRService.setName("sUSRService");
    sUSRService.addColumn(DataTableColumnDef.createStringColumn("name"));
    sUSRService.addColumn(DataTableColumnDef.createStringColumn("class"));
    sUSRService.addColumn(DataTableColumnDef.createStringColumn("type"));

    DataTableDef sUSRFunctionFactory = new DataTableDef();
    sUSRFunctionFactory.setSchema(SYSTEM_SCHEMA);
    sUSRFunctionFactory.setName("sUSRFunctionFactory");
    sUSRFunctionFactory.addColumn(
                              DataTableColumnDef.createStringColumn("name"));
    sUSRFunctionFactory.addColumn(
                             DataTableColumnDef.createStringColumn("class"));
    sUSRFunctionFactory.addColumn(
                              DataTableColumnDef.createStringColumn("type"));

    DataTableDef sUSRFunction = new DataTableDef();
    sUSRFunction.setSchema(SYSTEM_SCHEMA);
    sUSRFunction.setName("sUSRFunction");
    sUSRFunction.addColumn(DataTableColumnDef.createStringColumn("name"));
    sUSRFunction.addColumn(DataTableColumnDef.createStringColumn("class"));
    sUSRFunction.addColumn(DataTableColumnDef.createStringColumn("type"));

    DataTableDef sUSRView = new DataTableDef();
    sUSRView.setSchema(SYSTEM_SCHEMA);
    sUSRView.setName("sUSRView");
    sUSRView.addColumn(DataTableColumnDef.createStringColumn("schema"));
    sUSRView.addColumn(DataTableColumnDef.createStringColumn("name"));
    sUSRView.addColumn(DataTableColumnDef.createBinaryColumn("data"));

    DataTableDef sUSRLabel = new DataTableDef();
    sUSRLabel.setSchema(SYSTEM_SCHEMA);
    sUSRLabel.setName("sUSRLabel");
    sUSRLabel.addColumn(DataTableColumnDef.createNumericColumn("object_type"));
    sUSRLabel.addColumn(DataTableColumnDef.createStringColumn("object_name"));
    sUSRLabel.addColumn(DataTableColumnDef.createStringColumn("label"));

    // Create the tables
    connection.alterCreateTable(sUSRPassword, 91, 128);
    connection.alterCreateTable(sUSRUserPriv, 91, 128);
    connection.alterCreateTable(sUSRUserConnectPriv, 91, 128);
    connection.alterCreateTable(sUSRGrant, 195, 128);
    connection.alterCreateTable(sUSRService, 91, 128);
    connection.alterCreateTable(sUSRFunctionFactory, 91, 128);
    connection.alterCreateTable(sUSRFunction, 91, 128);
    connection.alterCreateTable(sUSRView, 91, 128);
    connection.alterCreateTable(sUSRLabel, 91, 128);

  }

  /**
   * Set up the system table grants.
   * <p>
   * This gives the grantee user full access to sUSRPassword,
   * sUSRUserPriv, sUSRUserConnectPriv, sUSRService, sUSRFunctionFactory,
   * and sUSRFunction.  All other sUSR tables are granted SELECT only.
   * If 'grant_option' is true then the user is given the option to give the
   * grants to other users.
   */
  private void setSystemGrants(DatabaseConnection connection,
              String grantee, boolean grant_option) throws DatabaseException {

    String GRANTER = "@SYSTEM";

    // Add all priv grants to those that the system user is allowed to change
    GrantManager manager = connection.getGrantManager();
    manager.addGrant(Privileges.ALL_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRPassword", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.ALL_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRUserPriv", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.ALL_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRUserConnectPriv", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.ALL_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRService", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.ALL_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRFunctionFactory", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.ALL_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRFunction", grantee, grant_option, GRANTER);

    // Add select grants on all the uneditable system tables,
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRCheckInfo", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRDatabaseVars", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRFKeyInfo", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRForeignColumns", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRGrant", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRPKeyInfo", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRPrimaryColumns", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRSchemaInfo", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRUniqueColumns", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRUniqueInfo", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRView", grantee, grant_option, GRANTER);

    // The internally created table privs
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRDatabaseStatistics", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRTableColumns", grantee, grant_option, GRANTER);
    manager.addGrant(Privileges.READ_PRIVS, GrantManager.TABLE,
             "SYS_INFO.sUSRTableInfo", grantee, grant_option, GRANTER);

  }

  /**
   * Goes through all tables in the database not in the SYS_INFO schema and
   * adds an entry in the grant table for it.
   * <p>
   * This is for converting from a pre-grant database.
   *
   * @param connection the database transaction
   * @param grantee the grantee to apply the table privs to
   * @param grant_option whethet to give the option to grant
   */
  private void convertPreGrant(DatabaseConnection connection,
            String grantee, boolean grant_option) throws DatabaseException {

    String GRANTER = "@SYSTEM";
    GrantManager manager = connection.getGrantManager();
    TableName[] all_tables = connection.getTableList();
    for (int i = 0; i < all_tables.length; ++i) {
      TableName tname = all_tables[i];
      // Table not in the system schema so make a grant for it.
      if (!tname.getSchema().equals(SYSTEM_SCHEMA)) {
        String table = tname.toString();
        manager.addGrant(Privileges.ALL_PRIVS, GrantManager.TABLE,
                         table, grantee, grant_option, GRANTER);
      }
    }
  }

  /**
   * Converts tables from a database that are pre database schema.
   */
  private void convertPreSchema(DatabaseConnection connection)
                                                   throws DatabaseException {

    throw new DatabaseException(
                 "Converting from pre-schema no longer supported.");

//    // For each table set up the unique and primary key tables,
//    TableName[] tables = connection.getTableList();
//    for (int i = 0; i < tables.length; ++i) {
//      if (tables[i].getSchema().equals(DEFAULT_SCHEMA)) {
//        TableName table_name = tables[i];
////        System.out.println("Converting: " + table_name);
//        DataTable table = connection.getTable(table_name);
//        DataTableDef table_def = table.getDataTableDef();
//
//        // Is there a check expression to bring across?
//        Expression exp = table_def.compatCheckExpression();
//        if (exp != null) {
////          System.out.println("  Check constraint found.");
//          connection.addCheckConstraint(table_name,
//                exp, Transaction.INITIALLY_IMMEDIATE, null);
//        }
//
//        // Lists that are generated from the unique and primary key column
//        // lists  from old data.  This is used to convert to the new
//        // constraint list.
//        ArrayList unique_column_list = new ArrayList();
//        ArrayList primary_key_col_list = new ArrayList();
//
//        for (int n = 0; n < table_def.columnCount(); ++n) {
//          DataTableColumnDef column = table_def.columnAt(n);
//          if (column.compatIsUnique()) {
//            unique_column_list.add(column.getName());
//          }
//          if (column.compatIsPrimaryKey()) {
//            primary_key_col_list.add(column.getName());
//          }
//        }
//
//        if (unique_column_list.size() > 0) {
////          System.out.println("  Unique constraint found.");
//          String[] cols = (String[]) unique_column_list.toArray(
//                                   new String[unique_column_list.size()]);
//          connection.addUniqueConstraint(table_name,
//                 cols, Transaction.INITIALLY_IMMEDIATE, null);
//        }
//        if (primary_key_col_list.size() > 0) {
////          System.out.println("  Primary Key constraint found.");
//          String[] cols = (String[]) primary_key_col_list.toArray(
//                                   new String[primary_key_col_list.size()]);
//          connection.addPrimaryKeyConstraint(table_name,
//                 cols, Transaction.INITIALLY_IMMEDIATE, null);
//        }
//
//      }
//    }
  }

  /**
   * Creates the database.  This must be called when the database is first
   * created.  It creates the security tables.  It is very important that
   * this method is called when the database is first created else behaviour
   * is undefined.  It sets up all the important access database tables.
   * It takes two arguements, the 'username' of the root user that initially
   * has access to change all the security tables.  And a 'password' for the
   * root user.  These values must be setup so that an operator can go in
   * and add usernames, passwords and priviledge groups.
   */
  public void create(String username, String password) {
    // Error check to make sure that the 'init()' function has been called
    // before the 'create()'.

//    if (!initialised) {
//      throw new Error("Must call init() method before create()");
//    }
    if (isReadOnly()) {
      throw new RuntimeException("Can not create database in read only mode.");
    }

    if (username == null || username.length() == 0 ||
        password == null || password.length() == 0) {
      throw new RuntimeException(
                               "Must have valid username and password String");
    }

    // If the database path doesn't exist, create it now,
    if (!database_path.exists()) {
      database_path.mkdirs();
    }

    try {
      // Create the conglomerate
      conglomerate.create(database_path, getName());

      DatabaseConnection connection = createNewConnection(null, null);
      DatabaseQueryContext context = new DatabaseQueryContext(connection);
      connection.getLockingMechanism().setMode(
                                            LockingMechanism.EXCLUSIVE_MODE);
      connection.setCurrentSchema(SYSTEM_SCHEMA);

      // The system tables that are present in every conglomerate.
      createSystemTables(connection);

      // Create the schema information tables introduced in version 0.90
      createSchemaInfoTables(connection);

      // Creates the administrator user.
      User user = createUser(context, username, password);
      // This is the admin user so add to the 'secure access' table.
      addUserToGroup(context, user, "secure access");
      // Allow all localhost TCP connections.
      // NOTE: Permissive initial security!
      grantHostAccessToUser(connection, user.getUserName(), "TCP", "%");
      // Allow all Local connections (from within JVM).
      grantHostAccessToUser(connection, user.getUserName(), "Local", "%");

      // Sets the system grants for the administrator and gives grant_option
      setSystemGrants(connection, username, true);

//      DataTable table;
//      String secure_group_name = "secure access";
//
//      // Add user.
//      table = connection.getTable("sUSRPassword");
//      RowData rdat = new RowData(table);
//      rdat.setColumnDataFromObject(0, username);
//      rdat.setColumnDataFromObject(1, password);
//      table.add(rdat);
//
//      // Add this user to the 'secure access' priv group.
//      table = connection.getTable("sUSRUserPriv");
//      rdat = new RowData(table);
//      rdat.setColumnDataFromObject(0, username);
//      rdat.setColumnDataFromObject(1, secure_group_name);
//      table.add(rdat);

      try {
        // Close and commit this transaction.
        connection.commit();
      }
      catch (TransactionException e) {
        Debug().writeException(e);
        throw new Error("Transaction Error: " + e.getMessage());
      }

      connection.getLockingMechanism().finishMode(
                                            LockingMechanism.EXCLUSIVE_MODE);
      connection.close();

      // Close the conglomerate.
      conglomerate.close();

    }
    catch (DatabaseException e) {
      Debug().writeException(e);
      throw new Error("Database Exception: " + e.getMessage());
    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new Error("IO Error: " + e.getMessage());
    }

  }

  /**
   * Initialised the database.  This should be called as part of the database
   * boot up sequence.  It scans through the database path and finds and tables
   * that exist in it.  It then updates the state of this object with the
   * information it finds.
   * <p>
   * It returns the number of tables that are currently within the database.
   * <p>
   * We can use this number to determine whether the database is valid or
   * not.  If it returns that there are 0 tables in the database, the database
   * has not yet been 'create'd, or has been made invalid.
   * <p>
   * NOTE: After this method is called, you will need to enable commands so
   *   the worker thread will dispatch commands on this database.  To do this,
   *   call 'DatabaseSystem.setIsExecutingCommands(true);'
   */
  public void init() throws DatabaseException {

    if (initialised) {
      throw new RuntimeException("Init() method can only be called once.");
    }

    // Reset all session statistics.
    stats().resetSession();

    try {
      File log_path = system.getLogDirectory();
      if (log_path != null) {
        commands_log = new Log(new File(log_path.getPath(), "commands.log"),
                               256 * 1024, 5);
      }
      else {
        commands_log = Log.nullLog();
      }

//      if (getConfigBundle() != null) {
//        String log_path_string = null;
//        try {
//          log_path_string = getConfigBundle().getString("log_path");
//        }
//        catch (MissingResourceException e) { }
//        // Create the logs
//        if (log_path_string != null) {
//          commands_log = new Log(log_path_string +
//                                 File.separator + "commands.log");
//        }
//        else {
//          commands_log = Log.nullLog();
//        }
//      }

      // Open the conglomerate
      conglomerate.open(database_path, getName(), isReadOnly());

      // Check the state of the conglomerate,
      DatabaseConnection connection = createNewConnection(null, null);
      QueryContext context = new DatabaseQueryContext(connection);
      connection.getLockingMechanism().setMode(
                                     LockingMechanism.EXCLUSIVE_MODE);
      if (!connection.tableExists(
                           new TableName(SYSTEM_SCHEMA, "sUSRDatabaseVars"))) {
        if (isReadOnly()) {
          throw new Error("Unable to convert to version 1 because database " +
                          "is in read only mode.");
        }
        // If the sUSRDatabaseVars table doesn't exist then we can assume
        // that we are converting from version 1.
        System.out.println("Converting database to version 1 schema...");

        // First create the new schema tables,
        createSchemaInfoTables(connection);

        // Convert from pre-schema days,
        convertPreSchema(connection);

      }

      // What version is the data?
      DataTable database_vars =
        connection.getTable(new TableName(SYSTEM_SCHEMA, "sUSRDatabaseVars"));
      Map vars = database_vars.toMap();
      String db_version = (String) vars.get("database.version");
      if (db_version.equals("1.0")) {
        // Convert from 1.0
        System.out.println("Converting database to version 1.1 schema...");

        try {
          // Drop the tables that were deprecated
          connection.dropTable(new TableName(SYSTEM_SCHEMA, "sUSRPrivAdd"));
          connection.dropTable(new TableName(SYSTEM_SCHEMA, "sUSRPrivAlter"));
          connection.dropTable(new TableName(SYSTEM_SCHEMA, "sUSRPrivRead"));
        }
        catch (Error e) { /* ignore */ }

        // The system tables that are present in every conglomerate.
        createSystemTables(connection);

        // HACK: Guess the administrator
        DataTable table = connection.getTable(SYS_PASSWORD);
        DataCell user = table.getCellContents(0, 0);
        String admin_user = user.getCell().toString();
        System.out.println("Guessing administrator is: " + admin_user);
        System.out.println("Setting full grant options for administrator.");

        // Sets the system grants for the administrator and gives grant_option
        setSystemGrants(connection, admin_user, true);
        // Sets the table grants for the administrator and gives grant_option
        convertPreGrant(connection, admin_user, true);

        // Allow all localhost TCP connections.
        // NOTE: Permissive initial security!
        grantHostAccessToUser(connection, admin_user, "TCP", "%");
        // Allow all Local connections (from within JVM).
        grantHostAccessToUser(connection, admin_user, "Local", "%");

        // Update to version 1.1
        updateDatabaseVars(context, database_vars, "database.version", "1.1");

      }
      else if (!db_version.equals("1.1")) {
        throw new DatabaseException(
                         "Unrecognised database version '" + db_version +"'");
      }

      // Commit and close the connection.
      connection.commit();
      connection.getLockingMechanism().finishMode(
                                     LockingMechanism.EXCLUSIVE_MODE);
      connection.close();

    }
    catch (TransactionException e) {
      // This would be very strange error to receive for in initializing
      // database...
      throw new Error("Transaction Error: " + e.getMessage());
    }
    catch (IOException e) {
//      Debug().writeException(e);
      throw new Error("IO Error: " + e.getMessage());

//      throw new Error("\n" +
//        "Unable to open the database. Perhaps the data is pre-version 0.88.\n"+
//        "See the README file for instructions for converting pre version \n" +
//        "0.88 database files to the newest format.\n\n" +
//        "IO Message: " + e.getMessage());
    }

    initialised = true;

//    // Call to listeners to notify that the database has started up (so that
//    // they may recreate any results with the new tables).
//    fireDatabaseInit();

  }

  /**
   * Shuts down the database.  It is important that this method is called
   * just before the system closes down.  This should occur before any reboots.
   * It cleanly closes down the database.
   * <p>
   * The main purpose of this method is to write out the relational models to
   * a persistant state.
   * <p>
   * NOTE: Before calling this method, ensure that no commands can be executed
   *   on this database by making a call to
   *   'DatabaseSystem.setIsExecutingCommands(false);'
   */
  public void shutdown() throws DatabaseException {

    if (initialised == false) {
      throw new Error("The database is not initialized.");
    }

//    // Call to listeners to notify that database is shutting down (so that
//    // they may clear up any references to tables that may be keeping).
//    fireDatabaseShutdown();

    try {
      if (delete_on_shutdown == true) {
        // Delete the conglomerate if the database is set to delete on
        // shutdown.
        conglomerate.delete();
      }
      else {
        // Otherwise close the conglomerate.
        conglomerate.close();
      }
    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new Error("IO Error: " + e.getMessage());
    }

    // Shut down the logs...
    if (commands_log != null) {
      commands_log.close();
    }

    initialised = false;

  }

  /**
   * Returns true if the database exists.  This must be called before 'init'
   * and 'create'.  It checks that the database files exist and we can boot
   * into the database.
   */
  public boolean exists() {
    if (initialised == true) {
      throw new RuntimeException(
          "The database is initialised, so no point testing it's existance.");
    }

    try {
      return conglomerate.exists(database_path, getName());
    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new RuntimeException("IO Error: " + e.getMessage());
    }

  }

  /**
   * If the 'deleteOnShutdown' flag is set, the database will delete the
   * database from the file system when it is shutdown.
   * <p>
   * NOTE: Use with care - if this is set to true and the database is shutdown
   *   it will result in total loss of data.
   */
  public final void setDeleteOnShutdown(boolean status) {
    delete_on_shutdown = status;
  }



  /**
   * Returns true if the database is initialised.
   */
  public boolean isInitialized() {
    return initialised;
  }

  /**
   * Copies all the persistent data in this database (the conglomerate) to the
   * given destination path.  This can copy information while the database
   * is 'live'.
   */
  public void liveCopyTo(File path) throws IOException {
    if (initialised == false) {
      throw new Error("The database is not initialized.");
    }

    // Make a copy of the conglomerate,
    conglomerate.liveCopyTo(path);

  }


  // ---------- Database events ----------

//  /**
//   * Adds a listener to the list of database listeners on this database.
//   */
//  public void addDatabaseListener(DatabaseListener listener) {
//    listener_list.add(listener);
//    stats().increment("Database." + name + ".listeners");
//  }
//
//  /**
//   * Removes a listener from the list of database listeners on this database.
//   */
//  public void removeDatabaseListener(DatabaseListener listener) {
//    if (listener_list.remove(listener)) {
//      stats().decrement("Database." + name + ".listeners");
//    }
//  }

//  /**
//   * Fires an event that signifies that the database has shut down.
//   */
//  private void fireDatabaseShutdown() {
//    for (int i = 0; i < listener_list.size(); ++i) {
//      try {
//        DatabaseListener listener = (DatabaseListener) listener_list.get(i);
//        listener.dbShutDown(this);
//      }
//      catch (Throwable e) {
//        Debug().write(Lvl.ERROR, this,
//                    "Exception on database listener during shutdown.");
//        Debug().writeException(e);
//      }
//    }
//  }

//  /**
//   * Fires an event that signifies that the database has initialised.
//   */
//  private void fireDatabaseInit() {
//    for (int i = 0; i < listener_list.size(); ++i) {
//      try {
//        DatabaseListener listener = (DatabaseListener) listener_list.get(i);
//        listener.dbInit(this);
//      }
//      catch (Throwable e) {
//        Debug().write(Lvl.ERROR, this,
//                    "Exception on database listener during init.");
//        Debug().writeException(e);
//      }
//    }
//  }

//  /**
//   * Fires an event that signifies that a table structure has been altered.
//   * Either it has been added, altered or dropped.
//   */
//  private void fireDatabaseTableAltered(String table_name, int alter_type) {
//    for (int i = 0; i < listener_list.size(); ++i) {
//      try {
//        DatabaseListener listener = (DatabaseListener) listener_list.get(i);
//        listener.dbTableAltered(this, table_name, alter_type);
//      }
//      catch (Throwable e) {
//        Debug().write(Lvl.ERROR, this,
//                "Exception on database listener during table alter event.");
//        Debug().writeException(e);
//      }
//    }
//  }


  // ---------- Server side procedures ----------

  /**
   * Resolves a procedure name into a DBProcedure object.  This is used for
   * finding a server side script.  It throws a DatabaseException if the
   * procedure could not be resolved or there was an error retrieving it.
   * <p>
   * ISSUE: Move this to DatabaseSystem?
   */
  public DatabaseProcedure getDBProcedure(
               String procedure_name, DatabaseConnection connection)
                                                     throws DatabaseException {

    // The procedure we are getting.
    DatabaseProcedure procedure_instance;

    // See if we can find the procedure as a .js (JavaScript) file in the
    // procedure resources.
    String p = "/" + procedure_name.replace('.', '/');
    // If procedure doesn't starts with '/com/mckoi/procedure/' then add it
    // on here.
    if (!p.startsWith("/com/mckoi/procedure/")) {
      p = "/com/mckoi/procedure/" + p;
    }
    p = p + ".js";

    // Is there a resource available?
    java.net.URL url = getClass().getResource(p);

    if (url != null) {
      // Create a server side procedure for the .js file
      //   ( This code is not included in the GPL release )
      procedure_instance = null;

    }
    else {
      try {
        // Couldn't find the javascript script, so try and resolve as an
        // actual Java class file.
        // Find the procedure
        Class proc = Class.forName("com.mckoi.procedure." + procedure_name);
        // Instantiate a new instance of the procedure
        procedure_instance = (DatabaseProcedure) proc.newInstance();

        Debug().write(Lvl.INFORMATION, this,
                        "Getting raw Java class file: " + procedure_name);
      }
      catch (IllegalAccessException e) {
        Debug().writeException(e);
        throw new DatabaseException("Illegal Access: " + e.getMessage());
      }
      catch (InstantiationException e) {
        Debug().writeException(e);
        throw new DatabaseException("Instantiation Error: " + e.getMessage());
      }
      catch (ClassNotFoundException e) {
        Debug().writeException(e);
        throw new DatabaseException("Class Not Found: " + e.getMessage());
      }
    }

    // Return the procedure.
    return procedure_instance;

  }

  /**
   * Returns the name of the table within this transaction that is used to
   * determine if the user can add information to this database.
   */
  public String getUserPasswordTable() {
    return "sUSRPassword";
  }

  /**
   * Returns the name of the table within this transaction that is used to
   * determine if the user can add information to this database.
   */
  public String getUserPrivTable() {
    return "sUSRUserPriv";
  }

  // ---------- System access ----------

  /**
   * Returns the DatabaseSystem that this Database is from.
   */
  public final DatabaseSystem getSystem() {
    return system;
  }

  /**
   * Convenience static for accessing the global Stats object.  Perhaps this
   * should be deprecated?
   */
  public final Stats stats() {
    return getSystem().stats();
  }

  /**
   * Returns the DebugLogger implementation from the DatabaseSystem.
   */
  public final DebugLogger Debug() {
    return getSystem().Debug();
  }

  /**
   * Returns the system trigger manager.
   */
  public final TriggerManager getTriggerManager() {
    return getSystem().getTriggerManager();
  }

  /**
   * Returns the system user manager.
   */
  public final UserManager getUserManager() {
    return getSystem().getUserManager();
  }

  /**
   * Creates an event for the database dispatcher.
   */
  public final Object createEvent(Runnable runner) {
    return getSystem().createEvent(runner);
  }

  /**
   * Posts an event on the database dispatcher.
   */
  public final void postEvent(int time, Object event) {
    getSystem().postEvent(time, event);
  }

  /**
   * Returns the system function expression preparer (prepares functions in
   * expressions via the function factory lookup defined).
   */
  public final ExpressionPreparer getFunctionExpressionPreparer() {
    return getSystem().getFunctionExpressionPreparer();
  }

  /**
   * Returns the system DataCellCache.
   */
  public final DataCellCache getDataCellCache() {
    return getSystem().getDataCellCache();
  }

  /**
   * Returns true if the database has shut down.
   */
  public final boolean hasShutDown() {
    return getSystem().hasShutDown();
  }

  /**
   * Starts the shutdown thread which should contain delegates that shut the
   * database and all its resources down.  This method returns immediately.
   */
  public final void startShutDownThread() {
    getSystem().startShutDownThread();
  }

  /**
   * Blocks until the database has shut down.
   */
  public final void waitUntilShutdown() {
    getSystem().waitUntilShutdown();
  }

  /**
   * Executes database functions from the 'run' method of the given runnable
   * instance on the first available worker thread.  All database functions
   * must go through a worker thread.  If we ensure this, we can easily stop
   * all database functions from executing if need be.  Also, we only need to
   * have a certain number of threads active at any one time rather than a
   * unique thread for each connection.
   */
  public final void execute(User user, DatabaseConnection database,
                            Runnable runner) {
    getSystem().execute(user, database, runner);
  }

  /**
   * Registers the delegate that is executed when the shutdown thread is
   * activated.
   */
  public final void registerShutDownDelegate(Runnable delegate) {
    getSystem().registerShutDownDelegate(delegate);
  }

  /**
   * Controls whether the database is allowed to execute commands or not.  If
   * this is set to true, then calls to 'execute' will be executed
   * as soon as there is a free worker thread available.  Otherwise no
   * commands are executed until this is enabled.
   */
  public final void setIsExecutingCommands(boolean status) {
    getSystem().setIsExecutingCommands(status);
  }




  // ---------- Static methods ----------

  /**
   * Given the sUSRDatabaseVars table, this will update the given key with
   * the given value in the table in the current transaction.
   */
  private static void updateDatabaseVars(QueryContext context,
                   DataTable database_vars, String key, String value)
                                                   throws DatabaseException {
    // The references to the first and second column (key/value)
    Variable c1 = database_vars.getResolvedVariable(0); // First column
    Variable c2 = database_vars.getResolvedVariable(1); // Second column

    // Assignment: second column = value
    Assignment assignment = new Assignment(c2, new Expression(value));
    // All rows from database_vars where first column = the key
    Table t1 = database_vars.simpleSelect(context, c1, Operator.get("="),
                                          new Expression(key));

    // Update the variable
    database_vars.update(context, t1, new Assignment[] { assignment }, -1);

  }


  public void finalize() throws Throwable {
    super.finalize();
    if (isInitialized()) {
      System.err.println("Database object was finalized and is initialized!");
    }
  }

}
