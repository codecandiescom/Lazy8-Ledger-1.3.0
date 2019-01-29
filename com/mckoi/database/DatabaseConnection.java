/**
 * com.mckoi.database.DatabaseConnection  21 Nov 2000
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

import com.mckoi.debug.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.math.BigDecimal;

/**
 * An object that represents a connection to a Database.  This object handles
 * all transactional queries and modifications to the database.
 *
 * @author Tobias Downer
 */

public class DatabaseConnection implements TriggerListener {

  /**
   * The User that this connection has been made by.
   */
  private User user;

  /**
   * The Database object that this connection is on.
   */
  private Database database;

  /**
   * The DebugLogger object that we can use to log messages to.
   */
  private DebugLogger logger;

  /**
   * A loop-back object that is managing this connection.  This typically is
   * the session protocol.  This is notified of all connection events, such as
   * triggers.
   */
  private CallBack call_back;

  /**
   * The locking mechanism within this connection.
   */
  private LockingMechanism locking_mechanism;

  /**
   * The TableDataConglomerate object that is used for transactional access
   * to the data.
   */
  private TableDataConglomerate conglomerate;

  /**
   * The current Transaction that this connection is operating within.
   */
  private Transaction transaction;

  /**
   * The current java.sql.Connection object that can be used to access the
   * transaction internally.
   */
  private java.sql.Connection jdbc_connection;

  /**
   * A HashMap of DataTable objects that have been created within this
   * connection.
   */
  private HashMap tables_cache;

  /**
   * A buffer of triggers.  This contains triggers that can't fire until
   * the current transaction has closed.
   */
  private ArrayList trigger_event_buffer;

  /**
   * If this is true then the database connection is in 'auto-commit' mode.
   * This implies a COMMIT instruction is executed after every complete
   * statement in the language grammar.  By default this is true.
   */
  private boolean auto_commit;

  /**
   * The current transaction isolation level this connect is operating under.
   * 1 = READ UNCOMMITTED, 2 = READ COMMITTED, 3 = REPEATABLE READ,
   * 4 = SERIALIZABLE.
   */
  private int transaction_isolation;

  /**
   * The name of the schema that this connection is currently in.  If the
   * schema is "" then this connection is in the default schema (effectively
   * no schema).
   */
  private String current_schema;

  /**
   * The GrantManager object for this connection.
   */
  private GrantManager grant_manager;

  // ----- Local flags -----

  /**
   * True if transactions through this connection generate an error when
   * there is a dirty select on a table.
   */
  private boolean error_on_dirty_select;

  /**
   * True if this connection resolves identifiers case insensitive.
   */
  private boolean case_insensitive_identifiers;



  /**
   * (package protected) Constructs the connection.
   */
  DatabaseConnection(Database database, User user, CallBack call_back) {
    this.database = database;
    this.user = user;
    this.logger = database.Debug();
    this.call_back = call_back;
    this.conglomerate = database.getConglomerate();
    this.locking_mechanism = new LockingMechanism(Debug());
    this.trigger_event_buffer = new ArrayList();
    tables_cache = new HashMap();
    auto_commit = true;

    current_schema = Database.DEFAULT_SCHEMA;
    this.grant_manager = new GrantManager(this);

    error_on_dirty_select =
                          database.getSystem().transactionErrorOnDirtySelect();
    case_insensitive_identifiers = database.getSystem().ignoreIdentifierCase();
  }

  /**
   * Returns the transaction.  If 'transaction' is null then it opens a
   * new transaction within the conglomerate.
   */
  private Transaction getTransaction() {
    synchronized (this) {
      if (transaction == null) {
        transaction = conglomerate.createTransaction();
        transaction.setErrorOnDirtySelect(error_on_dirty_select);
        transaction.setSecondaryInternalTableInfo(
                                        new ConnectionInternalTableInfo());
      }
    }
    return transaction;
  }

  /**
   * Returns a java.sql.Connection object that can be used as a JDBC
   * interface to access the current transaction of this DatabaseConnection.
   * <p>
   * There are a few important considerations when using the JDBC connection;
   * <ul>
   *   <li>The returned Connection does not allow auto-commit to be set.  It
   *       is intended to be used to issue commands to this
   *       DatabaseConnection from inside a transaction so auto-commit does
   *       not make sense.
   *   <li>The returned object must only be accessed from the same worker
   *       thread that is currently accessing this DatabaseConnection.  The
   *       returned Connection is <b>NOT</b> multi-thread capable.
   *   <li>The java.sql.Connection returned here is invalidated (disposed) when
   *       the current transaction is closed (committed or rolled back).
   *   <li>This method returns the same java.sql.Connection on multiple calls
   *       to this method (while a transaction is open).
   *   <li>The DatabaseConnection must be locked in EXCLUSIVE mode or the
   *       queries will fail.
   * </ul>
   */
  public java.sql.Connection getJDBCConnection() {
    if (jdbc_connection == null) {
      jdbc_connection = InternalJDBCHelper.createJDBCConnection(this);
    }
    return jdbc_connection;
  }

  /**
   * Returns the DatabaseSystem object for this connection.
   */
  public DatabaseSystem getSystem() {
    return database.getSystem();
  }

  /**
   * Returns the Database object for this connection.
   */
  public Database getDatabase() {
    return database;
  }

  /**
   * Returns the User object for this connection.
   */
  public User getUser() {
    return user;
  }

  /**
   * Returns a DebugLogger object that we can use to log debug messages to.
   */
  public final DebugLogger Debug() {
    return logger;
  }

  /**
   * Returns the GrantManager object that manages grants for tables in the
   * database for this connection/user.
   */
  public GrantManager getGrantManager() {
    return grant_manager;
  }

  /**
   * Sets the auto-commit mode.
   */
  public void setAutoCommit(boolean status) {
    auto_commit = status;
  }

  /**
   * Sets the transaction isolation level from a string.
   */
  public void setTransactionIsolation(String name) {
    if (name.equals("serializable")) {
      transaction_isolation = 4;
    }
    else {
      throw new Error("Can not set transaction isolation to " + name);
    }
  }

  /**
   * Assigns a variable to the expression for this connection.  This is a
   * generic way of setting properties of the connection.  Currently supported
   * variables are;
   * <p>
   * ERROR_ON_DIRTY_SELECT - set to Boolean.TRUE for turning this transaction
   *   conflict off on this connection.
   * CASE_INSENSITIVE_IDENTIFIERS - Boolean.TRUE means the grammar becomes
   *   case insensitive for identifiers resolved by the grammar.
   */
  public void setVar(String name, Expression exp) {
    if (name.toUpperCase().equals("ERROR_ON_DIRTY_SELECT")) {
      error_on_dirty_select = toBooleanValue(exp);
    }
    else if (name.toUpperCase().equals("CASE_INSENSITIVE_IDENTIFIERS")) {
      case_insensitive_identifiers = toBooleanValue(exp);
    }
  }

  /**
   * Evaluates the expression to a boolean value (true or false).
   */
  private static boolean toBooleanValue(Expression exp) {
    return Operator.toBooleanValue(exp.evaluate(null, null, null));
  }

  /**
   * Returns the auto-commit status of this connection.  If this is true then
   * the language layer must execute a COMMIT after every statement.
   */
  public boolean getAutoCommit() {
    return auto_commit;
  }

  /**
   * Returns the transaction isolation level of this connection.
   */
  public int getTransactionIsolation() {
    return transaction_isolation;
  }

  /**
   * Returns the transaction isolation level of this connection as a string.
   */
  public String getTransactionIsolationAsString() {
    int il = getTransactionIsolation();
    if (il == 1) {
      return "read uncommitted";
    }
    else if (il == 2) {
      return "read committed";
    }
    else if (il == 3) {
      return "repeatable read";
    }
    else if (il == 4) {
      return "serializable";
    }
    else {
      return "unknown isolation level";
    }
  }

  /**
   * Returns the name of the schema that this connection is within.
   */
  public String getCurrentSchema() {
    return current_schema;
  }

  /**
   * Returns true if the connection is in case insensitive mode.  In case
   * insensitive mode the case of identifier strings is not important.
   */
  public boolean isInCaseInsensitiveMode() {
    return case_insensitive_identifiers;
  }

  /**
   * Sets the schema that this connection is within.
   */
  public void setCurrentSchema(String current_schema) {
    this.current_schema = current_schema;
  }

  /**
   * Returns the LockingMechanism object that is within the context of this
   * database connection.  This manages read/write locking within this
   * connection.
   */
  public LockingMechanism getLockingMechanism() {
    return locking_mechanism;
  }

  /**
   * Returns a TableName[] array that contains the list of tables that are
   * visible within this transaction.
   * <p>
   * This does NOT filter tables that can not be accessed by this connection.
   * This fact should be determined by another query.
   */
  public TableName[] getTableList() {
    return getTransaction().getTableList();
  }

  /**
   * Returns true if the table exists within this connection transaction.
   */
  public boolean tableExists(String table_name) {
    return tableExists(new TableName(current_schema, table_name));
  }

  /**
   * Returns true if the table exists within this connection transaction.
   */
  public boolean tableExists(TableName table_name) {
    return getTransaction().tableExists(table_name);
  }

  /**
   * Attempts to resolve the given table name to its correct case assuming
   * the table name represents a case insensitive version of the name.  For
   * example, "aPP.CuSTOMer" may resolve to "APP.Customer".  If the table
   * name can not resolve to a valid identifier it returns the input table
   * name, therefore the actual presence of the table should always be
   * checked by calling 'tableExists' after this method returns.
   */
  public TableName tryResolveCase(TableName table_name) {
    return getTransaction().tryResolveCase(table_name);
  }

  /**
   * Resolves a TableName string (eg. 'Customer' 'APP.Customer' ) to a
   * TableName object.  If the schema part of the table name is not present
   * then it is set to the current schema of the database connection.  If the
   * database is ignoring the case then this will correctly resolve the table
   * to the cased version of the table name.
   */
  public TableName resolveTableName(String name) {
    TableName table_name = TableName.resolve(getCurrentSchema(), name);
    if (isInCaseInsensitiveMode()) {
      // Try and resolve the case of the table name,
      table_name = tryResolveCase(table_name);
    }
    return table_name;
  }

  /**
   * Returns the DataTableDef for the table with the given name.
   */
  public DataTableDef getDataTableDef(TableName name) {
    return getTransaction().getTable(name).getDataTableDef();
  }

  /**
   * Returns a DataTable that represents the table from the given schema,
   * name in the database.
   */
  public DataTable getTable(TableName name) {
    MutableTableDataSource table = getTransaction().getTable(name);

    synchronized (tables_cache) {
      DataTable dtable = (DataTable) tables_cache.get(table);
      if (dtable == null) {
        try {
          dtable = new DataTable(this, table);
          tables_cache.put(table, dtable);
        }
        catch (DatabaseException e) {
          Debug().writeException(e);
          throw new Error("Database Exception: " + e.getMessage());
        }
      }
      return dtable;
    }
  }

  /**
   * Returns a DataTable that represents the table with the given name in the
   * database from the current connection schema.
   */
  public DataTable getTable(String table_name) {
    return getTable(new TableName(current_schema, table_name));
  }

  /**
   * Create a new table within the context of the current connection
   * transaction.
   */
  public void createTable(DataTableDef table_def) {
    getTransaction().createTable(table_def);
  }

  /**
   * Create a new table with a starting initial sector size.  This should
   * only be used as very fine grain optimization for creating tables.  If
   * in the future the underlying table model is changed so that the given
   * 'sector_size' value is unapplicable, then the value will be ignored.
   */
  public void createTable(DataTableDef table_def,
                          int data_sector_size, int index_sector_size) {
    getTransaction().createTable(table_def,
                                 data_sector_size, index_sector_size);
  }

  /**
   * Updates a given table within the context of the current connection
   * transaction.
   */
  public void updateTable(DataTableDef table_def) {
    getTransaction().alterTable(table_def.getTableName(), table_def);
  }

  /**
   * Updates a given table within the context of the current connection
   * transaction.  This should only be used as very fine grain optimization
   * for creating tables.If in the future the underlying table model is
   * changed so that the given 'sector_size' value is unapplicable, then the
   * value will be ignored.
   */
  public void updateTable(DataTableDef table_def,
                          int data_sector_size, int index_sector_size) {
    getTransaction().alterTable(table_def.getTableName(), table_def,
                                data_sector_size, index_sector_size);
  }

  /**
   * Given a DataTableDef, if the table exists then it is updated otherwise
   * if it doesn't exist then it is created.
   * <p>
   * This should only be used as very fine grain optimization for creating/
   * altering tables.  If in the future the underlying table model is changed
   * so that the given 'sector_size' value is unapplicable, then the value
   * will be ignored.
   */
  public void alterCreateTable(DataTableDef table_def,
                               int data_sector_size, int index_sector_size) {
    if (!tableExists(table_def.getTableName())) {
      createTable(table_def, data_sector_size, index_sector_size);
    }
    else {
      updateTable(table_def, data_sector_size, index_sector_size);
    }
  }

  /**
   * Given a DataTableDef, if the table exists then it is updated otherwise
   * if it doesn't exist then it is created.
   */
  public void alterCreateTable(DataTableDef table_def) {
    if (!tableExists(table_def.getTableName())) {
      createTable(table_def);
    }
    else {
      updateTable(table_def);
    }
  }

  /**
   * Checks all the rows in the table for immediate constraint violations
   * and when the transaction is next committed check for all deferred
   * constraint violations.  This method is used when the constraints on a
   * table changes and we need to determine if any constraint violations
   * occurred.  To the constraint checking system, this is like adding all
   * the rows to the given table.
   */
  public void checkAllConstraints(TableName table_name) {
    // Assert
    checkExclusive();
    getTransaction().checkAllConstraints(table_name);
  }

  /**
   * Drops a table from within the context of the current connection
   * transaction.
   */
  public void dropTable(String table_name) {
    dropTable(new TableName(current_schema, table_name));
  }

  /**
   * Drops a table from within the context of the current connection
   * transaction.
   */
  public void dropTable(TableName table_name) {
    getTransaction().dropTable(table_name);
  }

  /**
   * Compacts the table with the given name.  Throws an exception if the
   * table doesn't exist.
   */
  public void compactTable(String table_name) {
    compactTable(new TableName(current_schema, table_name));
  }

  /**
   * Compacts the table with the given name.  Throws an exception if the
   * table doesn't exist.
   */
  public void compactTable(TableName table_name) {
    getTransaction().compactTable(table_name);
  }

  /**
   * Adds the given table name to the list of tables that are selected from
   * within the transaction in this connection.
   */
  public void addSelectedFromTable(String table_name) {
    addSelectedFromTable(new TableName(current_schema, table_name));
  }

  /**
   * Adds the given table name to the list of tables that are selected from
   * within the transaction in this connection.
   */
  public void addSelectedFromTable(TableName name) {
    getTransaction().addSelectedFromTable(name);
  }

  /**
   * Returns the next unique identifier for the given table from the schema.
   */
  public long nextUniqueID(TableName name) {
    return getTransaction().nextUniqueID(name);
  }

  /**
   * Returns the next unique identifier for the given table in the connection
   * schema.
   */
  public long nextUniqueID(String table_name) {
    TableName tname = TableName.resolve(current_schema, table_name);
    return nextUniqueID(tname);
  }

  /**
   * Adds a type of trigger for the given trigger source (usually the
   * name of the table).
   * <p>
   * Adds a type of trigger to the given Table.  When the event is fired, the
   * UserCallBack method is notified of the event.
   */
  public void createTrigger(String trigger_name,
                            String trigger_source, int type) {
    database.getTriggerManager().addTriggerListener(
                              this, trigger_name, type, trigger_source, this);
  }

  /**
   * Removes a type of trigger for the given trigger source (usually the
   * name of the table).
   */
  public void deleteTrigger(String trigger_name) {
    database.getTriggerManager().removeTriggerListener(this, trigger_name);
  }

  /**
   * Informs the underlying transaction that a high level transaction event
   * has occurred and should be dispatched to any listeners occordingly.
   */
  public void notifyTriggerEvent(TriggerEvent evt) {
    getTransaction().addTriggerEvent(evt);
  }

  // ---------- Schema management and constraint methods ----------
  // Methods that handle getting/setting schema information such as;
  // * Creating/dropping/querying schema
  // * Creating/dropping/querying constraint information including;
  //     check constraints, unique constraints, primary key constraints,
  //     foreign key constraints, etc.

  /**
   * Changes the default schema to the given schema.
   */
  public void setDefaultSchema(String schema_name) {
    boolean ignore_case = isInCaseInsensitiveMode();
    SchemaDef schema = resolveSchemaCase(schema_name, ignore_case);
    if (schema == null) {
      throw new Error("Schema '" + schema_name + "' does not exist.");
    }
    else {
      // Set the default schema for this connection
      setCurrentSchema(schema.getName());
    }
  }

  // NOTE: These methods are copied because they simply call through to the
  //   Transaction implementation of the method with the same signature.

  private void checkExclusive() {
    if (!getLockingMechanism().isInExclusiveMode()) {
      throw new Error("Assertion failed: Expected to be in exclusive mode.");
    }
  }

  public void createSchema(String name, String type) {
    // Assert
    checkExclusive();
    getTransaction().createSchema(name, type);
  }

  public void dropSchema(String name) {
    // Assert
    checkExclusive();
    getTransaction().dropSchema(name);
  }

  public boolean schemaExists(String name) {
    return getTransaction().schemaExists(name);
  }

  public SchemaDef resolveSchemaCase(String name, boolean ignore_case) {
    return getTransaction().resolveSchemaCase(name, ignore_case);
  }

  public void setPersistentVar(String variable, String value) {
    // Assert
    checkExclusive();
    getTransaction().setPersistentVar(variable, value);
  }

  public String getPersistentVar(String variable) {
    return getTransaction().getPersistantVar(variable);
  }

  public void addUniqueConstraint(TableName table_name, String[] cols,
                                  short deferred, String constraint_name) {
    // Assert
    checkExclusive();
    getTransaction().addUniqueConstraint(table_name, cols,
                                         deferred, constraint_name);
  }

  public void addForeignKeyConstraint(TableName table, String[] cols,
                                      TableName ref_table, String[] ref_cols,
                                      String delete_rule, String update_rule,
                                      short deferred, String constraint_name) {
    // Assert
    checkExclusive();
    getTransaction().addForeignKeyConstraint(table, cols, ref_table, ref_cols,
                                             delete_rule, update_rule,
                                             deferred, constraint_name);
  }

  public void addPrimaryKeyConstraint(TableName table_name, String[] cols,
                                      short deferred, String constraint_name) {
    // Assert
    checkExclusive();
    getTransaction().addPrimaryKeyConstraint(table_name, cols,
                                             deferred, constraint_name);
  }

  public void addCheckConstraint(TableName table_name,
               Expression expression, short deferred, String constraint_name) {
    // Assert
    checkExclusive();
    getTransaction().addCheckConstraint(table_name, expression,
                                        deferred, constraint_name);
  }

  public void dropAllConstraintsForTable(TableName table_name) {
    // Assert
    checkExclusive();
    getTransaction().dropAllConstraintsForTable(table_name);
  }

  public void dropNamedConstraint(TableName table_name,
                                  String constraint_name) {
    // Assert
    checkExclusive();
    getTransaction().dropNamedConstraint(table_name, constraint_name);
  }


  public void dropPrimaryKeyConstraintForTable(
                              TableName table_name, String constraint_name) {
    // Assert
    checkExclusive();
    getTransaction().dropPrimaryKeyConstraintForTable(table_name,
                                                      constraint_name);
  }

  public TableName[] queryTablesRelationallyLinkedTo(TableName table) {
    return getTransaction().queryTablesRelationallyLinkedTo(table);
  }

  public Transaction.ColumnGroup[] queryTableUniqueGroups(
                                               TableName table_name) {
    return getTransaction().queryTableUniqueGroups(table_name);
  }

  public Transaction.ColumnGroup queryTablePrimaryKeyGroup(
                                               TableName table_name) {
    return getTransaction().queryTablePrimaryKeyGroup(table_name);
  }

  public Transaction.CheckExpression[] queryTableCheckExpressions(
                                               TableName table_name) {
    return getTransaction().queryTableCheckExpressions(table_name);
  }

  public Transaction.ColumnGroupReference[] queryTableForeignKeyReferences(
                                                      TableName table_name) {
    return getTransaction().queryTableForeignKeyReferences(table_name);
  }

  public Transaction.ColumnGroupReference[]
               queryTableImportedForeignKeyReferences(TableName table_name) {
    return getTransaction().queryTableImportedForeignKeyReferences(table_name);
  }









  // ---------- Implemented from TriggerListener ----------

  /**
   * Notifies when a trigger has fired for this user.  If there are no open
   * transactions on this connection then we do a straight call back trigger
   * notify.  If there is a transaction open then trigger events are added
   * to the 'trigger_event_buffer' which fires when the connection transaction
   * is committed or rolled back.
   */
  public void fireTrigger(DatabaseConnection database, String trigger_name,
                          TriggerEvent evt) {

    if (this != database) {
      throw new Error("User object mismatch.");
    }

    try {
      // Did we pass in a call back interface?
      if (call_back != null) {
        synchronized (trigger_event_buffer) {
          // If there is no active transaction then fire trigger immediately.
          if (transaction == null) {
            call_back.triggerNotify(trigger_name, evt.getType(),
                                    evt.getSource(), evt.getCount());
          }
          // Otherwise add to buffer
          else {
            trigger_event_buffer.add(trigger_name);
            trigger_event_buffer.add(evt);
          }
        }
      }
    }
    catch (Throwable e) {
      Debug().write(Lvl.ERROR, this, "TRIGGER Exception: " + e.getMessage());
    }
  }

  /**
   * Fires any triggers that are pending in the trigger buffer.
   */
  public void firePendingTriggerEvents() {
    if (trigger_event_buffer.size() > 0) {
      // Post an event that fires the triggers for each listener.
      Runnable runner = new Runnable() {
        public void run() {
          synchronized (trigger_event_buffer) {
            // Fire all pending trigger events in buffer
            for (int i = 0; i < trigger_event_buffer.size(); i += 2) {
              String trigger_name = (String) trigger_event_buffer.get(i);
              TriggerEvent evt =
                          (TriggerEvent) trigger_event_buffer.get(i + 1);
              call_back.triggerNotify(trigger_name, evt.getType(),
                                      evt.getSource(), evt.getCount());
            }
            // Clear the buffer
            trigger_event_buffer.clear();
          }
        }
      };

      // Post the event to go off approx 3ms from now.
      database.postEvent(3, database.createEvent(runner));
    }

  }

  /**
   * Tries to commit the current transaction.  If the transaction can not be
   * committed because there were concurrent changes that interfered with
   * each other then a TransactionError is thrown and the transaction is
   * rolled back.
   * <p>
   * NOTE: It's guarenteed that the transaction will be closed even if a
   *   transaction exception occurs.
   * <p>
   * Synchronization is implied on this method, because the locking mechanism
   *   should be exclusive when this is called.
   */
  public void commit() throws TransactionException {
    if (user != null) {
      user.refreshLastCommandTime();
    }

    // NOTE, always connection exclusive op.
    getLockingMechanism().reset();
    tables_cache.clear();

    if (transaction != null) {
      try {

        // Close and commit the transaction
        transaction.closeAndCommit();

      }
      finally {
        transaction = null;
        // Fire any pending trigger events in the trigger buffer.
        firePendingTriggerEvents();
      }
    }
  }

  /**
   * Rolls back the current transaction operating within this connection.
   * <p>
   * NOTE: It's guarenteed that the transaction will be closed even if an
   *   exception occurs.
   * <p>
   * Synchronization is implied on this method, because the locking mechanism
   *   should be exclusive when this is called.
   */
  public void rollback() {
    if (user != null) {
      user.refreshLastCommandTime();
    }

    // NOTE, always connection exclusive op.
    tables_cache.clear();

    if (transaction != null) {
      getLockingMechanism().reset();
      try {
        transaction.closeAndRollback();
      }
      finally {
        transaction = null;
        // Dispose the jdbc connection
        if (jdbc_connection != null) {
          try {
            InternalJDBCHelper.disposeJDBCConnection(jdbc_connection);
          }
          catch (Throwable e) {
            Debug().write(Lvl.ERROR, this,
                        "Error disposing internal JDBC connection.");
            Debug().writeException(Lvl.ERROR, e);
            // We don't wrap this exception
          }
          jdbc_connection = null;
        }
        // Fire any pending trigger events in the trigger buffer.
        firePendingTriggerEvents();
      }
    }
  }

  /**
   * Closes this database connection.
   */
  public void close() {
    try {
      rollback();
      database.getTriggerManager().clearAllDatabaseConnectionTriggers(this);
    }
    catch (Throwable e) {
    }
    database.connectionClosed(this);
  }


  public void finalize() throws Throwable {
    super.finalize();
    close();
  }

  // ---------- Inner classes ----------

  /**
   * A list of DataTableDef system table definitions for tables internal to
   * the database connection.
   */
  private final static DataTableDef[] INTERNAL_DEF_LIST;

  static {
    INTERNAL_DEF_LIST = new DataTableDef[4];
    INTERNAL_DEF_LIST[0] = GTStatisticsDataSource.DEF_DATA_TABLE_DEF;
    INTERNAL_DEF_LIST[1] = GTConnectionInfoDataSource.DEF_DATA_TABLE_DEF;
    INTERNAL_DEF_LIST[2] = GTCurrentConnectionsDataSource.DEF_DATA_TABLE_DEF;
    INTERNAL_DEF_LIST[3] = GTSQLTypeInfoDataSource.DEF_DATA_TABLE_DEF;
  }

  /**
   * An internal table info object that handles tables internal to a
   * DatabaseConnection object.
   */
  private class ConnectionInternalTableInfo extends InternalTableInfo {

    /**
     * Constructor.
     */
    public ConnectionInternalTableInfo() {
      super(INTERNAL_DEF_LIST);
    }

    // ---------- Implemented ----------

    GTDataSource createInternalTable(int index) {
      if (index == 0) {
        return new GTStatisticsDataSource(DatabaseConnection.this).init();
      }
      else if (index == 1) {
        return new GTConnectionInfoDataSource(DatabaseConnection.this).init();
      }
      else if (index == 2) {
        return new GTCurrentConnectionsDataSource(
                                              DatabaseConnection.this).init();
      }
      else if (index == 3) {
        return new GTSQLTypeInfoDataSource(DatabaseConnection.this).init();
      }
      else {
        throw new RuntimeException();
      }
    }

  }

  /**
   * Call back interface for events that occur within the connection instance.
   */
  public static interface CallBack {

    /**
     * Notifies the callee that a trigger event was fired that this user
     * is listening for.
     */
    void triggerNotify(String trigger_name, int trigger_event,
                       String trigger_source, int fire_count);

  }

}
