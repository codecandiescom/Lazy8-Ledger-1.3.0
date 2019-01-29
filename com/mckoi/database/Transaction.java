/**
 * com.mckoi.database.Transaction  18 Nov 2000
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
import com.mckoi.util.IntegerVector;
import com.mckoi.database.global.ByteLongObject;
import com.mckoi.database.global.ObjectTranslator;
import java.math.BigDecimal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * An open transaction that manages all data access to the
 * TableDataConglomerate.  A transaction sees a view of the data as it was when
 * the transaction was created.  It also sees any modifications that were made
 * within the context of this transaction.  It does not see modifications made
 * by other open transactions.
 * <p>
 * A transaction ends when it is committed or rollbacked.  All operations
 * on this transaction object only occur within the context of this transaction
 * and are not permanent changes to the database structure.  Only when the
 * transaction is committed are changes reflected in the master data.
 *
 * @author Tobias Downer
 */

public class Transaction {

  // ---------- Constraint statics ----------
  // These statics are for managing constraints.

  /**
   * The type of deferrance.
   */
  public static final short INITIALLY_DEFERRED =
                  java.sql.DatabaseMetaData.importedKeyInitiallyDeferred;
  public static final short INITIALLY_IMMEDIATE =
                 java.sql.DatabaseMetaData.importedKeyInitiallyImmediate;
  public static final short NOT_DEFERRABLE =
                      java.sql.DatabaseMetaData.importedKeyNotDeferrable;

  /**
   * Foreign key referential trigger actions.
   */
  public static final String NO_ACTION   = "NO ACTION";
  public static final String CASCADE     = "CASCADE";
  public static final String SET_NULL    = "SET NULL";
  public static final String SET_DEFAULT = "SET DEFAULT";

  // ---------- Member variables ----------

  /**
   * The TableDataConglomerate that this transaction is within the context of.
   */
  private TableDataConglomerate conglomerate;

  /**
   * The id given to this transaction when it was created.  This id is used
   * to determine the committed transactions that can be seen by this
   * transaction.
   */
  private long transaction_id;

  /**
   * The commit_id that represents the id of the last commit that occurred
   * when this transaction was created.
   */
  private long commit_id;

  /**
   * The list of tables that had been committed to the database when this
   * transaction was created.
   * (MasterTableDataSource).
   */
  private ArrayList visible_tables;

  /**
   * An IndexSet for each visible table from the above list.  These objects
   * are used to create index objects at transaction creation time.
   * (IndexSet)
   */
  private ArrayList table_indices;

  /**
   * All tables touched by this transaction.  (MutableTableDataSource)
   */
  private ArrayList touched_tables;

  /**
   * All tables selected from in this transaction.  (MasterTableDataSource)
   */
  private ArrayList selected_from_tables;

  /**
   * A list of MasterTableDataSource and IndexSet representing the list of
   * items that were dropped during this transaction that we need to clean
   * up.
   */
  private ArrayList clean_up_info;

  /**
   * A cache of tables that have been accessed via this transaction.  This is
   * a map of table_name -> MutableTableDataSource.
   */
  private HashMap table_cache;

  /**
   * A journal of high level events that occured during this transaction.  This
   * journal is used to generate trigger events when the transaction is
   * committed.
   */
  private ArrayList trigger_event_list;

  /**
   * The journal for this transaction.  This journal describes all changes
   * made to the database by this transaction.
   */
  private TransactionJournal journal;

  /**
   * The list of InternalTableInfo objects that are containers for generating
   * internal tables (GTDataSource).
   */
  private InternalTableInfo[] internal_tables;

  /**
   * True if this transaction is closed.
   */
  private boolean closed;

  /**
   * True if an error should be generated on a dirty select.
   */
  private boolean transaction_error_on_dirty_select;




  /**
   * Constructs the transaction.
   */
  Transaction(TableDataConglomerate conglomerate, long transaction_id,
              long commit_id, ArrayList visible_tables,
              ArrayList table_indices) {
    this.conglomerate = conglomerate;
    this.transaction_id = transaction_id;
    this.commit_id = commit_id;
    this.visible_tables = visible_tables;
    this.table_indices = table_indices;
    this.table_cache = new HashMap();

    this.touched_tables = new ArrayList();
    this.selected_from_tables = new ArrayList();
    this.trigger_event_list = new ArrayList();
    journal = new TransactionJournal();

    // NOTE: We currently only support 2 - internal tables to the transaction
    //  layer, and internal tables to the database connection layer.
    internal_tables = new InternalTableInfo[2];
    internal_tables[0] = new TransactionInternalTables();

    closed = false;
    getSystem().stats().increment("Transaction.count");

    // Defaults to true (should be changed by called 'setErrorOnDirtySelect'
    // method.
    transaction_error_on_dirty_select = true;
  }

  /**
   * Returns the TransactionSystem that this Transaction is part of.
   */
  public final TransactionSystem getSystem() {
    return conglomerate.getSystem();
  }

  /**
   * Returns a DebugLogger object that we use to log debug messages to.
   */
  public final DebugLogger Debug() {
    return getSystem().Debug();
  }

  /**
   * Sets the secondary internal table container (InternalTableInfo) used to
   * resolve internal tables.  This is intended as a way for the
   * DatabaseConnection layer to plug in internal tables, such as those
   * showing connection statistics, etc.
   */
  void setSecondaryInternalTableInfo(InternalTableInfo info) {
    internal_tables[1] = info;
  }

  /**
   * Returns the 'commit_id' which is the last commit that occured before
   * this transaction was created.
   * <p>
   * NOTE: Don't make this synchronized over anything.  This is accessed
   *   by OpenTransactionList.
   */
  long getCommitID() {
    // REINFORCED NOTE: This absolutely must never be synchronized because
    //   it is accessed by OpenTransactionList synchronized.
    return commit_id;
  }


  /**
   * Searches through the list of tables visible within this transaction and
   * returns the MasterTableDataSource object with the given name.  Returns
   * null if no visible table with the given name could be found.
   */
  private MasterTableDataSource findVisibleTable(TableName table_name,
                                                 boolean ignore_case) {
    // NOTE: We don't have to worry about synchronization here because
    //   'visible_tables' can only be updated under an exclusive lock on the
    //   connection this transaction is within.
    int size = visible_tables.size();
    for (int i = 0; i < size; ++i) {
      MasterTableDataSource master =
                                (MasterTableDataSource) visible_tables.get(i);
      DataTableDef table_def = master.getDataTableDef();
      if (ignore_case) {
        if (table_def.getTableName().equalsIgnoreCase(table_name)) {
          return master;
        }
      }
      else {
        // Not ignore case
        if (table_def.getTableName().equals(table_name)) {
          return master;
        }
      }
    }
    return null;
  }

  /**
   * Returns the IndexSet for the given MasterTableDataSource object that
   * is visible in this transaction.
   */
  IndexSet getIndexSetForTable(MasterTableDataSource table) {
    int sz = table_indices.size();
    for (int i = 0; i < sz; ++i) {
      if (visible_tables.get(i) == table) {
        return (IndexSet) table_indices.get(i);
      }
    }
    throw new Error("MasterTableDataSource not found in this transaction.");
  }

  /**
   * Returns a TableDataSourceFactory for use to retrieve TableDataSource
   * objects within this transaction.
   */
  TableDataConglomerate.TableDataSourceFactory getTableDataSourceFactory() {
    return new TableDataConglomerate.TableDataSourceFactory() {
      public TableDataSource createTableDataSource(TableName table) {
        return getTable(table);
      }
    };
  }

  // ----- Operations within the context of this transaction -----

  /**
   * Returns a MutableTableDataSource object that represents the table with
   * the given name within this transaction.  Any changes made to this table
   * are only made within the context of this transaction.  This means if a
   * row is added or removed, it is not made perminant until the transaction
   * is committed.
   * <p>
   * If the table does not exist then an exception is thrown.
   */
  public MutableTableDataSource getTable(TableName table_name) {

//    String schema_name = full_table_name.getSchema();
//    String table_name = full_table_name.getName();

//    System.out.println(this + " GET: " + table_name);

    // Synchronize over the table cache for multi-threaded.

    synchronized (table_cache) {

      // System tables are not cached and are generated freshly each call
      // within a transaction...
      if (isInternalTable(table_name)) {
        return getInternalTable(table_name);
      }

      MutableTableDataSource table =
                         (MutableTableDataSource) table_cache.get(table_name);
      if (table == null) {
        // Not found in the cache
        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master == null) {
          throw new Error("Table '" + table_name + "' not found.");
        }
        table = master.createTableDataSourceAtCommit(this);
        // Log in the journal that this table was touched by the transaction.
        journal.entryAddTouchedTable(master.getTableID());
        touched_tables.add(table);

        // Put table name in the cache
        table_cache.put(table_name, table);
      }

      return table;
    }

  }

  /**
   * Called by the query evaluation layer when information is selected
   * from this table as part of this transaction.  When there is a select
   * query on a table, when the transaction is committed we should look for
   * any concurrently committed changes to the table.  If there are any, then
   * any selects on the table should be considered incorrect and cause a
   * commit failure.
   */
  public void addSelectedFromTable(TableName table_name) {
    // Special handling of internal tables,
    if (isInternalTable(table_name)) {
      return;
    }

    MasterTableDataSource master = findVisibleTable(table_name, false);
    if (master == null) {
      throw new Error("Table with name not available: " + table_name);
    }
//    System.out.println("Selected from table: " + table_name);
    synchronized (selected_from_tables) {
      if (!selected_from_tables.contains(master)) {
        selected_from_tables.add(master);
      }
    }

  }

  /**
   * Returns the DataTableDef for the table with the given name that is
   * visible within this transaction.
   * <p>
   * Returns null if table name doesn't refer to a table that exists.
   */
  public DataTableDef getDataTableDef(TableName table_name) {
    // If this is an internal system table then handle specially
    if (isInternalTable(table_name)) {
      return getInternalTableDataTableDef(table_name);
    }
    else {
      // Otherwise return from the pool of visible tables
      int sz = visible_tables.size();
      for (int i = 0; i < sz; ++i) {
        MasterTableDataSource master =
                                 (MasterTableDataSource) visible_tables.get(i);
        DataTableDef table_def = master.getDataTableDef();
        if (table_def.getTableName().equals(table_name)) {
          return table_def;
        }
      }
      return null;
    }
  }

  /**
   * Returns a list of table names that are visible within this transaction.
   */
  public TableName[] getTableList() {
    TableName[] internal_tables = getInternalTableList();

    int sz = visible_tables.size();
    // The result list
    TableName[] tables = new TableName[sz + internal_tables.length];
    // Add the master tables
    for (int i = 0; i < sz; ++i) {
      MasterTableDataSource master =
                               (MasterTableDataSource) visible_tables.get(i);
      DataTableDef table_def = master.getDataTableDef();
      tables[i] = new TableName(table_def.getSchema(), table_def.getName());
    }

    // Add any internal system tables to the list
    for (int i = 0; i < internal_tables.length; ++i) {
      tables[sz + i] = internal_tables[i];
    }

    return tables;
  }

  /**
   * Returns true if the table with the given name exists within this
   * transaction.
   */
  public boolean tableExists(TableName table_name) {
    // NOTE: We don't have to worry about synchronization here because
    //   'visible_tables' can only be updated under an exclusive lock on the
    //   connection this transaction is within.

    return isInternalTable(table_name) ||
           findVisibleTable(table_name, false) != null;
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
    // Is it a visable table (match case insensitive)
    MasterTableDataSource table = findVisibleTable(table_name, true);
    if (table != null) {
      return table.getTableName();
    }
    // Is it an internal table?
    String tschema = table_name.getSchema();
    String tname = table_name.getName();
    TableName[] list = getInternalTableList();
    for (int i = 0; i < list.length; ++i) {
      TableName ctable = list[i];
      if (ctable.getSchema().equalsIgnoreCase(tschema) &&
          ctable.getName().equalsIgnoreCase(tname)) {
        return ctable;
      }
    }

    // No matches so return the original object.
    return table_name;
  }

  // ---------- Internally generated tables ----------

//  private static TableName[] INTERNAL_TABLE_LIST = {
//    Database.SYS_TABLE_COLUMNS, Database.SYS_TABLE_INFO,
//    Database.SYS_DB_STATISTICS
//  };

  /**
   * Returns true if the given table name represents an internally generated
   * system table.
   */
  private boolean isInternalTable(TableName table_name) {
    for (int i = 0; i < internal_tables.length; ++i) {
      InternalTableInfo info = internal_tables[i];
      if (info != null) {
        if (info.containsTableName(table_name)) {
          return true;
        }
      }
    }
    return false;

//    if ( table_name.equals(Database.SYS_TABLE_COLUMNS) ||
//         table_name.equals(Database.SYS_TABLE_INFO) ||
//         table_name.equals(Database.SYS_DB_STATISTICS) ) {
//      return true;
//    }
//    return false;
  }

  /**
   * Returns a list of all internal table names.  This method returns a
   * reference to a static, make sure you don't change the contents of the
   * array!
   */
  private TableName[] getInternalTableList() {
    int sz = 0;
    for (int i = 0; i < internal_tables.length; ++i) {
      InternalTableInfo info = internal_tables[i];
      if (info != null) {
        sz += info.getTableCount();
      }
    }

    TableName[] list = new TableName[sz];
    int index = 0;

    for (int i = 0; i < internal_tables.length; ++i) {
      InternalTableInfo info = internal_tables[i];
      if (info != null) {
        sz = info.getTableCount();
        for (int n = 0; n < sz; ++n) {
          list[index] = info.getTableName(n);
          ++index;
        }
      }
    }

    return list;

//    return INTERNAL_TABLE_LIST;
  }

  /**
   * Returns the DataTableDef for the given internal table.
   */
  private DataTableDef getInternalTableDataTableDef(TableName table_name) {

    for (int i = 0; i < internal_tables.length; ++i) {
      InternalTableInfo info = internal_tables[i];
      int index = info.findTableName(table_name);
      if (index != -1) {
        return info.getDataTableDef(index);
      }
    }

//    if (table_name.equals(Database.SYS_TABLE_COLUMNS)) {
//      return new GTTableColumnsDataSource(this).getDataTableDef();
//    }
//    else if (table_name.equals(Database.SYS_TABLE_INFO)) {
//      return new GTTableInfoDataSource(this).getDataTableDef();
//    }
//    else if (table_name.equals(Database.SYS_DB_STATISTICS)) {
//      return new GTStatisticsDataSource(this).getDataTableDef();
//    }

    throw new RuntimeException("Not an internal table: " + table_name);
  }

  /**
   * Returns an instance of MutableDataTableSource that represents the
   * contents of the internal table with the given name.
   */
  private MutableTableDataSource getInternalTable(TableName table_name) {

    for (int i = 0; i < internal_tables.length; ++i) {
      InternalTableInfo info = internal_tables[i];
      int index = info.findTableName(table_name);
      if (index != -1) {
        return info.createInternalTable(index);
      }
    }

//    if (table_name.equals(Database.SYS_TABLE_COLUMNS)) {
//      return new GTTableColumnsDataSource(this).init();
//    }
//    else if (table_name.equals(Database.SYS_TABLE_INFO)) {
//      return new GTTableInfoDataSource(this).init();
//    }
//    else if (table_name.equals(Database.SYS_DB_STATISTICS)) {
//      return new GTStatisticsDataSource(this).init();
//    }

    throw new RuntimeException("Not an internal table: " + table_name);
  }

  // ---------- Transaction manipulation ----------

  /**
   * Adds a high level trigger event to the list of actions that occured during
   * this transaction.  The list of trigger events is flushed when the
   * transaction is successfully committed.
   */
  public void addTriggerEvent(TriggerEvent evt) {
    // We synchronize over the list to prevent trigger events interfering
    // (I found that INSERT's to different tables can cause this list to be
    //  updated concurrently).
    synchronized (trigger_event_list) {
      trigger_event_list.add(evt);
    }
  }

//  /**
//   * Creates and returns a new MasterTableDataSource within this transaction
//   * with the given sector size.  This does NOT throw an exception if the
//   * master table already exists.  This can be used to temporarily create
//   * two tables with the same name in the database.
//   * <p>
//   * This should only be called under an exclusive lock on the connection.
//   */
//  private MasterTableDataSource createMasterTable(DataTableDef table_def,
//                               int data_sector_size, int index_sector_size) {
//    table_def.setImmutable();
//
//    if (data_sector_size < 27) {
//      data_sector_size = 27;
//    }
//    else if (data_sector_size > 4096) {
//      data_sector_size = 4096;
//    }
//
//    // Create the new master table and add to list of visible tables.
//    master = conglomerate.createMasterTable(table_def, data_sector_size,
//                                            index_sector_size);
//    visible_tables.add(master);
//    // Add the IndexSet to the end of the indices
//    table_indices.add(master.createIndexSet());
//
//    // Log in the journal that this transaction touched the table_id.
//    int table_id = master.getTableID();
//    journal.entryAddTouchedTable(table_id);
//
//    // Log in the journal that we created this table.
//    journal.entryTableCreate(table_id);
//
//    return master;
//  }

  /**
   * Creates a new table within this transaction with the given sector size.
   * If the table already exists then an exception is thrown.
   * <p>
   * This should only be called under an exclusive lock on the connection.
   */
  public void createTable(DataTableDef table_def,
                          int data_sector_size, int index_sector_size) {

    TableName table_name = table_def.getTableName();
//    String schema_name = tname.getSchema();
//    String table_name = tname.getName();
    MasterTableDataSource master = findVisibleTable(table_name, false);
    if (master != null) {
      throw new Error("Table '" + table_name + "' already exists.");
    }

    table_def.setImmutable();

    if (data_sector_size < 27) {
      data_sector_size = 27;
    }
    else if (data_sector_size > 4096) {
      data_sector_size = 4096;
    }

    // Create the new master table and add to list of visible tables.
    master = conglomerate.createMasterTable(table_def, data_sector_size,
                                            index_sector_size);
    visible_tables.add(master);
    // Add the IndexSet to the end of the indices
    table_indices.add(master.createIndexSet());

    // Log in the journal that this transaction touched the table_id.
    int table_id = master.getTableID();
    journal.entryAddTouchedTable(table_id);

    // Log in the journal that we created this table.
    journal.entryTableCreate(table_id);
  }

  /**
   * Creates a new table within this transaction.  If the table already
   * exists then an exception is thrown.
   * <p>
   * This should only be called under an exclusive lock on the connection.
   */
  public void createTable(DataTableDef table_def) {
    // data sector size defaults to 251
    // index sector size defaults to 1024
    createTable(table_def, 251, 1024);
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
      alterTable(table_def.getTableName(), table_def,
                 data_sector_size, index_sector_size);
    }
  }

//  /**
//   * Drops the given MasterTableDataSource table from within this transaction
//   * and disposes of the resources.
//   */
//  private void dropMasterTable(MasterTableDataSource master) {
//    if (master == null) {
//      throw new Error("Table '" + table_name + "' doesn't exist.");
//    }
//
//    int table_index = visible_tables.indexOf(master);
//    // Remove from visible tables, and remove from cache.
//    visible_tables.remove(table_index);
//    // Remove from table indices
//    IndexSet index_set = (IndexSet) table_indices.remove(table_index);
//    // Make sure we dispose the IndexSet of the dropped table.
//    index_set.dispose();
//
//    synchronized (table_cache) {
//      table_cache.remove(table_name);
//    }
//
//    // Log in the journal that this transaction touched the table_id.
//    int table_id = master.getTableID();
//    journal.entryAddTouchedTable(table_id);
//
//    // Log in the journal that we dropped this table.
//    journal.entryTableDrop(table_id);
//  }

  /**
   * Drops a table within this transaction.  If the table does not exist then
   * an exception is thrown.
   * <p>
   * This should only be called under an exclusive lock on the connection.
   */
  public void dropTable(TableName table_name) {
//    System.out.println(this + " DROP: " + table_name);
    MasterTableDataSource master = findVisibleTable(table_name, false);

    if (master == null) {
      throw new Error("Table '" + table_name + "' doesn't exist.");
    }

    int table_index = visible_tables.indexOf(master);
    // Remove from visible tables, and remove from cache.
    master = (MasterTableDataSource) visible_tables.remove(table_index);
    // Remove from table indices
    IndexSet index_set = (IndexSet) table_indices.remove(table_index);

    // Add this information to the list of tables we need to dispose when we
    // clean up.
    if (clean_up_info == null) {
      clean_up_info = new ArrayList();
    }
    clean_up_info.add(master);
    clean_up_info.add(index_set);

    synchronized (table_cache) {
      table_cache.remove(table_name);
    }

    // Log in the journal that this transaction touched the table_id.
    int table_id = master.getTableID();
    journal.entryAddTouchedTable(table_id);

    // Log in the journal that we dropped this table.
    journal.entryTableDrop(table_id);

  }

  /**
   * Alter the table with the given name to the new definition and give the
   * copied table a new data sector size.  If the table does not exist then
   * an exception is thrown.
   * <p>
   * This copies all columns that were in the original table to the new
   * altered table if the name is the same.  Any names that don't exist are
   * set to the default value.
   * <p>
   * This should only be called under an exclusive lock on the connection.
   */
  public void alterTable(TableName table_name, DataTableDef table_def,
                         int data_sector_size, int index_sector_size) {

    table_def.setImmutable();

    // The current schema context is the schema of the table name
    String current_schema = table_name.getSchema();
    SystemQueryContext context = new SystemQueryContext(this, current_schema);

    // Get the next unique id of the unaltered table.
    long next_id = nextUniqueID(table_name);

    // NOTE: We don't need to make any journal entries for this because this
    //   is a convenience method using the other transaction functions to
    //   complete its task.
    MutableTableDataSource current_table = getTable(table_name);
    dropTable(table_name);
    createTable(table_def, data_sector_size, index_sector_size);
    MutableTableDataSource altered_table = getTable(table_def.getTableName());

    // ** Set copy of the unique_id value to the new altered table. **
    findVisibleTable(table_def.getTableName(), false).setUniqueID(next_id);

    // Work out which columns we have to copy to where
    int[] col_map = new int[table_def.columnCount()];
    DataTableDef orig_td = current_table.getDataTableDef();
    for (int i = 0; i < col_map.length; ++i) {
      String col_name = table_def.columnAt(i).getName();
      col_map[i] = orig_td.findColumnName(col_name);
    }

    // Copy all data from 'current_table' to 'altered_table'.
    try {
      RowEnumeration e = current_table.rowEnumeration();
      while (e.hasMoreRows()) {
        int row_index = e.nextRowIndex();
        RowData row_data = new RowData(altered_table);
        for (int i = 0; i < col_map.length; ++i) {
          int col = col_map[i];
          if (col != -1) {
            row_data.setColumnData(i,
                              current_table.getCellContents(col, row_index));
          }
        }
        row_data.setDefaultForRest(context);
        altered_table.addRow(row_data);
      }
    }
    catch (DatabaseException e) {
      Debug().writeException(e);
      throw new Error(e.getMessage());
    }

  }

  /**
   * Alters the table with the given name within this transaction to the
   * specified table definition.  If the table does not exist then an exception
   * is thrown.
   * <p>
   * This should only be called under an exclusive lock on the connection.
   */
  public void alterTable(TableName table_name, DataTableDef table_def) {

    // Make sure we remember the current sector size of the altered table so
    // we can create the new table with the original size.
    int current_data_sector_size;
    try {
      MasterTableDataSource master = findVisibleTable(table_name, false);
      current_data_sector_size = master.rawDataSectorSize();
    }
    catch (IOException e) {
      throw new Error("IO Error: " + e.getMessage());
    }

    // HACK: We use index sector size of 2043 for all altered tables
    alterTable(table_name, table_def, current_data_sector_size, 2043);

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
    // Get the table
    TableDataSource table = getTable(table_name);
    // Get all the rows in the table
    int[] rows = new int[table.getRowCount()];
    RowEnumeration row_enum = table.rowEnumeration();
    int i = 0;
    while (row_enum.hasMoreRows()) {
      rows[i] = row_enum.nextRowIndex();
      ++i;
    }
    // Check the constraints of all the rows in the table.
    TableDataConglomerate.checkAddConstraintViolations(
                     this, getTableDataSourceFactory(), table,
                     rows, INITIALLY_IMMEDIATE);

    // Add that we altered this table in the journal
    MasterTableDataSource master = findVisibleTable(table_name, false);
    if (master == null) {
      throw new Error("Table '" + table_name + "' doesn't exist.");
    }

    // Log in the journal that this transaction touched the table_id.
    int table_id = master.getTableID();

    journal.entryAddTouchedTable(table_id);
    // Log in the journal that we dropped this table.
    journal.entryTableConstraintAlter(table_id);

  }

  /**
   * Compacts the table with the given name within this transaction.  If the
   * table doesn't exist then an exception is thrown.
   */
  public void compactTable(TableName table_name) {

    // Find the master table.
    MasterTableDataSource current_table = findVisibleTable(table_name, false);
    if (current_table == null) {
      throw new Error("Table '" + table_name + "' doesn't exist.");
    }

    try {

      // Calculate the best sector size for the data in this table,
      if (current_table.rawRowCount() < 10) {
        // Not worth compacting
        return;
      }
      int row_count = current_table.rawRowCount();
      int[] store = new int[row_count];
      int index = 0;
      long total_size = 0;

      for (int i = 0; i < row_count; ++i) {
        int type = current_table.recordTypeInfo(i);
        if (type == RawDiagnosticTable.UNCOMMITTED ||
            type == RawDiagnosticTable.COMMITTED_ADDED) {
          int record_size = current_table.rawRecordSize(i);
          store[index] = record_size;
          ++index;
          total_size += record_size;
        }
      }

      if (index < 10) {
        // Not worth compacting
        return;
      }

      int avg = (int) (total_size / index);
//      System.out.println("Average row size: " + avg);

      double best_score = 100000000000.0;
      int best_ss = 45;
      int best_size = Integer.MAX_VALUE;

      for (int ss = 19; ss < avg + 128; ++ss) {
        int total_sectors = 0;
        for (int n = 0; n < store.length; ++n) {
          int sz = store[n];
          total_sectors += (sz / ( ss + 1 )) + 1;
        }

        int file_size = ((ss + 5) * total_sectors);
        double average_sec = (double) total_sectors / index;
        double score = file_size * (((average_sec - 1.0) / 20.0) + 1.0);

//        System.out.println(" (" + score + " : " + file_size +
//                           " : " + ss + " : " + average_sec + ") ");
        if (average_sec < 2.8 && score < best_score) {
          best_score = score;
          best_size = file_size;
          best_ss = ss;
        }
      }

//      System.out.println("Best sector size: " + best_ss +
//                         " Best file size: " + best_size );

      // The current sector size of the given table name.
      int current_sector_size = current_table.rawDataSectorSize();
      if (current_sector_size != best_ss) {
//      System.out.println("Changing sector size from " + current_sector_size +
//                         " to " + best_ss);
        // Alter the table to the new sector size.
        DataTableDef table_def = current_table.getDataTableDef();
        alterTable(table_name, table_def, best_ss, 2043);
      }

    }
    catch (IOException e) {
      throw new Error("IO Error: " + e.getMessage());
    }

  }

  /**
   * Atomically returns a unique id that can be used as a seed for a set of
   * unique identifiers for a table.  Values returned by this method are
   * guarenteed unique within this table.  This is true even across
   * transactions.
   * <p>
   * NOTE: This change can not be rolled back.
   */
  public long nextUniqueID(TableName table_name) {
    MasterTableDataSource master = findVisibleTable(table_name, false);
    if (master == null) {
      throw new Error("Table with name '" + table_name + "' could not be " +
                      "found to retrieve unique id.");
    }
    return master.nextUniqueID();
  }

  /**
   * Returns true if the conglomerate commit procedure should check for
   * dirty selects and produce a transaction error.  A dirty select is when
   * a query reads information from a table that is effected by another table
   * during a transaction.  This in itself will not cause data
   * consistancy problems but for strict conformance to SERIALIZABLE
   * isolation level this should return true.
   * <p>
   * NOTE; We MUST NOT make this method serialized because it is back called
   *   from within a commit lock in TableDataConglomerate.
   */
  boolean transactionErrorOnDirtySelect() {
    return transaction_error_on_dirty_select;
  }

  /**
   * Sets the transaction error on dirty select for this transaction.
   */
  void setErrorOnDirtySelect(boolean status) {
    transaction_error_on_dirty_select = status;
  }

  // ----- Setting/Querying constraint information -----
  // PENDING: Is it worth implementing a pluggable constraint architecture
  //   as described in the idea below.  With the current implementation we
  //   have tied a DataTableConglomerate to a specific constraint
  //   architecture.
  //
  // IDEA: These methods delegate to the parent conglomerate which has a
  //   pluggable architecture for setting/querying constraints.  Some uses of
  //   a conglomerate may not need integrity constraints or may implement the
  //   mechanism for storing/querying in a different way.  This provides a
  //   useful abstraction of being enable to implement constraint behaviour
  //   by only providing a way to set/query the constraint information in
  //   different conglomerate uses.

  /**
   * Convenience, given a SimpleTableQuery object this will return a list of
   * column names in sequence that represent the columns in a group constraint.
   * <p>
   * 'cols' is the unsorted list of indexes in the table that represent the
   * group.
   * <p>
   * Assumes column 2 of dt is the sequence number and column 1 is the name
   * of the column.
   */
  private static String[] toColumns(SimpleTableQuery dt, IntegerVector cols) {
    int size = cols.size();
    String[] list = new String[size];

    // for each n of the output list
    for (int n = 0; n < size; ++n) {
      // for each i of the input list
      for (int i = 0; i < size; ++i) {
        int row_index = cols.intAt(i);
        int seq_no = ((BigDecimal) dt.get(2, row_index)).intValue();
        if (seq_no == n) {
          list[n] = (String) dt.get(1, row_index);
          break;
        }
      }
    }

    return list;
  }

  /**
   * Convenience, generates a unique constraint name.  If the given constraint
   * name is 'null' then a new one is created, otherwise the given default
   * one is returned.
   */
  private static String makeUniqueConstraintName(String name,
                                                 BigDecimal unique_id) {
    if (name == null) {
      name = "_ANONYMOUS_CONSTRAINT_" + unique_id.toString();
    }
    return name;
  }


  /**
   * Create a new schema in this transaction.  When the transaction is
   * committed the schema will become globally accessable.  Note that any
   * security checks must be performed before this method is called.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void createSchema(String name, String type) {
    TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
    MutableTableDataSource t = getTable(table_name);
    SimpleTableQuery dt = new SimpleTableQuery(t);

    try {
      // Select entries where;
      //     sUSRSchemaInfo.name = name
      if (!dt.existsSingle(1, name)) {
        // Add the entry to the schema info table.
        RowData rd = new RowData(t);
        BigDecimal unique_id = new BigDecimal(nextUniqueID(table_name));
        rd.setColumnDataFromObject(0, unique_id);
        rd.setColumnDataFromObject(1, name);
        rd.setColumnDataFromObject(2, type);
        // Third (other) column is left as null
        t.addRow(rd);
      }
      else {
        throw new Error("Schema already exists: " + name);
      }
    }
    finally {
      dt.dispose();
    }
  }

  /**
   * Drops a schema from this transaction.  When the transaction is committed
   * the schema will be dropped perminently.  Note that any security checks
   * must be performed before this method is called.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void dropSchema(String name) {
    TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
    MutableTableDataSource t = getTable(table_name);
    SimpleTableQuery dt = new SimpleTableQuery(t);

    // Drop a single entry from dt where column 1 = name
    boolean b = dt.deleteSingle(1, name);
    dt.dispose();
    if (!b) {
      throw new Error("Schema doesn't exists: " + name);
    }
  }

  /**
   * Returns true if the schema exists within this transaction.
   */
  public boolean schemaExists(String name) {
    TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
    MutableTableDataSource t = getTable(table_name);
    SimpleTableQuery dt = new SimpleTableQuery(t);

    // Returns true if there's a single entry in dt where column 1 = name
    boolean b = dt.existsSingle(1, name);
    dt.dispose();
    return b;
  }

  /**
   * Resolves the case of the given schema name if the database is performing
   * case insensitive identifier matching.  Returns a SchemaDef object that
   * identifiers the schema.  Returns null if the schema name could not be
   * resolved.
   */
  public SchemaDef resolveSchemaCase(String name, boolean ignore_case) {
    // The list of schema
    SimpleTableQuery dt = new SimpleTableQuery(
                         getTable(TableDataConglomerate.SCHEMA_INFO_TABLE));

    RowEnumeration e = dt.rowEnumeration();
    if (ignore_case) {
      SchemaDef result = null;
      while (e.hasMoreRows()) {
        int row_index = e.nextRowIndex();
        String cur_name = (String) dt.get(1, row_index);
        if (name.equalsIgnoreCase(cur_name)) {
          if (result != null) {
            throw new Error("Ambiguous schema name: '" + name + "'");
          }
          String type = (String) dt.get(2, row_index);
          result = new SchemaDef(cur_name, type);
        }
      }
      return result;

    }
    else {  // if (!ignore_case)
      while (e.hasMoreRows()) {
        int row_index = e.nextRowIndex();
        String cur_name = (String) dt.get(1, row_index);
        if (name.equals(cur_name)) {
          String type = (String) dt.get(2, row_index);
          return new SchemaDef(cur_name, type);
        }
      }
      // Not found
      return null;
    }

  }



  /**
   * Sets a persistent variable of the database that becomes a committed
   * change once this transaction is committed.  The variable can later be
   * retrieved with a call to the 'getPersistantVar' method.  A persistant
   * var is created if it doesn't exist in the DatabaseVars table otherwise
   * it is overwritten.
   */
  public void setPersistentVar(String variable, String value) {
    TableName table_name = TableDataConglomerate.PERSISTENT_VAR_TABLE;
    MutableTableDataSource t = getTable(table_name);
    SimpleTableQuery dt = new SimpleTableQuery(t);
    dt.setVar(0, new Object[] { variable, value });
    dt.dispose();
  }

  /**
   * Returns the value of the persistent variable with the given name or null
   * if it doesn't exist.
   */
  public String getPersistantVar(String variable) {
    TableName table_name = TableDataConglomerate.PERSISTENT_VAR_TABLE;
    MutableTableDataSource t = getTable(table_name);
    SimpleTableQuery dt = new SimpleTableQuery(t);
    String val = (String) dt.getVar(1, 0, variable);
    dt.dispose();
    return val;
  }

  /**
   * Adds a unique constraint to the database which becomes perminant when
   * the transaction is committed.  Columns in a table that are defined as
   * unique are prevented from being duplicated by the engine.
   * <p>
   * NOTE: Security checks for adding constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void addUniqueConstraint(TableName table_name,
                    String[] cols, short deferred, String constraint_name) {

    TableName tn1 = TableDataConglomerate.UNIQUE_INFO_TABLE;
    TableName tn2 = TableDataConglomerate.UNIQUE_COLS_TABLE;
    MutableTableDataSource t = getTable(tn1);
    MutableTableDataSource tcols = getTable(tn2);

    try {

      // Insert a value into UNIQUE_INFO_TABLE
      RowData rd = new RowData(t);
      BigDecimal unique_id = new BigDecimal(nextUniqueID(tn1));
      constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
      rd.setColumnDataFromObject(0, unique_id);
      rd.setColumnDataFromObject(1, constraint_name);
      rd.setColumnDataFromObject(2, table_name.getSchema());
      rd.setColumnDataFromObject(3, table_name.getName());
      rd.setColumnDataFromObject(4, new BigDecimal(deferred));
      t.addRow(rd);

      // Insert the columns
      for (int i = 0; i < cols.length; ++i) {
        rd = new RowData(tcols);
        rd.setColumnDataFromObject(0, unique_id);          // unique id
        rd.setColumnDataFromObject(1, cols[i]);            // column name
        rd.setColumnDataFromObject(2, new BigDecimal(i));  // sequence number
        tcols.addRow(rd);
      }

    }
    catch (DatabaseConstraintViolationException e) {
      // Constraint violation when inserting the data.  Check the type and
      // wrap around an appropriate error message.
      if (e.getErrorCode() ==
                DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
        // This means we gave a constraint name that's already being used
        // for a primary key.
        throw new Error("Unique constraint name '" + constraint_name +
                        "' is already being used.");
      }
      throw e;
    }

  }

  /**
   * Adds a foreign key constraint to the database which becomes perminent
   * when the transaction is committed.  A foreign key represents a referential
   * link from one table to another (may be the same table).  The 'table_name',
   * 'cols' args represents the object to link from.  The 'ref_table',
   * 'ref_cols' args represents the object to link to.  The update rules are
   * for specifying cascading delete/update rules.  The deferred arg is for
   * IMMEDIATE/DEFERRED checking.
   * <p>
   * NOTE: Security checks for adding constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void addForeignKeyConstraint(TableName table, String[] cols,
                                      TableName ref_table, String[] ref_cols,
                                      String delete_rule, String update_rule,
                                      short deferred, String constraint_name) {
    TableName tn1 = TableDataConglomerate.FOREIGN_INFO_TABLE;
    TableName tn2 = TableDataConglomerate.FOREIGN_COLS_TABLE;
    MutableTableDataSource t = getTable(tn1);
    MutableTableDataSource tcols = getTable(tn2);

    try {

      // If 'ref_columns' empty then set to primary key for referenced table,
      // ISSUE: What if primary key changes after the fact?
      if (ref_cols.length == 0) {
        ColumnGroup set = queryTablePrimaryKeyGroup(ref_table);
        if (set == null) {
          throw new Error(
            "No primary key defined for referenced table '" + ref_table + "'");
        }
        ref_cols = set.columns;
      }

      if (cols.length != ref_cols.length) {
        throw new Error("Foreign key reference '" + table +
          "' -> '" + ref_table + "' does not have an equal number of " +
          "column terms.");
      }

      // Insert a value into FOREIGN_INFO_TABLE
      RowData rd = new RowData(t);
      BigDecimal unique_id = new BigDecimal(nextUniqueID(tn1));
      constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
      rd.setColumnDataFromObject(0, unique_id);
      rd.setColumnDataFromObject(1, constraint_name);
      rd.setColumnDataFromObject(2, table.getSchema());
      rd.setColumnDataFromObject(3, table.getName());
      rd.setColumnDataFromObject(4, ref_table.getSchema());
      rd.setColumnDataFromObject(5, ref_table.getName());
      rd.setColumnDataFromObject(6, update_rule);
      rd.setColumnDataFromObject(7, delete_rule);
      rd.setColumnDataFromObject(8, new BigDecimal(deferred));
      t.addRow(rd);

      // Insert the columns
      for (int i = 0; i < cols.length; ++i) {
        rd = new RowData(tcols);
        rd.setColumnDataFromObject(0, unique_id);          // unique id
        rd.setColumnDataFromObject(1, cols[i]);            // column name
        rd.setColumnDataFromObject(2, ref_cols[i]);        // ref column name
        rd.setColumnDataFromObject(3, new BigDecimal(i));  // sequence number
        tcols.addRow(rd);
      }

    }
    catch (DatabaseConstraintViolationException e) {
      // Constraint violation when inserting the data.  Check the type and
      // wrap around an appropriate error message.
      if (e.getErrorCode() ==
                DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
        // This means we gave a constraint name that's already being used
        // for a primary key.
        throw new Error("Foreign key constraint name '" + constraint_name +
                        "' is already being used.");
      }
      throw e;
    }

  }

  /**
   * Adds a primary key constraint that becomes perminent when the transaction
   * is committed.  A primary key represents a set of columns in a table
   * that are constrained to be unique and can not be null.  If the
   * constraint name parameter is 'null' a primary key constraint is created
   * with a unique constraint name.
   * <p>
   * NOTE: Security checks for adding constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void addPrimaryKeyConstraint(TableName table_name, String[] cols,
                                      short deferred, String constraint_name) {

    TableName tn1 = TableDataConglomerate.PRIMARY_INFO_TABLE;
    TableName tn2 = TableDataConglomerate.PRIMARY_COLS_TABLE;
    MutableTableDataSource t = getTable(tn1);
    MutableTableDataSource tcols = getTable(tn2);

    try {

      // Insert a value into PRIMARY_INFO_TABLE
      RowData rd = new RowData(t);
      BigDecimal unique_id = new BigDecimal(nextUniqueID(tn1));
      constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
      rd.setColumnDataFromObject(0, unique_id);
      rd.setColumnDataFromObject(1, constraint_name);
      rd.setColumnDataFromObject(2, table_name.getSchema());
      rd.setColumnDataFromObject(3, table_name.getName());
      rd.setColumnDataFromObject(4, new BigDecimal(deferred));
      t.addRow(rd);

      // Insert the columns
      for (int i = 0; i < cols.length; ++i) {
        rd = new RowData(tcols);
        rd.setColumnDataFromObject(0, unique_id);          // unique id
        rd.setColumnDataFromObject(1, cols[i]);            // column name
        rd.setColumnDataFromObject(2, new BigDecimal(i));  // Sequence number
        tcols.addRow(rd);
      }

    }
    catch (DatabaseConstraintViolationException e) {
      // Constraint violation when inserting the data.  Check the type and
      // wrap around an appropriate error message.
      if (e.getErrorCode() ==
                DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
        // This means we gave a constraint name that's already being used
        // for a primary key.
        throw new Error("Primary key constraint name '" + constraint_name +
                        "' is already being used.");
      }
      throw e;
    }

  }

  /**
   * Adds a check expression that becomes perminent when the transaction
   * is committed.  A check expression is an expression that must evaluate
   * to true for all records added/updated in the database.
   * <p>
   * NOTE: Security checks for adding constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void addCheckConstraint(TableName table_name,
               Expression expression, short deferred, String constraint_name) {

    TableName tn = TableDataConglomerate.CHECK_INFO_TABLE;
    MutableTableDataSource t = getTable(tn);
    int col_count = t.getDataTableDef().columnCount();

    try {

      // Insert check constraint data.
      BigDecimal unique_id = new BigDecimal(nextUniqueID(tn));
      constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
      RowData rd = new RowData(t);
      rd.setColumnDataFromObject(0, unique_id);
      rd.setColumnDataFromObject(1, constraint_name);
      rd.setColumnDataFromObject(2, table_name.getSchema());
      rd.setColumnDataFromObject(3, table_name.getName());
      rd.setColumnDataFromObject(4, new String(expression.text()));
      rd.setColumnDataFromObject(5, new BigDecimal(deferred));
      if (col_count > 6) {
        // Serialize the check expression
        ByteLongObject serialized_expression =
                                  ObjectTranslator.serialize(expression);
        rd.setColumnDataFromObject(6, serialized_expression);
      }
      t.addRow(rd);

    }
    catch (DatabaseConstraintViolationException e) {
      // Constraint violation when inserting the data.  Check the type and
      // wrap around an appropriate error message.
      if (e.getErrorCode() ==
                DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
        // This means we gave a constraint name that's already being used.
        throw new Error("Check constraint name '" + constraint_name +
                         "' is already being used.");
      }
      throw e;
    }

  }

  /**
   * Drops all the constraints defined for the given table.  This is a useful
   * function when dropping a table from the database.
   * <p>
   * NOTE: Security checks for dropping constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void dropAllConstraintsForTable(TableName table_name) {
    ColumnGroup primary = queryTablePrimaryKeyGroup(table_name);
    ColumnGroup[] uniques = queryTableUniqueGroups(table_name);
    CheckExpression[] expressions = queryTableCheckExpressions(table_name);
    ColumnGroupReference[] refs = queryTableForeignKeyReferences(table_name);

    if (primary != null) {
      dropPrimaryKeyConstraintForTable(table_name, primary.name);
    }
    for (int i = 0; i < uniques.length; ++i) {
      dropUniqueConstraintForTable(table_name, uniques[i].name);
    }
    for (int i = 0; i < expressions.length; ++i) {
      dropCheckConstraintForTable(table_name, expressions[i].name);
    }
    for (int i = 0; i < refs.length; ++i) {
      dropForeignKeyReferenceConstraintForTable(table_name, refs[i].name);
    }

  }

  /**
   * Drops the named constraint from the transaction.  Used when altering
   * table schema.
   * <p>
   * NOTE: Security checks for dropping constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void dropNamedConstraint(TableName table_name,
                                  String constraint_name) {
    dropPrimaryKeyConstraintForTable(table_name, constraint_name);
    dropUniqueConstraintForTable(table_name, constraint_name);
    dropCheckConstraintForTable(table_name, constraint_name);
    dropForeignKeyReferenceConstraintForTable(table_name, constraint_name);
  }

  /**
   * Drops the primary key constraint for the given table.  Used when altering
   * table schema.  If 'constraint_name' is null this method will search for
   * the primary key on the table name.
   * <p>
   * NOTE: Security checks for dropping constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void dropPrimaryKeyConstraintForTable(
                            TableName table_name, String constraint_name) {

    MutableTableDataSource t =
                         getTable(TableDataConglomerate.PRIMARY_INFO_TABLE);
    MutableTableDataSource t2 =
                         getTable(TableDataConglomerate.PRIMARY_COLS_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
    SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

    try {
      IntegerVector data;
      if (constraint_name != null) {
        // Returns the list of indexes where column 1 = constraint name
        //                               and column 2 = schema name
        data = dt.selectIndexesEqual(1, constraint_name,
                                     2, table_name.getSchema());
      }
      else {
        // Returns the list of indexes where column 3 = table name
        //                               and column 2 = schema name
        data = dt.selectIndexesEqual(3, table_name.getName(),
                                     2, table_name.getSchema());
      }

      if (data.size() > 1) {
        throw new Error("Assertion failed: multiple primary key for: " +
                        table_name);
      }
      else if (data.size() == 1) {
        int row_index = data.intAt(0);
        // The id
        BigDecimal id = (BigDecimal) dt.get(0, row_index);
        // All columns with this id
        IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
        // Delete from the table
        dtcols.deleteRows(ivec);
        dt.deleteRows(data);
      }

    }
    finally {
      dtcols.dispose();
      dt.dispose();
    }

  }

  /**
   * Drops a single named unique constraint from the given table.
   * <p>
   * NOTE: Security checks for dropping constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void dropUniqueConstraintForTable(
                                   TableName table, String constraint_name) {

    MutableTableDataSource t =
                         getTable(TableDataConglomerate.UNIQUE_INFO_TABLE);
    MutableTableDataSource t2 =
                         getTable(TableDataConglomerate.UNIQUE_COLS_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
    SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

    try {
      // Returns the list of indexes where column 1 = constraint name
      //                               and column 2 = schema name
      IntegerVector data = dt.selectIndexesEqual(1, constraint_name,
                                                 2, table.getSchema());

      if (data.size() > 1) {
        throw new Error("Assertion failed: multiple unique constraint name: " +
                        constraint_name);
      }
      else if (data.size() == 1) {
        int row_index = data.intAt(0);
        // The id
        BigDecimal id = (BigDecimal) dt.get(0, row_index);
        // All columns with this id
        IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
        // Delete from the table
        dtcols.deleteRows(ivec);
        dt.deleteRows(data);
      }
    }
    finally {
      dtcols.dispose();
      dt.dispose();
    }

  }

  /**
   * Drops a single named check constraint from the given table.
   * <p>
   * NOTE: Security checks for dropping constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void dropCheckConstraintForTable(
                                   TableName table, String constraint_name) {

    MutableTableDataSource t =
                         getTable(TableDataConglomerate.CHECK_INFO_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

    try {
      // Returns the list of indexes where column 1 = constraint name
      //                               and column 2 = schema name
      IntegerVector data = dt.selectIndexesEqual(1, constraint_name,
                                                 2, table.getSchema());

      if (data.size() > 1) {
        throw new Error("Assertion failed: multiple check constraint name: " +
                        constraint_name);
      }
      else if (data.size() == 1) {
        // Delete the check constraint
        dt.deleteRows(data);
      }
    }
    finally {
      dt.dispose();
    }

  }

  /**
   * Drops a single named foreign key reference from the given table.
   * <p>
   * NOTE: Security checks for dropping constraints must be checked for at a
   *   higher layer.
   * <p>
   * NOTE: We must guarentee that the transaction be in exclusive mode before
   *   this method is called.
   */
  public void dropForeignKeyReferenceConstraintForTable(
                                   TableName table, String constraint_name) {

    MutableTableDataSource t =
                         getTable(TableDataConglomerate.FOREIGN_INFO_TABLE);
    MutableTableDataSource t2 =
                         getTable(TableDataConglomerate.FOREIGN_COLS_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
    SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

    try {
      // Returns the list of indexes where column 1 = constraint name
      //                               and column 2 = schema name
      IntegerVector data = dt.selectIndexesEqual(1, constraint_name,
                                                 2, table.getSchema());

      if (data.size() > 1) {
        throw new Error("Assertion failed: multiple foreign key constraint " +
                        "name: " + constraint_name);
      }
      else if (data.size() == 1) {
        int row_index = data.intAt(0);
        // The id
        BigDecimal id = (BigDecimal) dt.get(0, row_index);
        // All columns with this id
        IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
        // Delete from the table
        dtcols.deleteRows(ivec);
        dt.deleteRows(data);
      }
    }
    finally {
      dtcols.dispose();
      dt.dispose();
    }

  }

  /**
   * Returns the list of tables (as a TableName array) that are dependant
   * on the data in the given table to maintain referential consistancy.  The
   * list includes the tables referenced as foreign keys, and the tables
   * that reference the table as a foreign key.
   * <p>
   * This is a useful query for determining ahead of time the tables that
   * require a read lock when inserting/updating a table.  A table will require
   * a read lock if the operation needs to query it for potential referential
   * integrity violations.
   */
  public TableName[] queryTablesRelationallyLinkedTo(TableName table) {
    ArrayList list = new ArrayList();
    ColumnGroupReference[] refs = queryTableForeignKeyReferences(table);
    for (int i = 0; i < refs.length; ++i) {
      TableName tname = refs[i].ref_table_name;
      if (!list.contains(tname)) {
        list.add(tname);
      }
    }
    refs = queryTableImportedForeignKeyReferences(table);
    for (int i = 0; i < refs.length; ++i) {
      TableName tname = refs[i].key_table_name;
      if (!list.contains(tname)) {
        list.add(tname);
      }
    }
    return (TableName[]) list.toArray(new TableName[list.size()]);
  }

  /**
   * Returns a set of unique groups that are constrained to be unique for
   * the given table in this transaction.  For example, if columns ('name')
   * and ('number', 'document_rev') are defined as unique, this will return
   * an array of two groups that represent unique columns in the given
   * table.
   */
  public ColumnGroup[] queryTableUniqueGroups(TableName table_name) {
    MutableTableDataSource t =
                         getTable(TableDataConglomerate.UNIQUE_INFO_TABLE);
    MutableTableDataSource t2 =
                         getTable(TableDataConglomerate.UNIQUE_COLS_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
    SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

    ColumnGroup[] groups;
    try {
      // Returns the list indexes where column 3 = table name
      //                            and column 2 = schema name
      IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                                                 2, table_name.getSchema());
      groups = new ColumnGroup[data.size()];

      for (int i = 0; i < data.size(); ++i) {
        Object id = dt.get(0, data.intAt(i));

        // Select all records with equal id
        IntegerVector cols = dtcols.selectIndexesEqual(0, id);

        // Put into a group.
        ColumnGroup group = new ColumnGroup();
        group.name = (String) dt.get(1, data.intAt(i));  // constraint name
        group.columns = toColumns(dtcols, cols);   // the list of columns
        group.deferred = ((BigDecimal) dt.get(4, data.intAt(i))).shortValue();
        groups[i] = group;
      }
    }
    finally {
      dt.dispose();
      dtcols.dispose();
    }

    return groups;
  }

  /**
   * Returns a set of primary key groups that are constrained to be unique
   * for the given table in this transaction (there can be only 1 primary
   * key defined for a table).  Returns null if there is no primary key
   * defined for the table.
   */
  public ColumnGroup queryTablePrimaryKeyGroup(TableName table_name) {
    MutableTableDataSource t =
                         getTable(TableDataConglomerate.PRIMARY_INFO_TABLE);
    MutableTableDataSource t2 =
                         getTable(TableDataConglomerate.PRIMARY_COLS_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
    SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

    try {
      // Returns the list indexes where column 3 = table name
      //                            and column 2 = schema name
      IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                                                 2, table_name.getSchema());

      if (data.size() > 1) {
        throw new Error("Assertion failed: multiple primary key for: " +
                        table_name);
      }
      else if (data.size() == 1) {
        int row_index = data.intAt(0);
        // The id
        BigDecimal id = (BigDecimal) dt.get(0, row_index);
        // All columns with this id
        IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
        // Make it in to a columns object
        ColumnGroup group = new ColumnGroup();
        group.name = (String) dt.get(1, row_index);
        group.columns = toColumns(dtcols, ivec);
        group.deferred = ((BigDecimal) dt.get(4, row_index)).shortValue();
        return group;
      }
      else {
        return null;
      }
    }
    finally {
      dt.dispose();
      dtcols.dispose();
    }

  }

  /**
   * Returns a set of check expressions that are constrained over all new
   * columns added to the given table in this transaction.  For example,
   * we may want a column called 'serial_number' to be constrained as
   * CHECK serial_number LIKE '___-________-___'.
   */
  public CheckExpression[] queryTableCheckExpressions(TableName table_name) {
    MutableTableDataSource t =
                         getTable(TableDataConglomerate.CHECK_INFO_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

    CheckExpression[] checks;
    try {
      // Returns the list indexes where column 3 = table name
      //                            and column 2 = schema name
      IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                                                 2, table_name.getSchema());
      checks = new CheckExpression[data.size()];

      for (int i = 0; i < checks.length; ++i) {
        int row_index = data.intAt(i);

        CheckExpression check = new CheckExpression();
        check.name = (String) dt.get(1, row_index);
        check.deferred = ((BigDecimal) dt.get(5, row_index)).shortValue();
        // Is the deserialized version available?
        if (t.getDataTableDef().columnCount() > 6) {
          ByteLongObject sexp = (ByteLongObject) dt.get(6, row_index);
          if (sexp != null) {
            // Deserialize the expression
            check.expression = (Expression) ObjectTranslator.deserialize(sexp);
          }
        }
        // Otherwise we need to parse it from the string
        if (check.expression == null) {
          Expression exp = Expression.parse((String) dt.get(4, row_index));
          check.expression = exp;
        }
        checks[i] = check;
      }

    }
    finally {
      dt.dispose();
    }

    return checks;
  }

  /**
   * Returns an array of column references in the given table that represent
   * foreign key references.  For example, say a foreign reference has been
   * set up in the given table as follows;<p><pre>
   *   FOREIGN KEY (customer_id) REFERENCES Customer (id)
   * </pre><p>
   * This method will return the column group reference
   * Order(customer_id) -> Customer(id).
   * <p>
   * This method is used to check that a foreign key reference actually points
   * to a valid record in the referenced table as expected.
   */
  public ColumnGroupReference[] queryTableForeignKeyReferences(
                                                      TableName table_name) {

    MutableTableDataSource t =
                         getTable(TableDataConglomerate.FOREIGN_INFO_TABLE);
    MutableTableDataSource t2 =
                         getTable(TableDataConglomerate.FOREIGN_COLS_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
    SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

    ColumnGroupReference[] groups;
    try {
      // Returns the list indexes where column 3 = table name
      //                            and column 2 = schema name
      IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                                                 2, table_name.getSchema());
      groups = new ColumnGroupReference[data.size()];

      for (int i = 0; i < data.size(); ++i) {
        int row_index = data.intAt(i);

        // The foreign key id
        Object id = dt.get(0, row_index);

        // The referenced table
        TableName ref_table_name = new TableName(
              (String) dt.get(4, row_index), (String) dt.get(5, row_index));

        // Select all records with equal id
        IntegerVector cols = dtcols.selectIndexesEqual(0, id);

        // Put into a group.
        ColumnGroupReference group = new ColumnGroupReference();
        group.name = (String) dt.get(1, row_index);  // constraint name
        group.key_table_name = table_name;
        group.ref_table_name = ref_table_name;
        group.update_rule = (String) dt.get(6, row_index);
        group.delete_rule = (String) dt.get(7, row_index);
        group.deferred = ((BigDecimal) dt.get(8, row_index)).shortValue();

        int cols_size = cols.size();
        String[] key_cols = new String[cols_size];
        String[] ref_cols = new String[cols_size];
        for (int n = 0; n < cols_size; ++n) {
          for (int p = 0; p < cols_size; ++p) {
            int cols_index = cols.intAt(p);
            if (((BigDecimal) dtcols.get(3, cols_index)).intValue() == n) {
              key_cols[n] = (String) dtcols.get(1, cols_index);
              ref_cols[n] = (String) dtcols.get(2, cols_index);
              break;
            }
          }
        }
        group.key_columns = key_cols;
        group.ref_columns = ref_cols;

        groups[i] = group;
      }
    }
    finally {
      dt.dispose();
      dtcols.dispose();
    }

    return groups;
  }

  /**
   * Returns an array of column references in the given table that represent
   * foreign key references that reference columns in the given table.  This
   * is a reverse mapping of the 'queryTableForiegnKeyReferences' method.  For
   * example, say a foreign reference has been set up in any table as follows;
   * <p><pre>
   *   [ In table Order ]
   *   FOREIGN KEY (customer_id) REFERENCE Customer (id)
   * </pre><p>
   * And the table name we are querying is 'Customer' then this method will
   * return the column group reference
   * Order(customer_id) -> Customer(id).
   * <p>
   * This method is used to check that a reference isn't broken when we remove
   * a record (for example, removing a Customer that has references to it will
   * break integrity).
   */
  public ColumnGroupReference[] queryTableImportedForeignKeyReferences(
                                                  TableName ref_table_name) {

    MutableTableDataSource t =
                         getTable(TableDataConglomerate.FOREIGN_INFO_TABLE);
    MutableTableDataSource t2 =
                         getTable(TableDataConglomerate.FOREIGN_COLS_TABLE);
    SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
    SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

    ColumnGroupReference[] groups;
    try {
      // Returns the list indexes where column 5 = ref table name
      //                            and column 4 = ref schema name
      IntegerVector data = dt.selectIndexesEqual(5,ref_table_name.getName(),
                                                 4,ref_table_name.getSchema());
      groups = new ColumnGroupReference[data.size()];

      for (int i = 0; i < data.size(); ++i) {
        int row_index = data.intAt(i);

        // The foreign key id
        Object id = dt.get(0, row_index);

        // The referencee table
        TableName table_name = new TableName(
              (String) dt.get(2, row_index), (String) dt.get(3, row_index));

        // Select all records with equal id
        IntegerVector cols = dtcols.selectIndexesEqual(0, id);

        // Put into a group.
        ColumnGroupReference group = new ColumnGroupReference();
        group.name = (String) dt.get(1, row_index);  // constraint name
        group.key_table_name = table_name;
        group.ref_table_name = ref_table_name;
        group.update_rule = (String) dt.get(6, row_index);
        group.delete_rule = (String) dt.get(7, row_index);
        group.deferred = ((BigDecimal) dt.get(8, row_index)).shortValue();

        int cols_size = cols.size();
        String[] key_cols = new String[cols_size];
        String[] ref_cols = new String[cols_size];
        for (int n = 0; n < cols_size; ++n) {
          for (int p = 0; p < cols_size; ++p) {
            int cols_index = cols.intAt(p);
            if (((BigDecimal) dtcols.get(3, cols_index)).intValue() == n) {
              key_cols[n] = (String) dtcols.get(1, cols_index);
              ref_cols[n] = (String) dtcols.get(2, cols_index);
              break;
            }
          }
        }
        group.key_columns = key_cols;
        group.ref_columns = ref_cols;

        groups[i] = group;
      }
    }
    finally {
      dt.dispose();
      dtcols.dispose();
    }

    return groups;
  }







  // ----- Transaction close operations -----

  /**
   * Closes and marks a transaction as committed.  Any changes made by this
   * transaction are seen by all transactions created after this method
   * returns.
   * <p>
   * This method will fail under the following circumstances:
   * <ol>
   * <li> There are any rows deleted in this transaction that were deleted
   *  by another successfully committed transaction.
   * <li> There were rows added in another committed transaction that would
   *  change the result of the search clauses committed by this transaction.
   * </ol>
   * The first check is not too difficult to check for.  The second is very
   * difficult however we need it to ensure TRANSACTION_SERIALIZABLE isolation
   * is enforced.  We may have to simplify this by throwing a transaction
   * exception if the table has had any changes made to it during this
   * transaction.
   * <p>
   * This should only be called under an exclusive lock on the connection.
   */
  public void closeAndCommit() throws TransactionException {

    if (!closed) {
      try {
        closed = true;
        getSystem().stats().decrement("Transaction.count");
        // Get the conglomerate to do this commit.
        conglomerate.processCommit(this, visible_tables, selected_from_tables,
                                   touched_tables, journal);
        // If we committed, then flush the trigger events,
        getSystem().getTriggerManager().flushTriggerEvents(
                                                   this, trigger_event_list);
      }
      finally {
        cleanup();
      }
    }

  }

  /**
   * Closes and rolls back a transaction as if the commands the transaction ran
   * never happened.  This will not throw a transaction exception.
   * <p>
   * This should only be called under an exclusive lock on the connection.
   */
  public void closeAndRollback() {

    if (!closed) {
      try {
        closed = true;
        getSystem().stats().decrement("Transaction.count");
        // Notify the conglomerate that this transaction has closed.
        conglomerate.processRollback(this, touched_tables, journal);
      }
      finally {
        cleanup();
      }
    }

  }

  /**
   * Cleans up this transaction.
   */
  private void cleanup() {
    // Dispose all the IndexSet for each table
    try {
      for (int i = 0; i < table_indices.size(); ++i) {
        ((IndexSet) table_indices.get(i)).dispose();
      }
    }
    catch (Throwable e) {
      Debug().writeException(e);
    }

    // Dispose all the table we created
    try {
      for (int i = 0; i < touched_tables.size(); ++i) {
        MutableTableDataSource source =
                               (MutableTableDataSource) touched_tables.get(i);
        source.dispose();
      }
    }
    catch (Throwable e) {
      Debug().writeException(e);
    }

    // Dispose all tables we dropped.
    try {
      if (clean_up_info != null) {
        for (int i = 0; i < clean_up_info.size(); i += 2) {
          MasterTableDataSource master =
                                (MasterTableDataSource) clean_up_info.get(i);
          IndexSet index_set = (IndexSet) clean_up_info.get(i + 1);
          // Ignore 'master' variable because that will be cleaned up in the
          // 'touched_tables' check above.
          index_set.dispose();
        }
        clean_up_info = null;
      }
    }
    catch (Throwable e) {
      Debug().writeException(e);
    }

    getSystem().stats().increment("Transaction.cleanup");
    conglomerate = null;
    visible_tables = null;
    table_indices = null;
    touched_tables = null;
    table_cache = null;
    trigger_event_list = null;
    journal = null;
  }

  /**
   * Finalize, we should close the transaction.
   */
  public void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      Debug().write(Lvl.ERROR, this, "Transaction not closed!");
    }
    closeAndRollback();
  }



  // ---------- Transaction inner classes ----------

  /**
   * A list of DataTableDef system table definitions for tables internal to
   * the transaction.
   */
  private final static DataTableDef[] INTERNAL_DEF_LIST;

  static {
    INTERNAL_DEF_LIST = new DataTableDef[3];
    INTERNAL_DEF_LIST[0] = GTTableColumnsDataSource.DEF_DATA_TABLE_DEF;
    INTERNAL_DEF_LIST[1] = GTTableInfoDataSource.DEF_DATA_TABLE_DEF;
    INTERNAL_DEF_LIST[2] = GTProductDataSource.DEF_DATA_TABLE_DEF;
  }

  /**
   * A static internal table info for internal tables to the transaction.
   * This implementation includes all the dynamically generated system tables
   * that are tied to information in a transaction.
   */
  private class TransactionInternalTables extends InternalTableInfo {

    /**
     * Constructor.
     */
    public TransactionInternalTables() {
      super(INTERNAL_DEF_LIST);
    }

    // ---------- Implemented ----------

    GTDataSource createInternalTable(int index) {
      if (index == 0) {
        return new GTTableColumnsDataSource(Transaction.this).init();
      }
      else if (index == 1) {
        return new GTTableInfoDataSource(Transaction.this).init();
      }
      else if (index == 2) {
        return new GTProductDataSource(Transaction.this).init();
      }
      else {
        throw new RuntimeException();
      }
    }

  }

  /**
   * A group of columns as used by the constraint system.  A ColumnGroup is
   * a simple list of columns in a table.
   */
  public static class ColumnGroup {

    /**
     * The name of the group (the constraint name).
     */
    public String name;

    /**
     * The list of columns that make up the group.
     */
    public String[] columns;

    /**
     * Whether this is deferred or initially immediate.
     */
    public short deferred;

  }

  /**
   * Represents a constraint expression to check.
   */
  public static class CheckExpression {

    /**
     * The name of the check expression (the constraint name).
     */
    public String name;

    /**
     * The expression to check.
     */
    public Expression expression;

    /**
     * Whether this is deferred or initially immediate.
     */
    public short deferred;

  }

  /**
   * Represents a reference from a group of columns in one table to a group of
   * columns in another table.  The is used to represent a foreign key
   * reference.
   */
  public static class ColumnGroupReference {

    /**
     * The name of the group (the constraint name).
     */
    public String name;

    /**
     * The key table name.
     */
    public TableName key_table_name;

    /**
     * The list of columns that make up the key.
     */
    public String[] key_columns;

    /**
     * The referenced table name.
     */
    public TableName ref_table_name;

    /**
     * The list of columns that make up the referenced group.
     */
    public String[] ref_columns;

    /**
     * The update rule.
     */
    public String update_rule;

    /**
     * The delete rule.
     */
    public String delete_rule;

    /**
     * Whether this is deferred or initially immediate.
     */
    public short deferred;

  }

}
