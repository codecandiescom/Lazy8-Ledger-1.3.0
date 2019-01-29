/**
 * com.mckoi.database.TableDataConglomerate  18 Nov 2000
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

import java.io.*;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.mckoi.util.IntegerListInterface;
import com.mckoi.util.IntegerIterator;
import com.mckoi.util.IntegerVector;
import com.mckoi.util.ByteArrayUtil;
import com.mckoi.util.UserTerminal;
import com.mckoi.debug.*;

/**
 * A conglomerate of data that represents the contents of all tables in a
 * complete database.  This object handles all data persistance management
 * (storage, retrieval, removal) issues.  It is a transactional manager for
 * both data and indices in the database.
 *
 * @author Tobias Downer
 */

public class TableDataConglomerate {

  /**
   * The name of the file containing the conglomerate state in the file
   * system conglomerate directory.
   */
  private static final String STATE_EXT = ".sf";

  /**
   * The name of the file extention of the file lock on this conglomerate.
   */
  private static final String FLOCK_EXT = ".lock";


  // ---------- The standard constraint/schema tables ----------

  /**
   * The name of the system schema where persistant conglomerate state is
   * stored.
   */
  public static final String SYSTEM_SCHEMA = "SYS_INFO";

  /**
   * The schema info table.
   */
  public static final TableName SCHEMA_INFO_TABLE =
                             new TableName(SYSTEM_SCHEMA, "sUSRSchemaInfo");

  public static final TableName PERSISTENT_VAR_TABLE =
                           new TableName(SYSTEM_SCHEMA, "sUSRDatabaseVars");

  public static final TableName FOREIGN_COLS_TABLE =
                         new TableName(SYSTEM_SCHEMA, "sUSRForeignColumns");

  public static final TableName UNIQUE_COLS_TABLE =
                          new TableName(SYSTEM_SCHEMA, "sUSRUniqueColumns");

  public static final TableName PRIMARY_COLS_TABLE =
                         new TableName(SYSTEM_SCHEMA, "sUSRPrimaryColumns");

  public static final TableName CHECK_INFO_TABLE =
                              new TableName(SYSTEM_SCHEMA, "sUSRCheckInfo");

  public static final TableName UNIQUE_INFO_TABLE =
                             new TableName(SYSTEM_SCHEMA, "sUSRUniqueInfo");

  public static final TableName FOREIGN_INFO_TABLE =
                               new TableName(SYSTEM_SCHEMA, "sUSRFKeyInfo");

  public static final TableName PRIMARY_INFO_TABLE =
                               new TableName(SYSTEM_SCHEMA, "sUSRPKeyInfo");



  /**
   * The TransactionSystem that this Conglomerate is a child of.
   */
  private final TransactionSystem system;

  /**
   * The path this conglomerate is stored in.  This points to a directory
   * in the file system.
   */
  private File path;

  /**
   * The name given to this conglomerate.
   */
  private String name;

  /**
   * An open random access file to the conglomerate state container.  This
   * file stores information persistantly about the state of this object.
   * This file is called 'mckoi.sf' in the conglomerate directory.
   */
  private FixedSizeDataStore state_file;

  /**
   * Set to true if this conglomerate is read only.
   */
  private boolean read_only;

//  /**
//   * A lock for transactions on this conglomerate.  This value indicates the
//   * number of open transactions on the database.  When this value is 0 then
//   * it is safe to make any data in the database permanent because we can be
//   * assured no transactions are open.
//   */
//  private int transaction_lock;

  /**
   * The current id for new transactions.  This is incremented each time a
   * new transaction is created.
   */
  private long transaction_id;

  /**
   * The current commit id for committed transactions.  Whenever transactional
   * changes are committed to the conglomerate, this id is incremented.
   */
  private long commit_id;

  /**
   * The current table_id for this conglomerate.  Every time a table is
   * created this number is incremented by 1.
   */
  private int cur_table_id;


  /**
   * The list of all tables that are currently open in this conglomerate.
   * This includes tables that are not committed.
   */
  private ArrayList table_list;

  /**
   * The list of MasterTableDataSource objects that are currently in the
   * conglomerate and commited.  These are the current active tables in our
   * database.
   */
  private ArrayList committed_tables;

  /**
   * The list of table file names that are committed dropped tables.
   */
  private ArrayList committed_dropped;


//  /**
//   * Set to true when either 'writeVisibleTables' or 'writeConglomerateState'
//   * is called.
//   */
//  private boolean new_tables = false,
//                  new_state = false;

  /**
   * The list of transactions that are currently open over this conglomerate.
   * This list is ordered from lowest commit_id to highest.  This object is
   * shared with all the children MasterTableDataSource objects.
   */
  private OpenTransactionList open_transactions;
//  private ArrayList open_transactions;

  // ---------- File lock ----------

  /**
   * The FileOutputStream for the conglomerate lock.  This is kept open so
   * that access to this conglomerate is restricted to one JVM.  You can not
   * delete a lock that is open.
   */
  private FileOutputStream conglomerate_file_lock;


  // ---------- Locks ----------

  /**
   * This lock is obtained when we go to commit a change to the table.
   * Grabbing this lock ensures that no other commits can occur at the same
   * time on this conglomerate.
   */
  Object commit_lock = new Object();




  /**
   * Constructs the conglomerate.
   */
  public TableDataConglomerate(TransactionSystem system) {
    this.system = system;
    open_transactions = new OpenTransactionList(system);
    committed_dropped = new ArrayList();

    conglomerate_cleanup_event =
                         system.createEvent(new ConglomerateCleanUpEvent());
  }

  /**
   * Returns the TransactionSystem that this conglomerate is part of.
   */
  public final TransactionSystem getSystem() {
    return system;
  }

  /**
   * Returns the DebugLogger object that we use to log debug messages to.
   */
  public final DebugLogger Debug() {
    return getSystem().Debug();
  }

  /**
   * Returns the path of this conglomerate.  The path returned here is only
   * valid if the conglomerate is open.
   */
  File getPath() {
    return path;
  }

  /**
   * Returns the name given to this conglomerate.
   */
  String getName() {
    return name;
  }


  // ---------- Conglomerate state methods ----------

  private byte[] reserved_buffer = new byte[64];

  private boolean updated_visible_tables = false;
  private boolean updated_dropped_tables = false;
  private boolean updated_conglomerate_state = false;

  /**
   * The conglomerate state contains 2 sector chains.  The list of all table
   * id's and table file names that are visible.  Also, index state
   * information for each table.
   */

  /**
   * Marks the given table id as committed dropped.
   */
  private void markAsCommittedDropped(int table_id) {
    MasterTableDataSource master_table = getMasterTable(table_id);
    // Add to the list of tables to drop
    if (!committed_dropped.contains(master_table.getFileName())) {
      committed_dropped.add(master_table.getFileName());
    }
  }

  /**
   * Updates the chain of visible tables.
   */
  private void writeVisibleTables() throws IOException {
    synchronized (commit_lock) {

      if (updated_visible_tables) {
        throw new Error("Updated visible tables information twice.");
      }
      updated_visible_tables = true;

      OutputStream sout = state_file.getSectorOutputStream();
      DataOutputStream dout = new DataOutputStream(sout);

      dout.writeInt(1);   // The version.

      int size = committed_tables.size();
      dout.writeInt(size);
      for (int i = 0; i < size; ++i) {
        MasterTableDataSource master =
                               (MasterTableDataSource) committed_tables.get(i);
        dout.writeInt(master.getTableID());
        dout.writeUTF(master.getFileName());
      }

      dout.close();

      // Update the index_header_data.
      int sector = state_file.getSectorOfLastOutputStream();
      ByteArrayUtil.setInt(sector, reserved_buffer, 4);
    }
  }

  /**
   * Updates the chain of dropped tables.
   */
  private void writeDroppedTables() throws IOException {
    synchronized (commit_lock) {

      if (updated_dropped_tables) {
        throw new Error("Updated dropped tables information twice.");
      }
      updated_dropped_tables = true;

      OutputStream sout = state_file.getSectorOutputStream();
      DataOutputStream dout = new DataOutputStream(sout);

      dout.writeInt(1);   // The version.

      int size = committed_dropped.size();
      dout.writeInt(size);
      for (int i = 0; i < size; ++i) {
        dout.writeUTF((String) committed_dropped.get(i));
      }

      dout.close();

      // Update the index_header_data.
      int sector = state_file.getSectorOfLastOutputStream();
      ByteArrayUtil.setInt(sector, reserved_buffer, 12);
    }
  }

  /**
   * Updates the chain of conglomerate state information.
   */
  private void writeConglomerateState() throws IOException {
    synchronized (commit_lock) {

      if (updated_conglomerate_state) {
        throw new Error("Updated conglomerate state information twice.");
      }
      updated_conglomerate_state = true;

      OutputStream sout = state_file.getSectorOutputStream();
      DataOutputStream dout = new DataOutputStream(sout);

      dout.writeInt(1);   // The version.

      dout.writeInt(cur_table_id);

      dout.close();

      // Update the index_header_data.
      int sector = state_file.getSectorOfLastOutputStream();
      ByteArrayUtil.setInt(sector, reserved_buffer, 8);
    }
  }

  /**
   * Commits the conglomerate state.  The conglomerate state is committed
   * by updating the records in the reserved area.
   */
  private void commitCurrentState() throws IOException {

    if (Debug().isInterestedIn(Lvl.INFORMATION)) {
      Debug().write(Lvl.INFORMATION, this,
                  "Conglomerate committing state: " + getName());
    }

    synchronized (commit_lock) {

      byte[] old_buf = new byte[64];
      state_file.readReservedBuffer(old_buf, 0, 64);

      // Update the current buffer to the conglomerate state.
      state_file.writeReservedBuffer(reserved_buffer, 0, 64);
      // Force the change to the file system.
      if (!getSystem().dontSynchFileSystem()) {
        state_file.hardSynch();
      }

      int new_tables_sector = ByteArrayUtil.getInt(reserved_buffer, 4);
      int new_state_sector = ByteArrayUtil.getInt(reserved_buffer, 8);
      int new_dropped_sector = ByteArrayUtil.getInt(reserved_buffer, 12);

      // Delete chains that are no longer referenced by the header.
      // Only delete sectors no longer referenced.
      int old_tables_sector = ByteArrayUtil.getInt(old_buf, 4);
      int old_state_sector = ByteArrayUtil.getInt(old_buf, 8);
      int old_dropped_sector = ByteArrayUtil.getInt(old_buf, 12);
      if (old_tables_sector != -1 && new_tables_sector != old_tables_sector) {
        state_file.deleteAcross(old_tables_sector);
      }
      if (old_state_sector != -1 && new_state_sector != old_state_sector) {
        state_file.deleteAcross(old_state_sector);
      }
      if (old_dropped_sector != -1 && new_dropped_sector != old_dropped_sector) {
        state_file.deleteAcross(old_dropped_sector);
      }

      // Reset the flags to indicate the state has been updated
      updated_visible_tables = false;
      updated_dropped_tables = false;
      updated_conglomerate_state = false;

    }

  }

  /**
   * Reads in the list of committed tables in this conglomerate.  This should
   * only be called during an 'open' like method.  This method fills the
   * 'committed_tables' and 'table_list' lists with the tables in this
   * conglomerate.
   */
  private void readVisibleTables() throws IOException {

    // Read the list of visible tables.
    int tables_sector = ByteArrayUtil.getInt(reserved_buffer, 4);
    InputStream sin = state_file.getSectorInputStream(tables_sector);
    DataInputStream din = new DataInputStream(sin);

    int vtver = din.readInt();   // The version.

    int size = din.readInt();
    // For each committed table,
    for (int i = 0 ; i < size; ++i) {
      int master_table_id = din.readInt();
      String file_name = din.readUTF();
      // Open the table and add to list of committed tables.
      MasterTableDataSource master =
                   new MasterTableDataSource(getSystem(), open_transactions);
      if (master.exists(getPath(), file_name)) {
        master.open(getPath(), file_name, read_only);
        committed_tables.add(master);
        table_list.add(master);
      }
      else {
        // If not exists, then generate an error message
        Debug().write(Lvl.ERROR, this,
                    "Couldn't find table source in path " + getPath() +
                    " filename: " + file_name + " table_id: " +
                    master_table_id);
      }
    }

    din.close();

  }

  /**
   * Checks the list of committed tables in this conglomerate.  This should
   * only be called during an 'check' like method.  This method fills the
   * 'committed_tables' and 'table_list' lists with the tables in this
   * conglomerate.
   */
  public void checkVisibleTables(UserTerminal terminal) throws IOException {
    // Read the list of visible tables.
    int tables_sector = ByteArrayUtil.getInt(reserved_buffer, 4);
    InputStream sin = state_file.getSectorInputStream(tables_sector);
    DataInputStream din = new DataInputStream(sin);

    int vtver = din.readInt();   // The version.

    int size = din.readInt();
    // For each committed table,
    for (int i = 0 ; i < size; ++i) {
      int master_table_id = din.readInt();
      String file_name = din.readUTF();
      // Open the table and add to list of committed tables.
      MasterTableDataSource master =
                 new MasterTableDataSource(getSystem(), open_transactions);
      if (master.exists(getPath(), file_name)) {
        master.checkAndRepair(getPath(), file_name, terminal);
        committed_tables.add(master);
        table_list.add(master);
      }
      else {
        // If not exists, then generate an error message
        Debug().write(Lvl.ERROR, this,
                    "Couldn't find table source in path " + getPath() +
                    " filename: " + file_name + " table_id: " +
                    master_table_id);
      }
    }

    din.close();
  }








  /**
   * Reads the conglerate state variables.
   * <p>
   * @param terminal the terminal to ask questions if problems are found.  If
   *   null then an exception is thrown if there are problems.
   */
  private void readConglomerateState() throws IOException {

    // Read the conglomerate state variables.
    int state_sector = ByteArrayUtil.getInt(reserved_buffer, 8);
    InputStream sin = state_file.getSectorInputStream(state_sector);
    DataInputStream din = new DataInputStream(sin);

    int csver = din.readInt();   // The version.

    // Read the current table id
    cur_table_id = din.readInt();

    din.close();

  }

  /**
   * Reads in the list of committed dropped tables on this conglomerate.  This
   * should only be called during an 'open' like method.  This method fills
   * the 'committed_dropped' and 'table_list' lists with the tables in this
   * conglomerate.
   * <p>
   * @param terminal the terminal to ask questions if problems are found.  If
   *   null then an exception is thrown if there are problems.
   */
  private void readDroppedTables() throws IOException {

    // Read the list of dropped tables.
    int dropped_sector = ByteArrayUtil.getInt(reserved_buffer, 12);
    if (dropped_sector > -1 && !read_only) {
      InputStream sin = state_file.getSectorInputStream(dropped_sector);
      DataInputStream din = new DataInputStream(sin);

      int dsver = din.readInt();  // The version.

      int size = din.readInt();
      // For each deleted table file name,
      for (int i = 0; i < size; ++i) {
        String file_name = din.readUTF();
        // Open the table and add to list of deleted tables.
        MasterTableDataSource master =
                  new MasterTableDataSource(getSystem(), open_transactions);
        if (master.exists(getPath(), file_name)) {
          master.open(getPath(), file_name, false);
          committed_dropped.add(file_name);
          table_list.add(master);
        }
        else {
//          ErrorState error = new ErrorState();
//          error.setString("Couldn't find table source in path " +
//                          getPath() + " filename: " + file_name);
//          error.setErrorCode("TABLE_NOT_EXIST");
//          error.setNotRecoverable();
//          return error;

          // If not exists, then generate an error message
          Debug().write(Lvl.ERROR, this,
                      "Couldn't find table source in path " + getPath() +
                      " filename: " + file_name);
        }
      }

      din.close();

    }

  }

  /**
   * Create the system tables that must be present in a conglomerates.  These
   * tables consist of contraint and table management data.
   */
  private void updateSystemTableSchema() {
    // Create the transaction
    Transaction transaction = createTransaction();

    DataTableDef table;

    table = new DataTableDef();
    table.setTableName(PRIMARY_INFO_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("id"));
    table.addColumn(DataTableColumnDef.createStringColumn("name"));
    table.addColumn(DataTableColumnDef.createStringColumn("schema"));
    table.addColumn(DataTableColumnDef.createStringColumn("table"));
    table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
    transaction.alterCreateTable(table, 187, 128);

    table = new DataTableDef();
    table.setTableName(FOREIGN_INFO_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("id"));
    table.addColumn(DataTableColumnDef.createStringColumn("name"));
    table.addColumn(DataTableColumnDef.createStringColumn("schema"));
    table.addColumn(DataTableColumnDef.createStringColumn("table"));
    table.addColumn(DataTableColumnDef.createStringColumn("ref_schema"));
    table.addColumn(DataTableColumnDef.createStringColumn("ref_table"));
    table.addColumn(DataTableColumnDef.createStringColumn("update_rule"));
    table.addColumn(DataTableColumnDef.createStringColumn("delete_rule"));
    table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
    transaction.alterCreateTable(table, 187, 128);

    table = new DataTableDef();
    table.setTableName(UNIQUE_INFO_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("id"));
    table.addColumn(DataTableColumnDef.createStringColumn("name"));
    table.addColumn(DataTableColumnDef.createStringColumn("schema"));
    table.addColumn(DataTableColumnDef.createStringColumn("table"));
    table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
    transaction.alterCreateTable(table, 187, 128);

    table = new DataTableDef();
    table.setTableName(CHECK_INFO_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("id"));
    table.addColumn(DataTableColumnDef.createStringColumn("name"));
    table.addColumn(DataTableColumnDef.createStringColumn("schema"));
    table.addColumn(DataTableColumnDef.createStringColumn("table"));
    table.addColumn(DataTableColumnDef.createStringColumn("expression"));
    table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
    table.addColumn(
            DataTableColumnDef.createBinaryColumn("serialized_expression"));
    transaction.alterCreateTable(table, 187, 128);

    table = new DataTableDef();
    table.setTableName(PRIMARY_COLS_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("pk_id"));
    table.addColumn(DataTableColumnDef.createStringColumn("column"));
    table.addColumn(DataTableColumnDef.createNumericColumn("seq_no"));
    transaction.alterCreateTable(table, 91, 128);

    table = new DataTableDef();
    table.setTableName(UNIQUE_COLS_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("un_id"));
    table.addColumn(DataTableColumnDef.createStringColumn("column"));
    table.addColumn(DataTableColumnDef.createNumericColumn("seq_no"));
    transaction.alterCreateTable(table, 91, 128);

    table = new DataTableDef();
    table.setTableName(FOREIGN_COLS_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("fk_id"));
    table.addColumn(DataTableColumnDef.createStringColumn("fcolumn"));
    table.addColumn(DataTableColumnDef.createStringColumn("pcolumn"));
    table.addColumn(DataTableColumnDef.createNumericColumn("seq_no"));
    transaction.alterCreateTable(table, 91, 128);

    table = new DataTableDef();
    table.setTableName(SCHEMA_INFO_TABLE);
    table.addColumn(DataTableColumnDef.createNumericColumn("id"));
    table.addColumn(DataTableColumnDef.createStringColumn("name"));
    table.addColumn(DataTableColumnDef.createStringColumn("type"));
    table.addColumn(DataTableColumnDef.createStringColumn("other"));
    transaction.alterCreateTable(table, 91, 128);

    // Stores misc variables of the database,
    table = new DataTableDef();
    table.setTableName(PERSISTENT_VAR_TABLE);
    table.addColumn(DataTableColumnDef.createStringColumn("variable"));
    table.addColumn(DataTableColumnDef.createStringColumn("value"));
    transaction.alterCreateTable(table, 91, 128);

    // Commit and close the transaction.
    try {
      transaction.closeAndCommit();
    }
    catch (TransactionException e) {
      Debug().writeException(e);
      throw new Error("Transaction Exception creating conglomerate.");
    }

  }

  /**
   * Populates the system table schema with initial data for an empty
   * conglomerate.  This sets up the standard variables and table
   * constraint data.
   */
  private void initializeSystemTableSchema() {
    // Create the transaction
    Transaction transaction = createTransaction();

    // Insert the two default schema names,
    transaction.createSchema(SYSTEM_SCHEMA, "SYSTEM");

    // -- Primary Keys --
    // The 'id' columns are primary keys on all the system tables,
    final String[] id_col = new String[] { "id" };
    transaction.addPrimaryKeyConstraint(PRIMARY_INFO_TABLE,
              id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PK_PK");
    transaction.addPrimaryKeyConstraint(FOREIGN_INFO_TABLE,
              id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_FK_PK");
    transaction.addPrimaryKeyConstraint(UNIQUE_INFO_TABLE,
              id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_PK");
    transaction.addPrimaryKeyConstraint(CHECK_INFO_TABLE,
              id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_CHECK_PK");
    transaction.addPrimaryKeyConstraint(SCHEMA_INFO_TABLE,
              id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_SCHEMA_PK");

    // -- Foreign Keys --
    // Create the foreign key references,
    final String[] fk_col = new String[1];
    final String[] fk_ref_col = new String[] { "id" };
    fk_col[0] = "pk_id";
    transaction.addForeignKeyConstraint(
              PRIMARY_COLS_TABLE, fk_col, PRIMARY_INFO_TABLE, fk_ref_col,
              Transaction.NO_ACTION, Transaction.NO_ACTION,
              Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PK_FK");
    fk_col[0] = "fk_id";
    transaction.addForeignKeyConstraint(
              FOREIGN_COLS_TABLE, fk_col, FOREIGN_INFO_TABLE, fk_ref_col,
              Transaction.NO_ACTION, Transaction.NO_ACTION,
              Transaction.INITIALLY_IMMEDIATE, "SYSTEM_FK_FK");
    fk_col[0] = "un_id";
    transaction.addForeignKeyConstraint(
              UNIQUE_COLS_TABLE, fk_col, UNIQUE_INFO_TABLE, fk_ref_col,
              Transaction.NO_ACTION, Transaction.NO_ACTION,
              Transaction.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_FK");

    // sUSRPKeyInfo 'schema', 'table' column is a unique set,
    // (You are only allowed one primary key per table).
    String[] columns = new String[] { "schema", "table" };
    transaction.addUniqueConstraint(PRIMARY_INFO_TABLE,
         columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PKEY_ST_UNIQUE");
    // sUSRSchemaInfo 'name' column is a unique column,
    columns = new String[] { "name" };
    transaction.addUniqueConstraint(SCHEMA_INFO_TABLE,
         columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_SCHEMA_UNIQUE");
    // sUSRPKeyInfo 'name' column is a unique column,
    transaction.addUniqueConstraint(PRIMARY_INFO_TABLE,
         columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PKEY_UNIQUE");
    // sUSRFKeyInfo 'name' column is a unique column,
    transaction.addUniqueConstraint(FOREIGN_INFO_TABLE,
         columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_FKEY_UNIQUE");
    // sUSRUniqueInfo 'name' column is a unique column,
    transaction.addUniqueConstraint(UNIQUE_INFO_TABLE,
         columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_UNIQUE");
    // sUSRCheckInfo 'name' column is a unique column,
    transaction.addUniqueConstraint(CHECK_INFO_TABLE,
         columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_CHECK_UNIQUE");

    // sUSRDatabaseVars 'variable' is unique
    columns = new String[] { "variable" };
    transaction.addUniqueConstraint(PERSISTENT_VAR_TABLE,
       columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_DATABASEVARS_UNIQUE");

    // Insert the version number of the database
    transaction.setPersistentVar("database.version", "1.1");

    // Commit and close the transaction.
    try {
      transaction.closeAndCommit();
    }
    catch (TransactionException e) {
      Debug().writeException(e);
      throw new Error("Transaction Exception initializing conglomerate.");
    }

  }











  // ---------- Private methods ----------



  /**
   * Returns the next unique table_id value for a new table and updates the
   * conglomerate state information as appropriate.
   */
  private int nextUniqueTableID() throws IOException {
    int next_table_id = cur_table_id;
    ++cur_table_id;

    // Write the table id change to the conglomerate.
    writeConglomerateState();
    commitCurrentState();

    return next_table_id;
  }


  /**
   * Sets up the internal state of this object.
   */
  private void setupInternal() {
    commit_id = 0;
    transaction_id = 0;
    committed_tables = new ArrayList();
    table_list = new ArrayList();
  }

  // ---------- Public methods ----------

  /**
   * Creates a new conglomerate at the given path in the file system.  This
   * must be an empty directory where files can be stored.  This will create
   * the conglomerate and exit in an open (read/write) state.
   */
  public void create(File path, String name) throws IOException {
    this.path = path;
    this.read_only = read_only;

    File state_fn = new File(path, name + STATE_EXT);
    if (state_fn.exists()) {
      throw new IOException("Conglomerate already exists: " + state_fn);
    }

    // Create the database lock file.
    File flock_fn = new File(path, name + FLOCK_EXT);
//#IFDEF(NO_1.1)
    // Atomically create the file,
    flock_fn.createNewFile();
    // Set it to delete on normal exit of the JVM.
    flock_fn.deleteOnExit();
//#ENDIF
    // Open up a stream and lock it in the OS
    conglomerate_file_lock = new FileOutputStream(flock_fn);

    // Create/Open the state file.
    state_file = new FixedSizeDataStore(state_fn, 507, Debug());
    state_file.open(false);

    // Intialize the reserved area to a known state.
    byte[] buf = new byte[64];
    ByteArrayUtil.setInt(-1, buf, 4);
    ByteArrayUtil.setInt(-1, buf, 8);
    ByteArrayUtil.setInt(-1, buf, 12);
    ByteArrayUtil.setInt(-1, buf, 16);
    ByteArrayUtil.setInt(-1, buf, 20);
    state_file.writeReservedBuffer(buf, 0, 64);
    state_file.readReservedBuffer(reserved_buffer, 0, 64);

    setupInternal();

    // Write blank information to the conglomerate.
    writeVisibleTables();
    writeConglomerateState();
    writeDroppedTables();
    commitCurrentState();

    // Creates the conglomerate system tables.
    updateSystemTableSchema();
    initializeSystemTableSchema();

  }

  /**
   * Opens a conglomerate that can be found at the given path in the file
   * system.  If the conglomerate does not exist then an IOException is
   * generated.  Once a conglomerate is open, we may start opening transactions
   * and altering the data within it.
   * <p>
   * If the 'read_only' boolean is true then the conglomerate is opened as
   * a read only source and may not be altered in any way.
   */
  public void open(File path, String name, boolean read_only)
                                                          throws IOException {
    this.path = path;
    this.name = name;
    this.read_only = read_only;

    File state_fn = new File(path, name + STATE_EXT);
    if (!state_fn.exists()) {
      throw new IOException("Conglomerate doesn't exist: " + state_fn);
    }

    // Check the file lock
    if (!read_only) {
      // We don't bother with file lock if opening conglomerate read only.
      File flock_fn = new File(path, name + FLOCK_EXT);
      if (flock_fn.exists()) {
        // Okay, the file lock exists.  This means either an extremely bad
        // crash or there is another database locked on the files.  If we can
        // delete the lock then we can go on.
        Debug().write(Lvl.WARNING, this,
                      "File lock file exists: " + flock_fn);
        boolean deleted = false;
        deleted = flock_fn.delete();
        if (!deleted) {
          // If we couldn't delete, then most likely database being used.
          System.err.println("\n" +
            "I couldn't delete the file lock for Database '" + name + "'.\n" +
            "This most likely means the database is open and being used by\n" +
            "another process.\n" +
            "The lock file is: " + flock_fn + "\n\n");
          throw new IOException("Couldn't delete conglomerate file lock.");
        }
      }
//#IFDEF(NO_1.1)
      // Atomically create the file,
      flock_fn.createNewFile();
      // Set it to delete on normal exit of the JVM.
      flock_fn.deleteOnExit();
//#ENDIF
      // Open up a stream and lock it in the OS
      conglomerate_file_lock = new FileOutputStream(flock_fn);

    }

    // Open the state file.
    state_file = new FixedSizeDataStore(state_fn, 507, Debug());
    state_file.open(read_only);

    setupInternal();

    // Read the header.
//    reserved_buffer = new byte[64];
    state_file.readReservedBuffer(reserved_buffer, 0, 64);

    readVisibleTables();
    readConglomerateState();
    readDroppedTables();

    // We possibly have things to clean up if there are deleted columns.
    cleanUpConglomerate();

  }

  /**
   * Closes this conglomerate.  The conglomerate must be open for it to be
   * closed.  When closed, any use of this object is undefined.
   */
  public void close() throws IOException {
    synchronized (commit_lock) {

      // We possibly have things to clean up.
      cleanUpConglomerate();

      // Go through and close all the committed tables.
      int size = table_list.size();
      for (int i = 0; i < size; ++i) {
        MasterTableDataSource master =
                                    (MasterTableDataSource) table_list.get(i);
        master.close();
      }

      state_file.close();

      path = null;
      committed_tables = null;
      table_list = null;

    }

    if (conglomerate_file_lock != null) {
      conglomerate_file_lock.close();
    }

  }

  /**
   * Deletes and closes the conglomerate.  This will delete all the files in
   * the file system associated with this conglomerate, so this method should
   * be used with care.
   * <p>
   * WARNING: Will result in total loss of all data stored in the conglomerate.
   */
  public void delete() throws IOException {
    synchronized (commit_lock) {

      // We possibly have things to clean up.
      cleanUpConglomerate();

      // Go through and delete and close all the committed tables.
      int size = table_list.size();
      for (int i = 0; i < size; ++i) {
        MasterTableDataSource master =
                                    (MasterTableDataSource) table_list.get(i);
        master.notifyDropped();
        master.close();
      }

      // Delete the state file
      state_file.close();
      state_file.delete();

      // Invalidate this object
      path = null;
      committed_tables = null;
      table_list = null;

    }

    // Close the file lock (will delete when the JVM exits).
    if (conglomerate_file_lock != null) {
      conglomerate_file_lock.close();
    }
  }

  /**
   * Returns true if the conglomerate is closed.
   */
  public boolean isClosed() {
    synchronized (commit_lock) {
      return path == null;
    }
  }


  /**
   * Returns true if the conglomerate exists in the file system and can
   * be opened.
   */
  public boolean exists(File path, String name) throws IOException {
    File state_fn = new File(path, name + STATE_EXT);
    return state_fn.exists();
  }

  /**
   * Makes a complete copy of this database at the given path in the file
   * system.  This may take a while to complete, and the conglomerate is put
   * under a 'commit_lock' while this is in progress.  This means that new
   * transactions will not be able to be created or committed.
   * <p>
   * Open result sets will be able to continue to download information, and
   * transaction operations will work provided a table is not being copied at
   * the time.
   * <p>
   * The conglomerate must be open before this method is called.
   */
  public void liveCopyTo(File path) throws IOException {
    // We are under a commit lock,
    synchronized (commit_lock) {

      // Copy the state file first
      state_file.copyTo(path);
      // Copy all the tables in the conglomerate.
      int size = table_list.size();
      for (int i = 0; i < size; ++i) {
        MasterTableDataSource master =
                                    (MasterTableDataSource) table_list.get(i);
        master.copyTo(path);
      }
      // That's it...
      getSystem().stats().increment("TableDataConglomerate.liveCopies");

    }
  }

  /**
   * Attempts to flush all the journal entries in the given committed table.
   * This operation will only flush journal entries up to the commit id of
   * the first open transaction.
   * <p>
   * NOTE: This does not write any information to disk - it only affects in
   *   memory state.
   */
  public void flushJournals(String schema_name, String table_name) {
    synchronized (commit_lock) {
      long min_commit_id = open_transactions.minimumCommitID(null);
      MasterTableDataSource master =
                            getCommittedMasterTable(schema_name, table_name);
      master.mergeJournalChanges(min_commit_id);
    }
  }

  // ---------- Diagnostic and repair ----------

  /**
   * Returns a RawDiagnosticTable object that is used for diagnostics of the
   * table with the given file name.
   */
  public RawDiagnosticTable getDiagnosticTable(String table_file_name) {
    synchronized (commit_lock) {
      for (int i = 0; i < table_list.size(); ++i) {
        MasterTableDataSource master =
                                    (MasterTableDataSource) table_list.get(i);
        if (master.getFileName().equals(table_file_name)) {
          return master.getRawDiagnosticTable();
        }
      }
    }
    return null;
  }

  /**
   * Returns the list of file names for all tables in this conglomerate.
   */
  public String[] getAllTableFileNames() {
    synchronized (commit_lock) {
      String[] list = new String[table_list.size()];
      for (int i = 0; i < table_list.size(); ++i) {
        MasterTableDataSource master =
                                    (MasterTableDataSource) table_list.get(i);
        list[i] = master.getFileName();
      }
      return list;
    }
  }


  // ---------- Transactional management ----------

  /**
   * Starts a new transaction.  The Transaction object returned by this
   * method is used to read the contents of the database at the time
   * the transaction was started.  It is also used if any modifications are
   * required to be made.
   */
  public Transaction createTransaction() {
    long this_transaction_id;
    long this_commit_id;
    ArrayList this_committed_tables = new ArrayList();

    // Don't let a commit happen while we are looking at this.
    synchronized (commit_lock) {

//      ++transaction_lock;
      ++transaction_id;
      this_transaction_id = transaction_id;
      this_commit_id = commit_id;
//      System.out.println("CREATING TRANSACTION: " + commit_id);
      this_committed_tables.addAll(committed_tables);

      // Create a set of IndexSet for all the tables in this transaction.
      int sz = this_committed_tables.size();
      ArrayList index_info = new ArrayList(sz);
      for (int i = 0; i < sz; ++i) {
        MasterTableDataSource mtable =
                       (MasterTableDataSource) this_committed_tables.get(i);
        index_info.add(mtable.createIndexSet());
      }

      // Create the transaction and record it in the open transactions list.
      Transaction t = new Transaction(this, this_transaction_id,
                        this_commit_id, this_committed_tables, index_info);
      open_transactions.addTransaction(t);
      return t;

    }

  }

  /**
   * This is called to notify the conglomerate that the transaction has
   * closed.  This is always called from either the rollback or commit method
   * of the transaction object.
   * <p>
   * NOTE: This increments 'commit_id' and requires that the conglomerate is
   *   commit locked.
   */
  private void closeTransaction(Transaction transaction) {
    boolean last_transaction = false;
    // Closing must happen under a commit lock.
    synchronized (commit_lock) {
      open_transactions.removeTransaction(transaction);
      // Increment the commit id.
      ++commit_id;
      // Was that the last transaction?
      last_transaction = open_transactions.count() == 0;
    }

    // If last transaction then schedule a clean up event.
    if (last_transaction) {
      getSystem().postEvent(5, conglomerate_cleanup_event);
    }

  }


  /**
   * Closes and drops the MasterTableDataSource.  This should only be called
   * from the clean up method (cleanUpConglomerate()).
   * <p>
   * Returns true if the drop succeeded.  A drop may fail if, for example, the
   * roots of the table are locked.
   */
  private boolean closeAndDropTable(String table_file_name) throws IOException {
    // Find the table with this file name.
    for (int i = 0; i < table_list.size(); ++i) {
      MasterTableDataSource t = (MasterTableDataSource) table_list.get(i);
      if (t.getFileName().equals(table_file_name)) {
        // Close and remove from the list.
        if (t.isRootLocked()) {
          // We can't drop a table that has roots locked..
          return false;
        }

        // This drops if the table has been marked as being dropped.
        t.notifyDropped();
        t.close();
        table_list.remove(i);
        return true;
      }
    }
    return false;
//    // If not found in 'table_list' then we have to open it.
//    MasterTableDataSource t = new MasterTableDataSource(open_transactions);
//    t.open(path, table_file_name, false);
//    // Notify that it's been dropped
//    t.notifyDropped();
//    // And close it.
//    t.close();
  }

  /**
   * Cleans up the conglomerate by deleting all tables marked as deleted.
   * This should be called when the conglomerate is opened, shutdown and
   * when there are no transactions open.
   */
  private void cleanUpConglomerate() throws IOException {
    synchronized (commit_lock) {
      if (isClosed()) {
        return;
      }

      // If no open transactions on the database, then clean up.
      if (open_transactions.count() == 0) {
        if (committed_dropped.size() > 0) {
          int drop_count = 0;
          for (int i = committed_dropped.size() - 1; i >= 0; --i) {
            String fn = (String) committed_dropped.get(i);
            boolean dropped = closeAndDropTable(fn);
            // If we managed to drop the table, remove from the list.
            if (dropped) {
              committed_dropped.remove(i);
              ++drop_count;
            }
          }

          // If we dropped a table, commit an update to the conglomerate state.
          if (drop_count > 0) {
            writeDroppedTables();
            commitCurrentState();
          }
        }

      }
    }
  }

  /**
   * An event that cleans up the conglomerate, but only if there are no
   * open transactions.
   */
  private class ConglomerateCleanUpEvent implements Runnable {
    public void run() {
      try {
        cleanUpConglomerate();
      }
      catch (IOException e) {
        Debug().writeException(e);
      }
    }
  }
  private Object conglomerate_cleanup_event;

  // ---------- Detection of constraint violations ----------

  /**
   * A variable resolver for a single row of a table source.  Used when
   * evaluating a check constraint for newly added row.
   */
  private static class TableRowVariableResolver implements VariableResolver {

    private TableDataSource table;
    private int row_index = -1;

    public TableRowVariableResolver(TableDataSource table, int row) {
      this.table = table;
      this.row_index = row;
    }

    private int findColumnName(Variable variable) {
      int col_index = table.getDataTableDef().findColumnName(
                                                        variable.getName());
      if (col_index == -1) {
        throw new Error("Can't find column: " + variable);
      }
      return col_index;
    }

    // --- Implemented ---

    public int setID() {
      return row_index;
    }

    public Object resolve(Variable variable) {
      int col_index = findColumnName(variable);
      DataCell cell = table.getCellContents(col_index, row_index);
      Object ob = cell.getCell();
      if (ob == null) {
        return Expression.NULL_OBJ;
      }
      return ob;
    }

    public Class classType(Variable variable) {
      int col_index = findColumnName(variable);
      return table.getDataTableDef().columnAt(col_index).classType();
    }

  }

  /**
   * Convenience, converts a String[] array to a comma deliminated string
   * list.
   */
  private static String stringColumnList(String[] list) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < list.length - 1; ++i) {
      buf.append(list[i]);
    }
    buf.append(list[list.length - 1]);
    return new String(buf);
  }

  /**
   * Convenience, returns either 'Immediate' or 'Deferred' dependant on the
   * deferred short.
   */
  private static String deferredString(short deferred) {
    switch(deferred) {
      case(Transaction.INITIALLY_IMMEDIATE):
        return "Immediate";
      case(Transaction.INITIALLY_DEFERRED):
        return "Deferred";
      default:
        throw new Error("Unknown deferred string.");
    }
  }

  /**
   * Returns a list of column indices into the given DataTableDef for the
   * given column names.
   */
  private static int[] findColumnIndices(
                                   DataTableDef table_def, String[] cols) {
    // Resolve the list of column names to column indexes
    int[] col_indexes = new int[cols.length];
    for (int i = 0; i < cols.length; ++i) {
      col_indexes[i] = table_def.findColumnName(cols[i]);
    }
    return col_indexes;
  }

  /**
   * Checks the uniqueness of the columns in the row of the table.  If
   * the given column information in the row data is not unique then it
   * returns false.
   */
  private static boolean isUniqueColumns(
                       TableDataSource table, int rindex, String[] cols) {

    DataTableDef table_def = table.getDataTableDef();
    // 'identical_rows' keeps a tally of the rows that match our added cell.
    IntegerVector identical_rows = null;

    // Resolve the list of column names to column indexes
    int[] col_indexes = findColumnIndices(table_def, cols);

    for (int i = 0; i < col_indexes.length; ++i) {

      int col_index = col_indexes[i];

      // Get the column definition and the cell being inserted,
      DataTableColumnDef column_def = table_def.columnAt(col_index);
      DataCell cell = table.getCellContents(col_index, rindex);

      // We are assured of uniqueness if 'identical_rows != null &&
      // identical_rows.size() == 0'  This is because 'identical_rows' keeps
      // a running tally of the rows in the table that contain unique columns
      // whose cells match the record being added.

      if (identical_rows == null || identical_rows.size() > 0) {

        // Ask SelectableScheme to return pointers to row(s) if there is
        // already a cell identical to this in the table.

        SelectableScheme ss = table.getColumnScheme(col_index);
        IntegerVector ivec = ss.selectEqual(cell);

        // If 'identical_rows' hasn't been set up yet then set it to 'ivec'
        // (the list of rows where there is a cell which is equal to the one
        //  being added)
        // If 'identical_rows' has been set up, then perform an
        // 'intersection' operation on the two lists (only keep the numbers
        // that are repeated in both lists).  Therefore we keep the rows
        // that match the row being added.

        if (identical_rows == null) {
          identical_rows = ivec;
        }
        else {
          ivec.quickSort();
          int row_index = identical_rows.size() - 1;
          while (row_index >= 0) {
            int val = identical_rows.intAt(row_index);
            int found_index = ivec.sortedIndexOf(val);
            // If we _didn't_ find the index in the array
            if (found_index >= ivec.size() ||
                ivec.intAt(found_index) != val) {
              identical_rows.removeIntAt(row_index);
            }
            --row_index;
          }
        }

      }

    } // for each column

    // If there is 1 (the row we added) then we are unique, otherwise we are
    // not.
    if (identical_rows != null) {
      int sz = identical_rows.size();
      if (sz == 1) {
        return true;
      }
      if (sz > 1) {
        return false;
      }
      else if (sz == 0) {
        throw new Error("Assertion failed: We must be able to find the " +
                        "row we are testing uniqueness against!");
      }
    }
    return true;

  }


  /**
   * Counts the number of keys found in the given table.  The keys are
   * in the given column indices, and the key is in the 'key' array.  This
   * is used to count the number of keys found in a table for constraint
   * violation checking.
   */
  private static int countKeys(TableDataSource t2, int[] col2_indexes,
                               DataCell[] key_value) {

    int key_size = key_value.length;
    // Now query table 2 to determine if the key values are present.
    // Use index scan on first key.
    SelectableScheme ss = t2.getColumnScheme(col2_indexes[0]);
    IntegerVector list = ss.selectEqual(key_value[0]);
    if (key_size > 1) {
      // Full scan for the rest of the columns
      int sz = list.size();
      // For each element of the list
      for (int i = sz - 1; i >= 0; --i) {
        int r_index = list.intAt(i);
        // For each key in the column list
        for (int c = 1; c < key_size; ++c) {
          int col_index = col2_indexes[c];
          DataCell c_value = key_value[c];
          if (c_value.compareTo(t2.getCellContents(col_index, r_index)) != 0) {
            // If any values in the key are not equal set this flag to false
            // and remove the index from the list.
            list.removeIntAt(i);
            // Break the for loop
            break;
          }
        }
      }
    }
    return list.size();

  }

  /**
   * Finds the number of rows that are referenced between the given row of
   * table1 and that match table2.  This method is used to determine if
   * there are referential links.
   * <p>
   * If this method returns -1 it means the value being searched for is NULL
   * therefore we can't determine if there are any referenced links.
   * <p>
   * HACK: If 'check_source_table_key' is set then the key is checked for in
   * the source table and if it exists returns 0.  Otherwise it looks for
   * references to the key in table2.
   */
  private static int rowCountOfReferenceTable(
                 Transaction transaction, TableDataSourceFactory factory,
                 int row_index, TableName table1, String[] cols1,
                                TableName table2, String[] cols2,
                                boolean check_source_table_key) {

    // Get the tables
    TableDataSource t1 = factory.get(table1);
    TableDataSource t2 = factory.get(table2);
    // The table defs
    DataTableDef dtd1 = t1.getDataTableDef();
    DataTableDef dtd2 = t2.getDataTableDef();
    // Resolve the list of column names to column indexes
    int[] col1_indexes = findColumnIndices(dtd1, cols1);
    int[] col2_indexes = findColumnIndices(dtd2, cols2);

    int key_size = col1_indexes.length;
    // Get the data from table1
    DataCell[] key_value = new DataCell[key_size];
    int null_count = 0;
    for (int n = 0; n < key_size; ++n) {
      key_value[n] = t1.getCellContents(col1_indexes[n], row_index);
      if (key_value[n].isNull()) {
        ++null_count;
      }
    }

    // If we are searching for null then return -1;
    if (null_count == key_size) {
      return -1;
    }

    // HACK: This is a hack.  The purpose is if the key exists in the source
    //   table we return 0 indicating to the delete check that there are no
    //   references and it's valid.  To the semantics of the method this is
    //   incorrect.
    if (check_source_table_key) {
      int key_count = countKeys(t1, col1_indexes, key_value);
      if (key_count > 0) {
        return 0;
      }
    }

    return countKeys(t2, col2_indexes, key_value);
  }


  /**
   * Checks that the nullibility and class of the fields in the given
   * rows are valid.  Should be used as part of the insert procedure.
   */
  static void checkFieldConstraintViolations(Transaction transaction,
                                  TableDataSource table, int[] row_indices) {

    // Quick exit case
    if (row_indices == null || row_indices.length == 0) {
      return;
    }

    // Check for any bad cells - which are either cells that are 'null' in a
    // column declared as 'not null', or duplicated in a column declared as
    // unique.

    DataTableDef table_def = table.getDataTableDef();
    TableName table_name = table_def.getTableName();

    // Check not-null columns are not null.  If they are null, throw an
    // error.  Additionally check that JAVA_OBJECT columns are correctly
    // typed.

    // Check each field of the added rows
    int len = table_def.columnCount();
    for (int i = 0; i < len; ++i) {

      // Get the column definition and the cell being inserted,
      DataTableColumnDef column_def = table_def.columnAt(i);
      // For each row added to this column
      for (int rn = 0; rn < row_indices.length; ++rn) {
        DataCell cell = table.getCellContents(i, row_indices[rn]);

        // Check: Column defined as not null and cell being inserted is
        // not null.
        if (column_def.isNotNull() && cell.isNull()) {
          throw new DatabaseConstraintViolationException(
              DatabaseConstraintViolationException.NULLABLE_VIOLATION,
              "You tried to add 'null' cell to column '" +
              table_def.columnAt(i).getName() +
              "' which is declared as 'not_null'");
        }

        // Check: If column is a java object, then deserialize and check the
        //        object is an instance of the class constraint,
        if (!cell.isNull() &&
            column_def.getSQLType() ==
                           com.mckoi.database.global.SQLTypes.JAVA_OBJECT) {
          String class_constraint = column_def.getClassConstraint();
          // Everything is derived from java.lang.Object so this optimization
          // will not cause an object deserialization.
          if (!class_constraint.equals("java.lang.Object")) {
            Object ob = ((SerializedObjectDataCell) cell).deserialize();
            if (!ob.getClass().isAssignableFrom(
                        column_def.getClassConstraintAsClass())) {
              throw new DatabaseConstraintViolationException(
                DatabaseConstraintViolationException.JAVA_TYPE_VIOLATION,
                "The Java object being inserted is not derived from the " +
                "class constraint defined for the column (" +
                class_constraint + ")");
            }
          }
        }

      } // For each row being added

    } // for each column

  }

  /**
   * Performs constraint violation checks on an addition of the given set of
   * row indices into the TableDataSource in the given transaction.  If a
   * violation is detected a DatabaseConstraintViolationException is thrown.
   * <p>
   * If deferred = IMMEDIATE only immediate constraints are tested.  If
   * deferred = DEFERRED all constraints are tested.
   *
   * @param transaction the Transaction instance used to determine table
   *   constraints.
   * @param table the table to test
   * @param row_indices the list of rows that were added to the table.
   * @param deferred '1' indicates Transaction.IMMEDIATE,
   *   '2' indicates Transaction.DEFERRED.
   */
  static void checkAddConstraintViolations( Transaction transaction,
           TableDataSourceFactory factory, TableDataSource table,
           int[] row_indices, short deferred) {

    String cur_schema = table.getDataTableDef().getSchema();
    QueryContext context = new SystemQueryContext(transaction, cur_schema);

    // Quick exit case
    if (row_indices == null || row_indices.length == 0) {
      return;
    }

    DataTableDef table_def = table.getDataTableDef();
    TableName table_name = table_def.getTableName();

    // ---- Constraint checking ----

    // Check any primary key constraint.
    Transaction.ColumnGroup primary_key =
                        transaction.queryTablePrimaryKeyGroup(table_name);
    if (primary_key != null &&
        (deferred == Transaction.INITIALLY_DEFERRED ||
         primary_key.deferred == Transaction.INITIALLY_IMMEDIATE)) {

      // For each row added to this column
      for (int rn = 0; rn < row_indices.length; ++rn) {
        if (!isUniqueColumns(table, row_indices[rn],
                             primary_key.columns)) {
          throw new DatabaseConstraintViolationException(
            DatabaseConstraintViolationException.PRIMARY_KEY_VIOLATION,
            deferredString(deferred) + " primary Key constraint violation (" +
            primary_key.name + ") Columns = ( " +
            stringColumnList(primary_key.columns) +
            " ) Table = ( " + table_name.toString() + " )");
        }
      } // For each row being added

    }

    // Check any unique constraints.
    Transaction.ColumnGroup[] unique_constraints =
                   transaction.queryTableUniqueGroups(table_name);
    for (int i = 0; i < unique_constraints.length; ++i) {
      Transaction.ColumnGroup unique = unique_constraints[i];
      if (deferred == Transaction.INITIALLY_DEFERRED ||
          unique.deferred == Transaction.INITIALLY_IMMEDIATE) {

        // For each row added to this column
        for (int rn = 0; rn < row_indices.length; ++rn) {
          if (!isUniqueColumns(table, row_indices[rn], unique.columns)) {
            throw new DatabaseConstraintViolationException(
              DatabaseConstraintViolationException.UNIQUE_VIOLATION,
              deferredString(deferred) + " unique constraint violation (" +
              unique.name + ") Columns = ( " +
              stringColumnList(unique.columns) + " ) Table = ( " +
              table_name.toString() + " )");
          }
        } // For each row being added

      }
    }

    // Check any foreign key constraints.
    // This ensures all foreign references in the table are referenced
    // to valid records.
    Transaction.ColumnGroupReference[] foreign_constraints =
                     transaction.queryTableForeignKeyReferences(table_name);
    for (int i = 0; i < foreign_constraints.length; ++i) {
      Transaction.ColumnGroupReference ref = foreign_constraints[i];
      if (deferred == Transaction.INITIALLY_DEFERRED ||
          ref.deferred == Transaction.INITIALLY_IMMEDIATE) {
        // For each row added to this column
        for (int rn = 0; rn < row_indices.length; ++rn) {
          // Make sure the referenced record exists

          // Return the count of records where the given row of
          //   table_name(columns, ...) IN
          //                    ref_table_name(ref_columns, ...)
          int row_count = rowCountOfReferenceTable(transaction,
                                     factory, row_indices[rn],
                                     ref.key_table_name, ref.key_columns,
                                     ref.ref_table_name, ref.ref_columns,
                                     false);
          if (row_count == -1) {
            // foreign key is NULL
          }
          if (row_count == 0) {
            throw new DatabaseConstraintViolationException(
              DatabaseConstraintViolationException.FOREIGN_KEY_VIOLATION,
              deferredString(deferred)+" foreign key constraint violation (" +
              ref.name + ") Columns = " +
              ref.key_table_name.toString() + "( " +
              stringColumnList(ref.key_columns) + " ) -> " +
              ref.ref_table_name.toString() + "( " +
              stringColumnList(ref.ref_columns) + " )");
          }
        } // For each row being added.
      }
    }

    // Any general checks of the inserted data
    Transaction.CheckExpression[] check_constraints =
                     transaction.queryTableCheckExpressions(table_name);

    // The TransactionSystem object
    TransactionSystem system = transaction.getSystem();

    // For each check constraint, check that it evaluates to true.
    for (int i = 0; i < check_constraints.length; ++i) {
      Transaction.CheckExpression check = check_constraints[i];
      if (deferred == Transaction.INITIALLY_DEFERRED ||
          check.deferred == Transaction.INITIALLY_IMMEDIATE) {

        check = system.prepareTransactionCheckConstraint(table_def, check);
        Expression exp = check.expression;

//        // Resolve the expression to this table and row and evaluate the
//        // check constraint.
//        Expression exp = check.expression;
//        table_def.resolveColumns(ignore_case, exp);
//        try {
//          // Prepare the functions
//          // ISSUE: Should we back reference to the DatabaseSystem layer?
//          //   This would probably be better as a plug-in.
//          exp.prepare(expression_preparer);
//        }
//        catch (Exception e) {
//          Debug().writeException(e);
//          throw new Error(e.getMessage());
//        }

        // For each row being added to this column
        for (int rn = 0; rn < row_indices.length; ++rn) {
          TableRowVariableResolver resolver =
                        new TableRowVariableResolver(table, row_indices[rn]);
          Object ob = exp.evaluate(null, resolver, context);

          if (ob instanceof Boolean) {
            if (ob.equals(Boolean.FALSE)) {
              // Evaluated to false so don't allow this row to be added.
              throw new DatabaseConstraintViolationException(
                 DatabaseConstraintViolationException.CHECK_VIOLATION,
                 deferredString(deferred) + " check constraint violation (" +
                 check.name + ") - '" + exp.text() +
                 "' evaluated to false for inserted/updated row.");
            }
          }
          else {
            // NOTE: This error will pass the row by default
            transaction.Debug().write(Lvl.ERROR,
              TableDataConglomerate.class,
              deferredString(deferred) + " check constraint violation (" +
              check.name + ") - '" + exp.text() +
              "' returned a non boolean result.");
          }
        } // For each row being added

      }
    }



  }

  /**
   * Performs constraint violation checks on an addition of the given
   * row index into the TableDataSource in the given transaction.  If a
   * violation is detected a DatabaseConstraintViolationException is thrown.
   * <p>
   * If deferred = IMMEDIATE only immediate constraints are tested.  If
   * deferred = DEFERRED all constraints are tested.
   *
   * @param transaction the Transaction instance used to determine table
   *   constraints.
   * @param table the table to test
   * @param row_index the row that was added to the table.
   * @param deferred '1' indicates Transaction.IMMEDIATE,
   *   '2' indicates Transaction.DEFERRED.
   */
  static void checkAddConstraintViolations(Transaction transaction,
             TableDataSourceFactory factory, TableDataSource table,
             int row_index, short deferred) {
    checkAddConstraintViolations(transaction, factory, table,
                                 new int[] { row_index }, deferred);
  }

  /**
   * Performs constraint violation checks on a removal of the given set of
   * row indexes from the TableDataSource in the given transaction.  If a
   * violation is detected a DatabaseConstraintViolationException is thrown.
   * <p>
   * If deferred = IMMEDIATE only immediate constraints are tested.  If
   * deferred = DEFERRED all constraints are tested.
   *
   * @param transaction the Transaction instance used to determine table
   *   constraints.
   * @param table the table to test
   * @param row_indices the set of rows that were removed from the table.
   * @param deferred '1' indicates Transaction.IMMEDIATE,
   *   '2' indicates Transaction.DEFERRED.
   */
  static void checkRemoveConstraintViolations(Transaction transaction,
           TableDataSourceFactory factory, TableDataSource table,
           int[] row_indices, short deferred) {

    // Quick exit case
    if (row_indices == null || row_indices.length == 0) {
      return;
    }

    DataTableDef table_def = table.getDataTableDef();
    TableName table_name = table_def.getTableName();

    // Check any imported foreign key constraints.
    // This ensures that a referential reference can not be removed making
    // it invalid.
    Transaction.ColumnGroupReference[] foreign_constraints =
               transaction.queryTableImportedForeignKeyReferences(table_name);
    for (int i = 0; i < foreign_constraints.length; ++i) {
      Transaction.ColumnGroupReference ref = foreign_constraints[i];
      if (deferred == Transaction.INITIALLY_DEFERRED ||
          ref.deferred == Transaction.INITIALLY_IMMEDIATE) {
        // For each row added to this column
        for (int rn = 0; rn < row_indices.length; ++rn) {
          // Make sure the referenced record exists

          // Return the count of records where the given row of
          //   ref_table_name(columns, ...) IN
          //                    table_name(ref_columns, ...)
          int row_count = rowCountOfReferenceTable(transaction,
                                     factory, row_indices[rn],
                                     ref.ref_table_name, ref.ref_columns,
                                     ref.key_table_name, ref.key_columns,
                                     true);
          // There must be 0 references otherwise the delete isn't allowed to
          // happen.
          if (row_count != 0) {
            throw new DatabaseConstraintViolationException(
              DatabaseConstraintViolationException.FOREIGN_KEY_VIOLATION,
              deferredString(deferred)+" foreign key constraint violation " +
              "on delete (" +
              ref.name + ") Columns = " +
              ref.key_table_name.toString() + "( " +
              stringColumnList(ref.key_columns) + " ) -> " +
              ref.ref_table_name.toString() + "( " +
              stringColumnList(ref.ref_columns) + " )");
          }
        } // For each row being added.
      }
    }

  }

  /**
   * Performs constraint violation checks on a removal of the given
   * row index from the TableDataSource in the given transaction.  If a
   * violation is detected a DatabaseConstraintViolationException is thrown.
   * <p>
   * If deferred = IMMEDIATE only immediate constraints are tested.  If
   * deferred = DEFERRED all constraints are tested.
   *
   * @param transaction the Transaction instance used to determine table
   *   constraints.
   * @param table the table to test
   * @param row_index the row that was removed from the table.
   * @param deferred '1' indicates Transaction.IMMEDIATE,
   *   '2' indicates Transaction.DEFERRED.
   */
  static void checkRemoveConstraintViolations(Transaction transaction,
              TableDataSourceFactory factory, TableDataSource table,
              int row_index, short deferred) {
    checkRemoveConstraintViolations(transaction, factory, table,
                                    new int[] { row_index }, deferred);
  }

  /**
   * Performs constraint violation checks on all the rows in the given
   * table.  If a violation is detected a DatabaseConstraintViolationException
   * is thrown.
   * <p>
   * This method is useful when the constraint schema of a table changes and
   * we need to check existing data in a table is conformant with the new
   * constraint changes.
   * <p>
   * If deferred = IMMEDIATE only immediate constraints are tested.  If
   * deferred = DEFERRED all constraint are tested.
   */
  static void checkAllAddConstraintViolations(Transaction transaction,
               TableDataSourceFactory factory, TableDataSource table,
               short deferred) {
    // Get all the rows in the table
    int[] rows = new int[table.getRowCount()];
    RowEnumeration row_enum = table.rowEnumeration();
    int p = 0;
    while (row_enum.hasMoreRows()) {
      rows[p] = row_enum.nextRowIndex();
      ++p;
    }
    // Check the constraints of all the rows in the table.
    checkAddConstraintViolations(transaction, factory, table,
                                 rows, Transaction.INITIALLY_DEFERRED);
  }





  // ---------- Conglomerate diagnosis and repair methods ----------

  /**
   * Checks the conglomerate state file.  The returned ErrorState object
   * contains information about any error generated.
   */
  public void fix(File path, String name, UserTerminal terminal) {

    this.path = path;
    this.name = name;
    this.read_only = false;

    try {
      File state_fn = new File(path, name + STATE_EXT);
      if (!state_fn.exists()) {
        terminal.println("Couldn't find file: " + state_fn);
        return;
      }
      terminal.println("+ Found state file: " + state_fn);

      setupInternal();

      state_file = new FixedSizeDataStore(state_fn, 507, Debug());
      try {
        // Open the state file.
        state_file.open(true);
        terminal.println("+ Opened state file: " + state_fn);
      }
      catch (IOException e) {
        // Couldn't open the state file.
        terminal.println("Couldn't open the state file: " + state_fn +
                         " Reason: " + e.getMessage());
        return;
      }

      try {
        // Read the header.
        state_file.readReservedBuffer(reserved_buffer, 0, 64);

        checkVisibleTables(terminal);
        readConglomerateState();
        readDroppedTables();

        // Some diagnostic information
        StringBuffer buf = new StringBuffer();
        MasterTableDataSource t;
        for (int i = 0; i < committed_tables.size(); ++i) {
          t = (MasterTableDataSource) committed_tables.get(i);
          terminal.println("+ COMMITTED TABLE: " + t.getFileName());
        }
        for (int i = 0; i < committed_dropped.size(); ++i) {
          t = (MasterTableDataSource) committed_dropped.get(i);
          terminal.println("+ COMMIT DROPPED TABLE: " + t.getFileName());
        }

        return;

      }
      catch (IOException e) {
        terminal.println("IOException: " + e.getMessage());
        return;
      }
    }
    finally {
      try {
        close();
      }
      catch (IOException e) {
        terminal.println("Unable to close conglomerate after fix.");
      }
    }

  }








  // ---------- low level File IO level operations on a conglomerate ----------
  // These operations are low level IO operations on the contents of the
  // conglomerate.  How the rows and tables are organised is up to the
  // transaction managemenet.  These methods deal with the low level
  // operations of creating/dropping tables and adding, deleting and querying
  // row in tables.

  /**
   * Tries to commit a transaction to the table data store.  This is called
   * by the 'closeAndCommit' method in Transaction.  This works as follows:
   * <ul>
   * <li> Determine if any transactions have been committed since this
   *  transaction was created.
   * <li> If no transactions committed then commit this transaction and exit.
   * <li> Otherwise, determine the tables that have been changed by the
   *  committed transactions since this was created.
   * <li> If no tables changed in the tables changed by this transaction then
   *  commit this transaction and exit.
   * <li> Determine if there are any rows that have been deleted that this
   *  transaction read/deleted.
   * <li> If there are then rollback this transaction and throw an error.
   * <li> Determine if any rows have been added to the tables this transaction
   *  read/changed.
   * <li> If there are then rollback this transaction and throw an error.
   * <li> Otherwise commit the transaction.
   * </ul>
   *
   * @param transaction the transaction to commit from.
   * @param visible_tables the list of visible tables at the end of the commit
   *   (MasterTableDataSource)
   * @param selected_from_tables ths list of tables that this transaction
   *   performed 'select' like queries on (MasterTableDataSource)
   * @param touched_tables the list of tables touched by the transaction
   *   (MutableTableDataSource)
   * @param journal the journal that describes all the changes within this
   *   transaction.
   */
  void processCommit(Transaction transaction, ArrayList visible_tables,
                     ArrayList selected_from_tables,
                     ArrayList touched_tables, TransactionJournal journal)
                                                 throws TransactionException {

    // Get individual journals for updates made to tables in this
    // transaction.
    // The list MasterTableJournal
    ArrayList journal_list = new ArrayList();
    for (int i = 0; i < touched_tables.size(); ++i) {
      MasterTableJournal table_journal =
                 ((MutableTableDataSource) touched_tables.get(i)).getJournal();
      if (table_journal.entries() > 0) {  // Check the journal has entries.
        journal_list.add(table_journal);
      }
    }
    MasterTableJournal[] changed_tables =
                (MasterTableJournal[]) journal_list.toArray(
                                  new MasterTableJournal[journal_list.size()]);

    // The list of tables created by this journal.
    IntegerVector created_tables = journal.getTablesCreated();
    // Ths list of tables dropped by this journal.
    IntegerVector dropped_tables = journal.getTablesDropped();
    // The list of tables that constraints were alter by this journal
    IntegerVector constraint_altered_tables =
                         journal.getTablesConstraintAltered();

    // Exit early if nothing changed (this is a select transaction)
    if (changed_tables.length == 0 &&
        created_tables.size() == 0 && dropped_tables.size() == 0 &&
        constraint_altered_tables.size() == 0) {
      closeTransaction(transaction);
      return;
    }

    // This flag is set to true when entries from the changes tables are
    // at a point of no return.  If this is false it is safe to rollback
    // changes if necessary.
    boolean entries_committed = false;

    // Grab the commit lock.
    synchronized (commit_lock) {
      // Our factory used for constraint checks.
      RootTableDataSourceFactory table_factory = null;
      try {

        // ---- Commit check stage ----

        long tran_commit_id = transaction.getCommitID();

        // We only perform this check if transaction error on dirty selects
        // are enabled.
        if (transaction.transactionErrorOnDirtySelect()) {

//          System.out.println(selected_from_tables.size());
//          System.out.println(selected_from_tables);

          // For each table that this transaction selected from, if there are
          // any committed changes then generate a transaction error.
          for (int i = 0; i < selected_from_tables.size(); ++i) {
            MasterTableDataSource selected_table =
                          (MasterTableDataSource) selected_from_tables.get(i);
            // Find all committed journals equal to or greater than this
            // transaction's commit_id.
            MasterTableJournal[] journals_since =
                          selected_table.findAllJournalsSince(tran_commit_id);
            if (journals_since.length > 0) {
              // Yes, there are changes so generate transaction error and
              // rollback.
              throw new TransactionException(
                TransactionException.DIRTY_TABLE_SELECT,
                    "Concurrent Serializable Transaction Conflict(4): " +
                    "Select from table that has committed changes: " +
                    selected_table.getName());
            }
          }
        }

        // For each journal,
        for (int i = 0; i < changed_tables.length; ++i) {
          MasterTableJournal change_journal = changed_tables[i];
          // The table the change was made to.
          int table_id = change_journal.getTableID();
          // Get the master table with this table id.
          MasterTableDataSource master = getMasterTable(table_id);

          // Check this table is still in the committed tables list.
          if (!created_tables.contains(table_id) &&
              !committed_tables.contains(master)) {
            // This table is no longer a committed table, so rollback
            throw new TransactionException(
                  TransactionException.TABLE_DROPPED,
                  "Concurrent Serializable Transaction Conflict(2): " +
                  "Table altered/dropped: " + master.getName());
          }

          // Since this journal was created, check to see if any changes to the
          // tables have been committed since.
          // This will return all journals on the table with the same commit_id
          // or greater.
          MasterTableJournal[] journals_since =
                                 master.findAllJournalsSince(tran_commit_id);

          // For each journal, determine if there's any clashes.
          for (int n = 0; n < journals_since.length; ++n) {
            // This will thrown an exception if a commit classes.
            change_journal.testCommitClash(journals_since[n]);
          }

        }

        // Check that no duplicate tables were created.
        if (created_tables.size() > 0) {
          performDuplicateTableCheck(new IntegerVector(created_tables),
                                     new IntegerVector(dropped_tables));
        }

        // Look at the transaction journal, if a table is dropped that has
        // journal entries since the last commit then we have an exception
        // case.
        for (int i = 0; i < dropped_tables.size(); ++i) {
          int table_id = dropped_tables.intAt(i);
          // Get the master table with this table id.
          MasterTableDataSource master = getMasterTable(table_id);
          // Any journal entries made to this dropped table?
          if (master.findAllJournalsSince(tran_commit_id).length > 0) {
            // Oops, yes, rollback!
            throw new TransactionException(
                  TransactionException.TABLE_REMOVE_CLASH,
                  "Concurrent Serializable Transaction Conflict(3): " +
                  "Dropped table has modifications: " + master.getName());
          }
        }

        // Tests passed so go on to commit,

        // ---- Commit stage ----

//        // The 'commit_id' that we can merge journal changes in the master
//        // tables up to.
//        minimum_commit_id = open_transactions.minimumCommitID(transaction);

        // NOTE: This isn't as fail safe as it could be.  We really need to
        //  do the commit in two phases.  The first writes updated indices to
        //  the index files.  The second updates the header pointer for the
        //  respective table.  Perhaps we can make the header update
        //  procedure just one file write.

        final int changed_tables_count = changed_tables.length;
        // An array of MasterTableDataSource objects for each changed table
        MasterTableDataSource[] changed_master_tables =
                           new MasterTableDataSource[changed_tables_count];
        // An array of IndexSet objects for the changed table
        IndexSet[] changed_table_index_set =
                                        new IndexSet[changed_tables_count];
        // An array of TableDataSource objects for each changed table.  These
        // objects represent the current raw committed data in the respective
        // table.
        RootTableDataSource[] changed_table_source =
                             new RootTableDataSource[changed_tables_count];
        // Our factory used for constraint checks.
        table_factory = new RootTableDataSourceFactory();

        // Initialize the above arrays
        for (int i = 0; i < changed_tables_count; ++i) {
          // The table the changes were made to.
          int table_id = changed_tables[i].getTableID();
          // Get the master table with this table id.
          MasterTableDataSource master = getMasterTable(table_id);
          // Create an IndexSet object for the table changed.
          IndexSet index_set = master.createIndexSet();
          // Set the data in the arrays
          changed_master_tables[i] = master;
          changed_table_index_set[i] = index_set;
          // Create a TableDataSource representing the data in the master
          // table including selectable scheme info.
          RootTableDataSource table_source =
                                new RootTableDataSource(master, index_set);
          // Add to the caches/stores
          table_factory.add(master.getTableName(), table_source);
          changed_table_source[i] = table_source;
        }

        // Any tables that the constraints were altered for we need to check
        // if any rows in the table violate the new constraints.
        for (int i = 0; i < constraint_altered_tables.size(); ++i) {
          // We need to check there are no constraint violations for all the
          // rows in the table.
          int table_id = constraint_altered_tables.intAt(i);
          for (int n = 0; n < changed_tables_count; ++n) {
            if (changed_master_tables[n].getTableID() == table_id) {
              checkAllAddConstraintViolations(transaction,
                            table_factory, changed_table_source[n],
                            Transaction.INITIALLY_DEFERRED);
            }
          }
        }

        // We must commit the changes made to the tables in this
        // transaction to our indices.

        // For each change to each table,
        for (int i = 0; i < changed_tables.length; ++i) {
          // Get the journal that details the change to the table.
          MasterTableJournal change_journal = changed_tables[i];
          // Commit the changes to the indexes of the table.
          // This has the effect of modifying the IndexSet array we have
          // set up with updated committed changes to the table.
          // The root table.
          RootTableDataSource root = changed_table_source[i];
          // Update the indexes in the root table.
          root.updateIndexes(change_journal);
        }

        // For each changed table we must determine the rows that
        // were deleted and perform the remove constraint checks on the
        // deleted rows.  Note that this happens after the records are
        // removed from the index.

        // For each changed table,
        for (int i = 0; i < changed_tables_count; ++i) {
          // Get the journal that details the change to the table.
          MasterTableJournal change_journal = changed_tables[i];
          // Find the normalized deleted rows.
          int[] normalized_removed_rows =
                                      change_journal.normalizedRemovedRows();
          // Check removing any of the data doesn't cause a constraint
          // violation.
          checkRemoveConstraintViolations(transaction,
                     table_factory, changed_table_source[i],
                     normalized_removed_rows,
                     Transaction.INITIALLY_DEFERRED);
        }

        // Now perform deferred constraint checks on added records.

        // For each changed table,
        for (int i = 0; i < changed_tables_count; ++i) {
          // Get the journal that details the change to the table.
          MasterTableJournal change_journal = changed_tables[i];
          // Find the normalized added rows.
          int[] normalized_added_rows =
                                      change_journal.normalizedAddedRows();
          // Check adding any of the data doesn't cause a constraint
          // violation.
          checkAddConstraintViolations(transaction,
                                       table_factory, changed_table_source[i],
                                       normalized_added_rows,
                                       Transaction.INITIALLY_DEFERRED);
        }

        // Finally, at this point all constraint checks have passed and the
        // changes are ready to finally be committed as perminant changes
        // to the conglomerate.  All that needs to be done is to commit our
        // IndexSet indices for each changed table as final.
        // ISSUE: Should we separate the 'committing of indexes' changes and
        //   'committing of delete/add flags' to make the FS more robust?
        //   It would be more robust if all indexes are committed in one go,
        //   then all table flag data.

        // Set flag to indicate we have committed entries.
        entries_committed = true;

        // For each change to each table,
        for (int i = 0; i < changed_tables.length; ++i) {
          // Get the journal that details the change to the table.
          MasterTableJournal change_journal = changed_tables[i];
          // Get the master table with this table id.
          MasterTableDataSource master = changed_master_tables[i];
          // Commit the changes to the table.
          // We use 'this.commit_id' which is the current commit level we are
          // at.
          master.commitTransactionChange(this.commit_id, change_journal,
                                         changed_table_index_set[i]);
          // Dispose the table source
          changed_table_source[i].dispose();
        }

// [ The older semantics of the table commit methods ]
//        // For each change to each table,
//        for (int i = 0; i < changed_tables.length; ++i) {
//          // Get the journal that details the change to the table.
//          MasterTableJournal change_journal = changed_tables[i];
//          // The table the changes were made to.
//          int table_id = change_journal.getTableID();
//          // Get the master table with this table id.
//          MasterTableDataSource master = getMasterTable(table_id);
//          // Add this to the list of tables this commit changed.
//          changed_master_tables.add(master);
//          // Commit the changes to the table.
//          // We use 'this.commit_id' which is the current commit level we are
//          // at.
//          master.commitTransactionChange(this.commit_id, change_journal);
//        }

        // Only do this if we've created or dropped tables.
        if (created_tables.size() > 0 || dropped_tables.size() > 0) {
          // Update the committed tables in the conglomerate.
          // This will update and synchronize the headers in this conglomerate.
          commitToTables(created_tables, dropped_tables);
//          commitToTables(visible_tables);
        }

        // Mark each dropped table.
        for (int i = 0; i < dropped_tables.size(); ++i) {
          int table_id = dropped_tables.intAt(i);
          markAsCommittedDropped(table_id);
        }

      }
      finally {

        if (table_factory != null) {
          try {
            // Dispose the table factory.
            table_factory.dispose();
          }
          catch (Throwable e) {
            Debug().writeException(e);
          }
        }

        try {

          // If entries_committed == false it means we didn't get to a point
          // where any changed tables were committed.  Attempt to rollback the
          // changes in this transaction if they haven't been committed yet.
          if (entries_committed == false) {
            // For each change to each table,
            for (int i = 0; i < changed_tables.length; ++i) {
              // Get the journal that details the change to the table.
              MasterTableJournal change_journal = changed_tables[i];
              // The table the changes were made to.
              int table_id = change_journal.getTableID();
              // Get the master table with this table id.
              MasterTableDataSource master = getMasterTable(table_id);
              // Commit the rollback on the table.
              master.rollbackTransactionChange(change_journal);
            }
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
              Debug().write(Lvl.INFORMATION, this,
                          "Rolled back transaction changes in a commit.");
            }
          }

        }
        finally {
          // Always ensure a transaction close, even if we have an exception.
          // Notify the conglomerate that this transaction has closed.
          closeTransaction(transaction);
        }

      }

    }  // synchronized (commit_lock)


// [ NOTE: Not needed because journal entries are merged when they are
//         requested now. ]
//
//    // NOTE: This is non-essential and is run on another thread outside an
//    //   exclusive lock.
//    final int min_commit_id = minimum_commit_id;
//    Object evt = DatabaseSystem.createEvent(new Runnable() {
//      public void run() {
//        // NOTE: This happens outside the commit_lock.
//        // Merge journal changes on the tables altered.
//        for (int i = 0; i < changed_master_tables.size(); ++i) {
//          // Get the master table that changed.
//          MasterTableDataSource master =
//                       (MasterTableDataSource) changed_master_tables.get(i);
//          // Merge the journal entries for this table up to the minimum open
//          // transaction commit_id.
//          master.mergeJournalChanges(min_commit_id);
//        }
//      }
//    });
//    DatabaseSystem.postEvent(50, evt);

  }

  /**
   * Rollbacks a transaction and invalidates any changes that the transaction
   * made to the database.  The rows that this transaction changed are given
   * up as freely available rows.  This is called by the 'closeAndRollback'
   * method in Transaction.
   */
  void processRollback(Transaction transaction,
                       ArrayList touched_tables, TransactionJournal journal) {

    // Go through the journal.  Any rows added should be marked as deleted
    // in the respective master table.

    // Get individual journals for updates made to tables in this
    // transaction.
    // The list MasterTableJournal
    ArrayList journal_list = new ArrayList();
    for (int i = 0; i < touched_tables.size(); ++i) {
      MasterTableJournal table_journal =
                 ((MutableTableDataSource) touched_tables.get(i)).getJournal();
      if (table_journal.entries() > 0) {  // Check the journal has entries.
        journal_list.add(table_journal);
      }
    }
    MasterTableJournal[] changed_tables =
                (MasterTableJournal[]) journal_list.toArray(
                                  new MasterTableJournal[journal_list.size()]);

    // The list of tables created by this journal.
    IntegerVector created_tables = journal.getTablesCreated();

    synchronized (commit_lock) {

      try {

        // For each change to each table,
        for (int i = 0; i < changed_tables.length; ++i) {
          // Get the journal that details the change to the table.
          MasterTableJournal change_journal = changed_tables[i];
          // The table the changes were made to.
          int table_id = change_journal.getTableID();
          // Get the master table with this table id.
          MasterTableDataSource master = getMasterTable(table_id);
          // Commit the rollback on the table.
          master.rollbackTransactionChange(change_journal);
        }

      }
      finally {
        // Notify the conglomerate that this transaction has closed.
        closeTransaction(transaction);
      }
    }
  }

  // -----

  /**
   * Checks that no tables were created that are duplicates of tables already
   * committed to the conglomerate.
   * <p>
   * This should be called as part of a transaction commit.
   */
  private void performDuplicateTableCheck(IntegerVector created_tables,
                  IntegerVector dropped_tables) throws TransactionException {

    // Remove created_tables that are in dropped_tables
    for (int i = created_tables.size() - 1; i >= 0; --i) {
      int table_id = created_tables.intAt(i);
      int drop_index = dropped_tables.indexOf(table_id);
      if (drop_index == -1) {
        MasterTableDataSource create_master = getMasterTable(table_id);
        TableName create_table_name = create_master.getTableName();
        // If there is a dropped table with the same TableName then drop it
        for (int n = dropped_tables.size() - 1; n >= 0; --n) {
          int dropped_table_id = dropped_tables.intAt(n);
          MasterTableDataSource drop_master = getMasterTable(dropped_table_id);
          if (drop_master.getTableName().equals(create_table_name)) {
            // Same table name so remove this
            drop_index = n;
          }
        }
      }
      if (drop_index != -1) {
        created_tables.removeIntAt(i);
        dropped_tables.removeIntAt(drop_index);
      }
    }

    // By this point, created_tables should only contain the tables really
    // being created.

    // For each created table, check that a table with the same name
    // doesn't already exist.
    for (int i = 0; i < created_tables.size(); ++i) {
      int table_id = created_tables.intAt(i);
      // The master table
      MasterTableDataSource master = getMasterTable(table_id);
      // Get the table name of the table created
      TableName created_table = master.getTableName();

      // Check this name isn't in the committed tables list
      for (int n = 0; n < committed_tables.size(); ++n) {
        MasterTableDataSource committed_master =
                          (MasterTableDataSource) committed_tables.get(n);
        if (committed_master.getTableName().equals(created_table)) {
          // It is so throw the transaction error
          throw new TransactionException(
               TransactionException.DUPLICATE_TABLE,
               "Concurrent Serializable Transaction Conflict(5): " +
               "Duplicate tables created: " + created_table);
        }
      }
    }

  }

  /**
   * Sets the given List of MasterTableDataSource objects to the currently
   * committed list of tables in this conglomerate.  This will make the change
   * permanent by updating the state file also.
   * <p>
   * This should be called as part of a transaction commit.
   */
  private void commitToTables(
                  IntegerVector created_tables, IntegerVector dropped_tables) {

    // Add created tables to the committed tables list.
    for (int i = 0; i < created_tables.size(); ++i) {
      MasterTableDataSource t = getMasterTable(created_tables.intAt(i));
      committed_tables.add(t);
    }

    // Remove dropped table ids from the committed_tables list
    int dropped_count = 0;
    for (int i = committed_tables.size() - 1; i >= 0 ; --i) {
      MasterTableDataSource table =
                              (MasterTableDataSource) committed_tables.get(i);
      if (dropped_tables.contains(table.getTableID())) {
        committed_tables.remove(i);
        ++dropped_count;
      }
    }

    if (dropped_count != dropped_tables.size()) {
      throw new Error(
                  "Failed assertion: dropped entries != dropped tables size");
    }

    // Remove any tables found here from the list of uncommitted.
    for (int i = 0; i < committed_tables.size(); ++i) {
      MasterTableDataSource t =
                              (MasterTableDataSource) committed_tables.get(i);
      committed_dropped.remove(t.getFileName());
    }

//    // PENDING: Is the given list of tables different from the current list?
//    committed_tables.clear();
//    committed_tables.addAll(tables);
//    // Remove any tables found here from the list of uncommitted.
//    for (int i = 0; i < tables.size(); ++i) {
//      MasterTableDataSource t = (MasterTableDataSource) tables.get(i);
//      committed_dropped.remove(t.getFileName());
//    }
    try {
      writeDroppedTables();
      writeVisibleTables();
      commitCurrentState();
    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new Error("IO Error: " + e.getMessage());
    }
  }

  /**
   * Returns the committed MasterTableDataSource in this conglomerate with
   * the given name.
   */
  MasterTableDataSource getCommittedMasterTable(String schema_name,
                                                String table_name) {
    synchronized (commit_lock) {
      // Find the table with this name.
      for (int i = 0; i < committed_tables.size(); ++i) {
        MasterTableDataSource t =
                              (MasterTableDataSource) committed_tables.get(i);
        if (t.getSchema().equals(schema_name) &&
            t.getName().equals(table_name)) {
          return t;
        }
      }
      throw new Error("Unable to find an open table with name: " + table_name);
    }
  }

  /**
   * Returns the MasterTableDataSource in this conglomerate with the given
   * table id.
   */
  MasterTableDataSource getMasterTable(int table_id) {
    synchronized (commit_lock) {
      // Find the table with this table id.
      for (int i = 0; i < table_list.size(); ++i) {
        MasterTableDataSource t = (MasterTableDataSource) table_list.get(i);
        if (t.getTableID() == table_id) {
          return t;
        }
      }
      throw new Error("Unable to find an open table with id: " + table_id);
    }
  }

  /**
   * Creates a table store in this conglomerate with the given name and returns
   * a reference to the table.  Note that this table is not a commited change
   * to the system.  It is a free standing blank table store.  The table
   * returned here is uncommitted and will be deleted unless it is committed.
   * <p>
   * Note that two tables may exist within a conglomerate with the same name,
   * however each <b>committed</b> table must have a unique name.
   * <p>
   * @param table_def the table definition.
   * @param data_sector_size the size of the data sectors (affects performance
   *   and size of the file).
   * @param index_sector_size the size of the index sectors.
   */
  MasterTableDataSource createMasterTable(DataTableDef table_def,
                                int data_sector_size, int index_sector_size) {
    synchronized (commit_lock) {
      try {

        // EFFICIENCY: Currently this writes to the conglomerate state file
        //   twice.  Once in 'nextUniqueTableID' and once in
        //   'commitCurrentState'.

        // The unique id that identifies this table,
        int table_id = nextUniqueTableID();

        // Create the object.
        MasterTableDataSource master_table =
                    new MasterTableDataSource(getSystem(), open_transactions);
        master_table.create(getPath(), table_id, table_def,
                            data_sector_size, index_sector_size);

        // Add to the list of all tables.
        table_list.add(master_table);

        // Add this to the list of deleted tables,
        // (This should really be renamed to uncommitted tables).
        markAsCommittedDropped(table_id);

        // Update the dropped tables list in the conglomerate state file.
        writeDroppedTables();
        commitCurrentState();

        // And return it.
        return master_table;

      }
      catch (IOException e) {
        Debug().writeException(e);
        throw new Error("Unable to create master table '" +
                        table_def.getName() + "' - " + e.getMessage());
      }
    }

  }

  // ---------- Inner classes ----------

  /**
   * A simple TableDataSource implementation that wraps around a
   * MasterTableDataSource object and an IndexSet (for index information).
   * This is a very minimal implementation which is used to test constraint
   * conditions and update index information inside a commit.
   * <p>
   * This is not designed to be thread safe and should only be used within
   * a commit cycle.
   * <p>
   * NOTE: This implementation is backed by the given master_table and
   *   index_set which means any changes to these objects are reflected
   *   directly in this object.
   */
  private static final class RootTableDataSource implements TableDataSource {

    /**
     * The MasterTableDataSource parent.
     */
    private MasterTableDataSource master_table;

    /**
     * The number of columns in the table.
     */
    private int column_count;

    /**
     * The IndexSet that contains index information for this table.
     */
    private IndexSet index_set;

    /**
     * The master index.
     */
    private IntegerListInterface master_index;

    /**
     * Cached schemes.
     */
    private SelectableScheme[] schemes;

    /**
     * Constructs the RootTableDataSource.
     */
    public RootTableDataSource(MasterTableDataSource master_table,
                               IndexSet index_set) {
      this.master_table = master_table;
      this.index_set = index_set;
      // master index is first list in the IndexSet
      this.master_index = index_set.getIndex(0);
      // Set up all the selectable schemes for the table
      column_count = master_table.getDataTableDef().columnCount();
      schemes = new SelectableScheme[column_count];
      for (int i = 0; i < column_count; ++i) {
        schemes[i] = master_table.createSelectableSchemeForColumn(
                                                       index_set, this, i);
      }
      master_table.addRootLock();
    }

    /**
     * Returns a DebugLogger object we can use to log debug messages to.
     */
    public final DebugLogger Debug() {
      return master_table.Debug();
    }

    /**
     * Updates this RootTableDataSource with the information found in
     * the journal.  When this method returns the root table will represent
     * the state of the table once committed, however it will not make any
     * finalized changes to the database.
     */
    void updateIndexes(MasterTableJournal change) {
      // Mark all rows in the data_store as appropriate to the changes.
      // The number of entries in the journal
      int size = change.entries();

//      long t = System.currentTimeMillis();

      SelectableScheme first_scheme = getColumnScheme(0);
      // Update the index indices with the changes made to the table
      for (int i = 0; i < size; ++i) {
        byte b = change.getCommand(i);
        int row_index = change.getRowIndex(i);
        // Was a row added or removed?
        if (b == MasterTableJournal.TABLE_ADD) {
          // Insert the row in the master index
          boolean inserted = master_index.uniqueInsertSort(row_index);
          if (!inserted) {
            Debug().write(Lvl.ERROR, this, "Table: " + master_table);
            Debug().write(Lvl.ERROR, this, "Row index: " + row_index);
            throw new Error(
                     "Assertion failed: Master index entry was duplicated.");
          }
          first_scheme.insert(row_index);
        }
        else if (b == MasterTableJournal.TABLE_REMOVE) {
          // Remove the row in the master index
          boolean removed = master_index.removeSort(row_index);
          if (!removed) {
            Debug().write(Lvl.ERROR, this, "Table: " + master_table);
            Debug().write(Lvl.ERROR, this, "Row index: " + row_index);
            throw new Error(
                     "Assertion failed: Master index entry was not present.");
          }
          first_scheme.remove(row_index);
        }
        else {
          throw new Error("Unknown row change code: " + b);
        }
      }

//      System.out.println("0: " + (System.currentTimeMillis() - t));

      // For each column
      for (int n = 1; n < column_count; ++n) {
        SelectableScheme cur_scheme = getColumnScheme(n);
        // Update the index indices with the changes made to the table
        for (int i = 0; i < size; ++i) {
          byte b = change.getCommand(i);
          int row_index = change.getRowIndex(i);
          // Was a row added or removed?
          if (b == MasterTableJournal.TABLE_ADD) {
            cur_scheme.insert(row_index);
          }
          else if (b == MasterTableJournal.TABLE_REMOVE) {
            cur_scheme.remove(row_index);
          }
          else {
            throw new Error("Unknown row change code: " + b);
          }
        }
//        System.out.println("" + n + ": " + (System.currentTimeMillis() - t));
      }

    }

    /**
     * Disposes of the table.
     */
    void dispose() {
      if (master_table != null) {
        master_table.removeRootLock();
        master_table = null;
      }
    }

    public void finalize() {
      dispose();
    }

    // ----- Implemented from TableDataSource -----

    public TransactionSystem getSystem() {
      return master_table.getSystem();
    }

    public DataTableDef getDataTableDef() {
      return master_table.getDataTableDef();
    }

    public int getRowCount() {
      return master_index.size();
    }

    public RowEnumeration rowEnumeration() {
      final IntegerIterator iterator = master_index.iterator();
      // Wrap it around a RowEnumeration object.
      return new RowEnumeration() {
        public boolean hasMoreRows() {
          return iterator.hasNext();
        }
        public int nextRowIndex() {
          return iterator.next();
        }
      };
    }

    public SelectableScheme getColumnScheme(int column) {
      return schemes[column];
    }

    public DataCell getCellContents(int column, int row) {
      return master_table.getCellContents(column, row);
    }
    public int compareCellTo(DataCell ob, int column, int row) {
      return master_table.compareCellTo(ob, column, row);
    }

  }

  /**
   * A class that handles the retrieving of TableDataSource objects for use
   * in constraint resolution.  This is passed to the constraint methods so
   * that they may resolve a table name to a TableDataSource.  This is used
   * when performing foreign key reference checks.
   */
  static abstract class TableDataSourceFactory {

    /**
     * The mapping of TableName -> TableDataSource cached by the object.
     */
    protected HashMap table_map;

    /**
     * Constructs the factory.
     */
    public TableDataSourceFactory() {
      table_map = new HashMap();
    }

    /**
     * Creates a TableDataSource for the given table name.
     */
    public abstract TableDataSource createTableDataSource(TableName table);

    /**
     * Finds and returns the TableDataSource with the given name.  If the name
     * isn't immediately found in the map it is created and added.
     */
    public TableDataSource get(TableName table) {
      TableDataSource table_source = (TableDataSource) table_map.get(table);
      if (table_source == null) {
        table_source = createTableDataSource(table);
        add(table, table_source);
      }
      return table_source;
    }

    /**
     * Adds a TableDataSource to this factory.
     */
    public void add(TableName table, TableDataSource table_source) {
      table_map.put(table, table_source);
    }

  }

  /**
   * An implementation of TableDataSourceFactory that creates
   * RootTableDataSource objects that represent the current committed state
   * of the tables.  This object may only be used under a commit lock.
   */
  private class RootTableDataSourceFactory extends TableDataSourceFactory {

    /**
     * Maps from TableDataSource -> IndexSet for the table created from this
     * factory.  We need to keep hold of the IndexSet because we need to
     * dispose it when the factory is disposed.
     */
    private HashMap index_lookup = new HashMap();

    // ---------- Implemented ----------

    public TableDataSource createTableDataSource(TableName table) {
      // Get the current committed version of the master table with the given
      // name
      MasterTableDataSource master = getCommittedMasterTable(
                                       table.getSchema(), table.getName());
      // Create an IndexSet object for the table changed.
      IndexSet index_set = master.createIndexSet();
      // Create a TableDataSource representing the data in the master
      // table including selectable scheme info.
      TableDataSource source = new RootTableDataSource(master, index_set);

      index_lookup.put(source, index_set);
      return source;
    }

    /**
     * Dispose of all RootTableDataSource resources associated with this
     * factory.
     */
    public void dispose() {
//      super.dispose();
      Iterator i = table_map.keySet().iterator();
      while (i.hasNext()) {
        Object ob = table_map.get(i.next());
        if (ob instanceof RootTableDataSource) {
          ((RootTableDataSource) ob).dispose();
          // Dispose the index set.
          IndexSet index_set = (IndexSet) index_lookup.get(ob);
          if (index_set != null) {
            index_set.dispose();
          }
        }
      }
    }

  }


}
