/**
 * com.mckoi.database.MasterTableDataSource  19 Nov 2000
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

import java.util.ArrayList;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.io.*;
import com.mckoi.util.IntegerListInterface;
import com.mckoi.util.IntegerIterator;
import com.mckoi.util.IntegerVector;
import com.mckoi.util.ByteArrayUtil;
import com.mckoi.util.UserTerminal;
import com.mckoi.util.Cache;
import com.mckoi.debug.*;

/**
 * A MasterTableDataSource is a table source inside a TableDataConglomerate
 * that is the master view of a table.  This encapsulates all transactional
 * data over the lifetime of a table.
 * <p>
 * The MasterTableDataSource sees the table as a construct of columns and
 * rows with data.  Any row may belong to a transaction number that may or
 * may not be committed.
 * <p>
 * This object is also used to physically add rows persistantly to a data
 * store and manage low level data table management issues such as retrieving
 * row contents.  It also merges transactional modifications with the master
 * index and updates the master index schemes when it can.
 * <p>
 * NOTE: This class is not thread safe.  This is because transactional
 *   updates will require an exclusive lock on the tables involved so
 *   synchronization can not be handled by this object.
 *
 * @author Tobias Downer
 */

final class MasterTableDataSource {

  // ---------- System information ----------

  /**
   * The global TransactionSystem object that points to the global system
   * that this table source belongs to.
   */
  private TransactionSystem system;

  // ---------- State information ----------

  /**
   * An integer that uniquely identifies this data source within the
   * conglomerate.
   */
  private int table_id;

  /**
   * The file name of this store in the conglomerate path.
   */
  private String file_name;

  /**
   * True if this table source is closed.
   */
  private boolean is_closed;

  /**
   * True if this table is in read only mode.
   */
  private boolean is_read_only;

  /**
   * True if the table should be dropped.
   */
  private boolean pending_dropped;

  // ---------- Root locking ----------

  /**
   * The number of root locks this table data source has on it.
   * <p>
   * While a MasterTableDataSource has at least 1 root lock, it may not
   * reclaim deleted space in the data store.  A root lock means that data
   * is still being pointed to in this file (even possibly committed deleted
   * data).
   */
  private int root_lock;

  // ---------- Persistant data ----------

  /**
   * A DataTableDef object that describes the table topology.  This includes
   * the name and columns of the table.
   */
  private DataTableDef table_def;

  /**
   * A multi-version representation of the table indices kept for this table
   * including the row list and the scheme indices.  This contains the
   * transaction journals.
   */
  private MultiVersionTableIndices table_indices;

  /**
   * A VariableSizeDataStore object that physically contains the information
   * stored in the file system in the contents of the data source.
   */
  private VariableSizeDataStore data_store;

  /**
   * An IndexStore object that manages the indexes for this table.
   */
  private IndexStore index_store;

  /**
   * The list of RIDList objects for each column in this table.  This is
   * a sorting optimization.
   */
  private RIDList[] column_rid_list;


  // ---------- Cached information ----------

  /**
   * Set to false to disable cell caching.
   */
  private boolean DATA_CELL_CACHING = true;

  /**
   * A reference to the DataCellCache object.
   */
  private final DataCellCache cache;

  /**
   * The number of columns in this table.  This is a cached optimization.
   */
  private int column_count;

  /**
   * The object we use to serialize DataCell objects.
   */
  private final DataCellSerialization data_cell_serializer =
                                            new DataCellSerialization();

  /**
   * The persistant object we use to read information from a row stream.
   */
  private CellInputStream cell_in;

  // --------- Parent information ----------

  /**
   * The list of all open transactions managed by the parent conglomerate.
   * This is a thread safe object, and is updated whenever new transactions
   * are created, or transactions are closed.
   */
  private OpenTransactionList open_transactions;

  // ---------- Row garbage collection ----------

  /**
   * Manages scanning and deleting of rows marked as deleted within this
   * data source.
   */
  private MasterTableGarbageCollector garbage_collector;

  // ----------- Stat keys -----------

  /**
   * The keys we use for Database.stats() for information for this table.
   */
  private String root_lock_key;
  private String total_hits_key;
  private String file_hits_key;
  private String delete_hits_key;
  private String insert_hits_key;


  /**
   * Constructs the MasterTableDataSource.  The argument is a reference to an
   * object that manages the list of open transactions in the conglomerate.
   * This object uses this information to determine how journal entries are
   * to be merged with the master indices.
   */
  MasterTableDataSource(TransactionSystem system,
                        OpenTransactionList open_transactions) {

    this.system = system;
    this.open_transactions = open_transactions;
    this.garbage_collector = new MasterTableGarbageCollector(this);
    this.cache = system.getDataCellCache();
    is_closed = true;

    DATA_CELL_CACHING = (cache != null);

    cell_in = new CellInputStream(null);

    commit_journals_event =
             system.createEvent(new TransactionJournalCleanUpEvent());

  }

  /**
   * Returns the TransactionSystem for this table.
   */
  public final TransactionSystem getSystem() {
    return system;
  }

  /**
   * Returns the DebugLogger object that can be used to log debug messages.
   */
  public final DebugLogger Debug() {
    return getSystem().Debug();
  }

  /**
   * Returns the TableName of this table source.
   */
  public TableName getTableName() {
    return getDataTableDef().getTableName();
  }

  /**
   * Returns the name of this table source.
   */
  public String getName() {
    return getDataTableDef().getName();
  }

  /**
   * Returns the schema name of this table source.
   */
  public String getSchema() {
    return getDataTableDef().getSchema();
  }

  /**
   * Sets the 'pending_dropped' flag to true.  This will cause the table
   * to drop when it has determined it is safe to do so.
   */
  synchronized void notifyDropped() {
    pending_dropped = true;
  }

  /**
   * Updates the master records from the journal logs up to the given
   * 'commit_id'.  This could be a fairly expensive operation if there are
   * a lot of modifications because each change could require a lookup
   * of records in the data source.
   * <p>
   * NOTE: It's extremely important that when this is called, there are no
   *  transaction open that are using the merged journal.  If there is, then
   *  a transaction may be able to see changes in a table that were made
   *  after the transaction started.
   * <p>
   * After this method is called, it's best to update the index file
   * with a call to 'synchronizeIndexFiles'
   */
  synchronized void mergeJournalChanges(long commit_id) {

    boolean all_merged = table_indices.mergeJournalChanges(commit_id);
    // If all journal entries merged then schedule deleted row collection.
    if (all_merged && !isReadOnly()) {
      garbage_collector.scheduleCollection();
    }

  }

  /**
   * Returns a list of all MasterTableJournal objects that have been
   * successfully committed against this table that have an 'commit_id' that
   * is greater or equal to the given.
   * <p>
   * This is part of the conglomerate commit check phase and will be on a
   * commit_lock.
   */
  synchronized MasterTableJournal[] findAllJournalsSince(long commit_id) {
    return table_indices.findAllJournalsSince(commit_id);
  }

  // ---------- Getters ----------

  /**
   * Returns table_id - the unique identifier for this data source.
   */
  int getTableID() {
    return table_id;
  }

  /**
   * Returns the name of the file in the conglomerate that represents this
   * store in the file system.
   */
  String getFileName() {
    return file_name;
  }

  /**
   * Returns the DataTableDef object that represents the topology of this
   * table data source (name, columns, etc).  Note that this information
   * can't be changed during the lifetime of a data source.
   */
  DataTableDef getDataTableDef() {
    return table_def;
  }

  // ---------- Transactional ----------

  /**
   * Synchronizes the data in the store, and then synchronizes the index file.
   * When this returns, the state is guarenteed persistently stored on disk.
   */
  private synchronized void synchAll() throws IOException {

    // Flush the indices.
    index_store.flush();

    // Synchronize the data store.
    if (!system.dontSynchFileSystem()) {
      data_store.hardSynch();
    }

    // Synchronize the file handle.  When this returns, we are guarenteed that
    // the index store and the data store are nore persistantly stored in the
    // file system.
    if (!system.dontSynchFileSystem()) {
      index_store.hardSynch();
    }

  }

  /**
   * Creates a SelectableScheme object for the given column in this table.
   * This reads the index from the index set (if there is one) then wraps
   * it around the selectable schema as appropriate.
   */
  synchronized SelectableScheme createSelectableSchemeForColumn(
                    IndexSet index_set, TableDataSource table, int column) {
    // What's the type of scheme for this column?
    DataTableColumnDef column_def = getDataTableDef().columnAt(column);

    // If the column isn't indexable then return a BlindSearch object
    if (!column_def.isIndexableType()) {
      return new BlindSearch(table, column);
    }

    String scheme_type = column_def.getIndexScheme();
    if (scheme_type.equals("InsertSearch")) {
      // Get the index from the index set and set up the new InsertSearch
      // scheme.
      IntegerListInterface index_list = index_set.getIndex(column + 1);
      InsertSearch iis = new InsertSearch(table, column, index_list);
      RIDList rid_list = column_rid_list[column];
      if (rid_list == null) {
        rid_list = new RIDList(this, column);
        column_rid_list[column] = rid_list;
      }
      iis.setRIDList(rid_list);
      return iis;
    }
    else if (scheme_type.equals("BlindSearch")) {
      return new BlindSearch(table, column);
    }
    else {
      throw new Error("Unknown scheme type");
    }
  }

  /**
   * Creates a minimal TableDataSource object that represents this
   * MasterTableDataSource.  It does not implement the 'getColumnScheme'
   * method.
   */
  private TableDataSource minimalTableDataSource(
                                  final IntegerListInterface master_index) {
    // Make a TableDataSource that represents the master table over this
    // index.
    return new TableDataSource() {
      public TransactionSystem getSystem() {
        return system;
      }
      public DataTableDef getDataTableDef() {
        return MasterTableDataSource.this.getDataTableDef();
      }
      public int getRowCount() {
        // NOTE: Returns the number of rows in the master index before journal
        //   entries have been made.
        return master_index.size();
      }
      public RowEnumeration rowEnumeration() {
        // NOTE: Returns iterator across master index before journal entry
        //   changes.
        // Get an iterator across the row list.
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
        throw new Error("Not implemented.");
      }
      public DataCell getCellContents(int column, int row) {
        return MasterTableDataSource.this.getCellContents(column, row);
      }
      public int compareCellTo(DataCell ob, int column, int row) {
        return MasterTableDataSource.this.compareCellTo(ob, column, row);
      }
    };
  }





  /**
   * Adds a new transaction modification to this master table source.  This
   * information represents the information that was added/removed in the
   * table in this transaction.  The IndexSet object represents the changed
   * index information to commit to this table.
   * <p>
   * It's guarenteed that 'commit_id' additions will be sequential.
   */
  synchronized void commitTransactionChange(long commit_id,
                              MasterTableJournal change, IndexSet index_set) {
    // ASSERT: Can't do this if source is read only.
    if (isReadOnly()) {
      throw new Error("Can't commit transaction journal, table is read only.");
    }

    change.setCommitID(commit_id);

    try {

      // Add this journal to the multi version table indices log
      table_indices.addTransactionJournal(change);

      // Write the modified index set to the index store
      // (Updates the index file)
      index_store.commitIndexSet(index_set);
      index_set.dispose();

      // Update the state of the committed added data to the file system.
      // (Updates data to the allocation file)
      //
      // ISSUE: This can add up to a lot of changes to the allocation file and
      //   the Java runtime could potentially be terminated in the middle of
      //   the update.  If an interruption happens the allocation information
      //   may be incorrectly flagged.  The type of corruption this would
      //   result in would be;
      //   + From an 'update' the updated record may disappear.
      //   + From a 'delete' the deleted record may not delete.
      //   + From an 'insert' the inserted record may not insert.
      //
      // Note, the possibility of this type of corruption occuring has been
      // minimized as best as possible given the current architecture.
      // Also note that is not possible for a table file to become corrupted
      // beyond recovery from this issue.

      int size = change.entries();
      for (int i = 0; i < size; ++i) {
        byte b = change.getCommand(i);
        int row_index = change.getRowIndex(i);
        // Was a row added or removed?
        if (b == MasterTableJournal.TABLE_ADD) {

          // Record commit added
          int old_type = data_store.writeRecordType(row_index + 1, 0x010);
          // Check the record was in an uncommitted state before we changed
          // it.
          if ((old_type & 0x0F0) != 0) {
            data_store.writeRecordType(row_index + 1, old_type & 0x0F0);
            throw new Error("Record " + row_index + " was not in an " +
                            "uncommitted state!");
          }

        }
        else if (b == MasterTableJournal.TABLE_REMOVE) {

          // Record commit removed
          int old_type = data_store.writeRecordType(row_index + 1, 0x020);
          // Check the record was in an added state before we removed it.
          if ((old_type & 0x0F0) != 0x010) {
            data_store.writeRecordType(row_index + 1, old_type & 0x0F0);
            throw new Error("Record " + row_index + " was not in an " +
                            "added state!");
          }
          // Notify collector that this row has been marked as deleted.
          garbage_collector.markRowAsDeleted(row_index);

        }
      }

      // synchronizes all the data in this table.
      // (Flushes all updates to the file system).
      synchAll();

    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new Error("IO Error: " + e.getMessage());
    }

    // Merge changes now, and schedule an event 25 ms from now.
    mergeJournalChanges(open_transactions.minimumCommitID(null));
    // Schedule event to commit transaction journals.
    system.postEvent(25, commit_journals_event);
  }

  /**
   * Rolls back a transaction change in this table source.  Any rows added
   * to the table will be uncommited rows (type_key = 0).  Those rows must be
   * marked as committed deleted.
   */
  synchronized void rollbackTransactionChange(MasterTableJournal change) {

    // ASSERT: Can't do this is source is read only.
    if (isReadOnly()) {
      throw new Error(
                   "Can't rollback transaction journal, table is read only.");
    }

    // Any rows added in the journal are marked as committed deleted and the
    // journal is then discarded.

    try {
      // Mark all rows in the data_store as appropriate to the changes.
      int size = change.entries();
      for (int i = 0; i < size; ++i) {
        byte b = change.getCommand(i);
        int row_index = change.getRowIndex(i);
        // Make row as added or removed.
        if (b == MasterTableJournal.TABLE_ADD) {
          // Record commit removed (we are rolling back remember).
          int old_type = data_store.writeRecordType(row_index + 1, 0x020);
          // Check the record was in an uncommitted state before we changed
          // it.
          if ((old_type & 0x0F0) != 0) {
            data_store.writeRecordType(row_index + 1, old_type & 0x0F0);
            throw new Error("Record " + (row_index + 1) + " was not in an " +
                            "uncommitted state!");
          }
          // Notify collector that this row has been marked as deleted.
          garbage_collector.markRowAsDeleted(row_index);
        }
        else if (b == MasterTableJournal.TABLE_REMOVE) {
          // Any journal entries marked as TABLE_REMOVE are ignored because
          // we are rolling back.  This means the row is not logically changed.
        }
      }

      // The journal entry is discarded, the indices do not need to be updated
      // to reflect this rollback.
    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new Error("IO Error: " + e.getMessage());
    }
  }

  /**
   * Creates and returns an IndexSet object that is used to create indices
   * for this table source.  The IndexSet represents a snapshot of the
   * table and the given point in time.
   * <p>
   * NOTE: Not synchronized because we synchronize in the IndexStore object.
   */
  IndexSet createIndexSet() {
    return index_store.getSnapshotIndexSet();
  }

  /**
   * Returns a MutableTableDataSource object that represents this data source
   * at the given commit_id time.  Any modifications to the returned table
   * are updated in the log of the Transaction object supplied.
   * <p>
   * This is a key method in this object because it allows us to get a data
   * source that represents the data in the table before any modifications
   * may have been committed.
   */
  MutableTableDataSource createTableDataSourceAtCommit(
                                                    Transaction transaction) {
    return new MMutableTableDataSource(transaction);
  }

  // ---------- File IO level table modification ----------

  /**
   * Returns a DataTableDef that represents the fields in this table file.
   * Conversion is done depending on what is saved in the file.
   */
  private synchronized DataTableDef loadDataTableDef() throws IOException {

    // Read record 0 which contains all this info.
    byte[] d = new byte[65536];
    int read = data_store.read(0, d, 0, 65536);
    if (read == 65536) {
      throw new IOException(
                     "Buffer overflow when reading table definition, > 64k");
    }
    ByteArrayInputStream bin = new ByteArrayInputStream(d, 0, read);

    DataTableDef def;

    DataInputStream din = new DataInputStream(bin);
    int mn = din.readInt();
    // This is the latest format...
    if (mn == 0x0bebb) {
      // Read the DataTableDef object from the input stream,
      def = DataTableDef.read(din);
    }
    else {
      // Legacy no longer supported...
      throw new IOException(
                "Couldn't find magic number for table definition data.");
    }

    return def;
  }

  /**
   * Saves the DataTableDef object to the store.
   */
  private synchronized void saveDataTableDef(DataTableDef def)
                                                       throws IOException {

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);

    dout.writeInt(0x0bebb);
    def.write(dout);

    // Write the byte array to the data store,

    byte[] d = bout.toByteArray();
    int rindex = data_store.write(d, 0, d.length);

    // rindex MUST be 0 else we buggered.
    if (rindex != 0) {
//      System.out.println(rindex);
      throw new IOException("Couldn't write table fields to record 0.");
    }

  }

  /**
   * Creates a unique table name for a file.  This can be changed to suit
   * a particular OS's style of filesystem namespace.  Or it could return some
   * arbitarily unique number.  However, for debugging purposes it's often
   * a good idea to return a name that a user can recognise.
   * <p>
   * The 'table_id' is a guarenteed unique number between all tables.
   */
  private String makeTableFileName(int table_id, TableName table_name) {
    // NOTE: We may want to change this for different file systems.
    //   For example DOS is not able to handle more than 8 characters
    //   and is case insensitive.
    String tid = Integer.toString(table_id);
    int pad = 3 - tid.length();
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < pad; ++i) {
      buf.append('0');
    }

    String str = table_name.toString().replace('.', '_');

    // Go through each character and remove each non a-z,A-Z,0-9,_ character.
    // This ensure there are no strange characters in the file name that the
    // underlying OS may not like.
    StringBuffer osified_name = new StringBuffer();
    int count = 0;
    for (int i = 0; i < str.length() || count > 64; ++i) {
      char c = str.charAt(i);
      if ((c >= 'a' && c <= 'z') ||
          (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') ||
          c == '_') {
        osified_name.append(c);
        ++count;
      }
    }

    return new String(buf) + tid + new String(osified_name);
  }

  /**
   * Loads the internal variables.
   */
  private synchronized void loadInternal() {
    // Set up the stat keys.
    String table_name = table_def.getName();
    String schema_name = table_def.getSchema();
    String n = table_name;
    if (schema_name.length() > 0) {
      n = schema_name + "." + table_name;
    }
    root_lock_key = "MasterTableDataSource.RootLocks." + n;
    total_hits_key = "MasterTableDataSource.Hits.Total." + n;
    file_hits_key = "MasterTableDataSource.Hits.File." + n;
    delete_hits_key = "MasterTableDataSource.Hits.Delete." + n;
    insert_hits_key = "MasterTableDataSource.Hits.Insert." + n;

    column_count = table_def.columnCount();

    is_closed = false;

  }

  /**
   * Physically create this master table in the file system at the given
   * path.  This will initialise the various file objects and result in a
   * new empty master table to store data in.
   * <p>
   * The 'data_sector_size' and 'index_sector_size' are for fine grain
   * performance and size optimization of the data files.  The default
   * 'index_sector_size' is 1024.
   *
   * @param data_sector_size used to configure the size of the sectors in the
   *   data files.  For tables with small records this number should be low.
   * @param index_sector_size used to configure the size of the sectors in the
   *   index file.  For small tables it is best to keep the index sector size
   *   low.  Recommend 1024 for normal use, 128 for minimalist use.
   */
  synchronized void create(File path, int table_id, DataTableDef table_def,
                           int data_sector_size, int index_sector_size)
                                                        throws IOException {

    // Check table_id isn't too large.
    if ((table_id & 0x0F0000000) != 0) {
      throw new Error("'table_id' exceeded maximum possible keys.");
    }

    // Creating a table opens table in read/write mode.
    is_read_only = false;

    // The name of the table to create,
    TableName table_name = table_def.getTableName();
    // Generate the name of the store file name.
    file_name = makeTableFileName(table_id, table_name);

    // Create the store.
    data_store = new VariableSizeDataStore(new File(path, file_name),
                                           data_sector_size, Debug());
    // Open the store in read/write mode
    data_store.open(false);

    // Create table indices
    table_indices = new MultiVersionTableIndices(getSystem(),
                                          table_name, table_def.columnCount());
    // The column rid list cache
    column_rid_list = new RIDList[table_def.columnCount()];

    // Open table indices
    index_store = new IndexStore(new File(path, file_name + ".iid"), Debug());
    // Open the table index file.
    index_store.create(index_sector_size);
    index_store.init();
    // Make room for columns+1 indices in the index store file
    index_store.addIndexLists(table_def.columnCount() + 1, (byte) 1);
    index_store.flush();

    // Save the table definition to the new store.
    saveDataTableDef(table_def);

    // Write the 'table_id' of this table to the reserved area of the data
    // store.
    byte[] reserved_buffer = new byte[64];
    ByteArrayUtil.setInt(table_id, reserved_buffer, 0);
    data_store.writeReservedBuffer(reserved_buffer, 0, 64);

    // Set up internal state of this object
    this.table_id = table_id;
    this.file_name = file_name;
    this.table_def = table_def;

    // Load internal state
    loadInternal();

  }

  /**
   * Returns true if the master table data source with the given filename
   * exists.
   */
  synchronized boolean exists(File path, String file_name) throws IOException {
    VariableSizeDataStore data_store =
                new VariableSizeDataStore(new File(path, file_name), Debug());
    return data_store.exists();
  }

  /**
   * Opens an existing master table from the file system at the path of the
   * conglomerate this belongs to.  This will set up the internal state of
   * this object with the data read in.
   */
  synchronized void open(File path, String file_name, boolean read_only)
                                                           throws IOException {

    // Set read only flag.
    is_read_only = read_only;

    // Open the store.
    data_store = new VariableSizeDataStore(new File(path, file_name), Debug());
    boolean need_check = data_store.open(read_only);
//    if (need_check) {
//      System.out.println("NEED CHECK");
//    }

    // Set up the internal state of this object
    // Get the 'table_id' of this table from the reserved area of the data
    // store.
    byte[] reserved_buffer = new byte[64];
    data_store.readReservedBuffer(reserved_buffer, 0, 64);
    table_id = ByteArrayUtil.getInt(reserved_buffer, 0);

    // Set the file name.
    this.file_name = file_name;

    // Load the table definition from the store.
    table_def = loadDataTableDef();

    // Set the column count
    column_count = table_def.columnCount();

    // Open table indices
    table_indices = new MultiVersionTableIndices(getSystem(),
                           table_def.getTableName(), table_def.columnCount());
    // The column rid list cache
    column_rid_list = new RIDList[table_def.columnCount()];

    // Open table indices
    index_store = new IndexStore(new File(path, file_name + ".iid"), Debug());
    // If the index store doesn't exist then create it.
    if (!index_store.exists()) {
      if (!read_only) {
        // Does the original .ijf file exist?
        File original_ijf = new File(path, file_name + ".ijf");
        if (original_ijf.exists()) {
          // Message
          String str = "Converting index file for: " + file_name;
          System.out.println(str);
          Debug().write(Lvl.INFORMATION, this, str);
          // NOTE: The following method leaves the index store open.
          ArrayList transaction_journals =
                  ConvertUtils.convertIndexFiles1(original_ijf, index_store,
                                                  table_def, Debug());
          if (transaction_journals.size() > 0) {
            // Notify the user that this may be a problem
            Debug().write(Lvl.ERROR, this,
                    "There are uncommitted changes that were not " +
                    "converted because the pre 0.92 database was not closed " +
                    "cleanly.");
          }
          // Force a full table scan
          need_check = true;
        }
        else {
          throw new IOException("The index file for '" + file_name +
                                "' does not exist.");
        }
      }
      else {
        throw new IOException(
                         "Can not create .iid index file in read-only mode.");
      }
    }
    else {
      // Open the table index file.
      index_store.open(read_only);
      index_store.init();
    }

    // Load internal state
    loadInternal();

    // Merge all journal changes when we open
    mergeJournalChanges(Integer.MAX_VALUE);

    if (need_check) {
      // Do an opening scan of the table.  Any records that are uncommited
      // must be marked as deleted.
      doOpeningScan();
    }

//    // Schedule a full garbage collector sweep.
//    garbage_collector.markFullSweep();
//    garbage_collector.scheduleCollection();

  }

  /**
   * Opens this source in the most minimal way.  This should only be used
   * for diagnostics of the data.  This will not load the index.
   */
  synchronized void dirtyOpen(File path, String file_name) throws IOException {
    // Set read only flag.
    is_read_only = false;

    // We have to open this...
    // Open the store.
    data_store = new VariableSizeDataStore(new File(path, file_name), Debug());
    data_store.open(false);

    // Set up the internal state of this object
    // Get the 'table_id' of this table from the reserved area of the data
    // store.
    byte[] reserved_buffer = new byte[64];
    data_store.readReservedBuffer(reserved_buffer, 0, 64);
    table_id = ByteArrayUtil.getInt(reserved_buffer, 0);

    // Set the file name.
    this.file_name = file_name;

    // Load the table definition from the store.
    table_def = loadDataTableDef();

  }

  /**
   * Closes this master table in the file system.  This frees up all the
   * resources associated with this master table.
   * <p>
   * This method is typically called when the database is shut down.
   */
  synchronized void close() throws IOException {
    if (table_indices != null) {
      // Merge all journal changes when we close
      mergeJournalChanges(Integer.MAX_VALUE);

      if (!is_read_only) {
        // Synchronize the current state with the file system.
        index_store.flush();
        //table_indices.synchronizeIndexFile();
      }
      // Close the index store.
      index_store.close();
    }

    data_store.close();

    // Are we pending to be dropped?
    if (pending_dropped) {
      Debug().write(Lvl.MESSAGE, this, "Dropping: " + getFileName());
      data_store.delete();
      index_store.delete();
    }

    table_id = -1;
    file_name = null;
    table_def = null;
    table_indices = null;
    column_rid_list = null;
    is_closed = true;
  }

  /**
   * Returns true if this table source is closed.
   */
  synchronized boolean isClosed() {
    return is_closed;
  }

  /**
   * Returns true if the source is read only.
   */
  boolean isReadOnly() {
    return is_read_only;
  }

  /**
   * Adds a new row to this table and returns an index that is used to
   * reference this row by the 'getCellContents' method.
   * <p>
   * Note that this method will not effect the master index or column schemes.
   * This is a low level mechanism for adding unreferenced data into a
   * conglomerate.  The data is referenced by commiting the change where it
   * eventually migrates into the master index and schemes.
   */
  synchronized int addRow(RowData data) throws IOException {

//    System.out.println("Writing: " + data);

    OutputStream out = data_store.getRecordOutputStream();
//    compress_out.setParentStream(out);
    DataOutputStream temp_out = new DataOutputStream(out);

    // Reserved for future use.
    temp_out.writeShort(0);

    int row_cells = data.getColumnCount();

    // Write out the data,
    for (int i = 0; i < row_cells; ++i) {
      DataCell cell = data.getCellData(i);
      data_cell_serializer.setToSerialize(cell);
      data_cell_serializer.writeSerialization(temp_out);
    }

    // Close the stream and complete it.
    temp_out.close();
    int record_index = data_store.completeRecordStreamWrite();

    // Update the cell cache as appropriate
    if (DATA_CELL_CACHING) {
      for (int i = 0; i < row_cells; ++i) {
        // Put the row/column/DataCell into the cache.
        cache.put(table_id, record_index, i, data.getCellData(i));
      }
    }

    // Record index is -1 because sector 0 is DataTableDef.
    int row_number = record_index - 1;

    // If we have a rid_list for any of the columns, then update the indexing
    // there,
    for (int i = 0; i < column_count; ++i) {
      RIDList rid_list = column_rid_list[i];
      if (rid_list != null) {
        rid_list.insertRID(data.getCellData(i), row_number);
      }
    }

    // Update stats
    system.stats().increment(insert_hits_key);

    // Return the record index of the new data in the table
    return row_number;

  }

  /**
   * Returns the number of bytes the row takes up in the data file.  This is
   * the actual space used.  If a cell is compressed then it includes the
   * compressed size, not the uncompressed.
   */
  synchronized int rawRecordSize(int row_number) throws IOException {

    int size = 2;

    ++row_number;

    // Open a stream for this row.
    InputStream in = data_store.getRecordInputStream(row_number);
    cell_in.setParentStream(in);

    cell_in.skip(2);

    for (int i = 0; i < column_count; ++i) {
      int len = data_cell_serializer.skipSerialization(cell_in);
      if (len <= 0) {
        throw new Error("Corrupt data - cell size is <= 0");
      }
      cell_in.skip(len);
      size += 4 + len;
    }

    cell_in.close();

    return size;

  }

  /**
   * Actually deletes the row from the table.  This is a perminant removal of
   * the row from the table.  After this method is called, the row can not
   * be retreived again.  This is generally only used by the row garbage
   * collector.
   * <p>
   * There is no checking in this method.
   */
  private synchronized void doHardRowRemove(int row_index) throws IOException {

    // If we have a rid_list for any of the columns, then update the indexing
    // there,
    for (int i = 0; i < column_count; ++i) {
      RIDList rid_list = column_rid_list[i];
      if (rid_list != null) {
        rid_list.removeRID(row_index);
      }
    }

    // And then delete the row perminantly from the data store.
    data_store.delete(row_index + 1);

    // Update stats
    system.stats().increment(delete_hits_key);

  }

  /**
   * Perminently removes a row from this table.  This must only be used when
   * it is determined that a transaction does not reference this row, and
   * that an open result set does not reference this row.  This will remove
   * the row perminantly from the underlying file representation.  Calls to
   * 'getCellContents(col, row)' where row is deleted will be undefined after
   * this method is called.
   * <p>
   * Note that the removed row must not be contained within the master index,
   * or be referenced by the index schemes, or be referenced in the
   * transaction modification list.
   */
  synchronized void hardRemoveRow(final int record_index) throws IOException {
    // ASSERTION: We are not under a root lock.
    if (!isRootLocked()) {
      int type_key = data_store.readRecordType(record_index + 1);
      // Check this record is marked as committed removed.
      if ((type_key & 0x0F0) == 0x020) {
//        System.out.println("[" + getName() + "] " +
//                           "Hard Removing: " + record_index);
        doHardRowRemove(record_index);
      }
      else {
        throw new Error(
                    "Row isn't marked as committed removed: " + record_index);
      }
    }
    else {
      throw new Error("Assertion failed: " +
                      "Can't remove row, table is under a root lock.");
    }
  }

  /**
   * Checks the given record index, and if it's possible to reclaim it then
   * it does so here.  Rows are only removed if they are marked as committed
   * removed.
   */
  synchronized boolean hardCheckAndReclaimRow(final int record_index)
                                                          throws IOException {
    // ASSERTION: We are not under a root lock.
    if (!isRootLocked()) {
      // Row already deleted?
      if (!data_store.recordDeleted(record_index + 1)) {
        int type_key = data_store.readRecordType(record_index + 1);
        // Check this record is marked as committed removed.
        if ((type_key & 0x0F0) == 0x020) {
//          System.out.println("[" + getName() + "] " +
//                             "Hard Removing: " + record_index);
          doHardRowRemove(record_index);
          return true;
        }
      }
      return false;
    }
    else {
      throw new Error("Assertion failed: " +
                      "Can't remove row, table is under a root lock.");
    }
  }

  /**
   * Returns true if the given record index is a deleted record.
   */
  synchronized boolean recordDeleted(int record_index) throws IOException {
    return data_store.recordDeleted(record_index + 1);
  }

  /**
   * Returns the raw count of rows in this master table source.  This includes
   * uncommited, committed added, committed removed and deleted rows.
   */
  synchronized int rawRowCount() throws IOException {
    return data_store.rawRecordCount() - 1;
  }

  /**
   * Returns the current sector size for this data source.
   */
  synchronized int rawDataSectorSize() throws IOException {
    return data_store.sectorSize();
  }

  /**
   * Returns the record type of the given record index.  Returns a type that
   * is compatible with RawDiagnosticTable record type.
   */
  synchronized int recordTypeInfo(int record_index) throws IOException {
    ++record_index;
    if (data_store.recordDeleted(record_index)) {
      return RawDiagnosticTable.DELETED;
    }
    int type_key = data_store.readRecordType(record_index) & 0x0F0;
    if (type_key == 0) {
      return RawDiagnosticTable.UNCOMMITTED;
    }
    else if (type_key == 0x010) {
      return RawDiagnosticTable.COMMITTED_ADDED;
    }
    else if (type_key == 0x020) {
      return RawDiagnosticTable.COMMITTED_REMOVED;
    }
    return RawDiagnosticTable.RECORD_STATE_ERROR;

  }

  /**
   * This may only be called from the 'fix' method.  It performs a full scan of
   * the records and rebuilds all the index information from the information.
   * <p>
   * This should only be used as a recovery mechanism and may not accurately
   * rebuild in some cases (but should rebuild as best as possible non the
   * less).
   */
  private synchronized void rebuildAllIndices(File path, String file_name)
                                                         throws IOException {

    // Temporary name of the index store
    File temporary_name = new File(path, file_name + ".id2");
    // Actual name of the index store
    File actual_name = new File(path, file_name + ".iid");

    // Make a new blank index store
    IndexStore temp_store = new IndexStore(temporary_name, Debug());
    // Copy the same block size as the original
    temp_store.create(index_store.getBlockSize());
    temp_store.init();
    temp_store.addIndexLists(column_count + 1, (byte) 1);

    // Get the index of rows in this table
    IndexSet index_set = temp_store.getSnapshotIndexSet();

    // The master index,
    IntegerListInterface master_index = index_set.getIndex(0);

    // The selectable schemes for the table.
    TableDataSource table = minimalTableDataSource(master_index);

    // Create a set of index for this table.
    SelectableScheme[] cols = new SelectableScheme[column_count];
    for (int i = 0; i < column_count; ++i) {
      cols[i] = createSelectableSchemeForColumn(index_set, table, i);
    }

    // For each row
    int row_count = rawRowCount();
    for (int i = 0 ; i < row_count; ++i) {
      // Is this record marked as deleted?
      if (!recordDeleted(i)) {
        // Get the type flags for this record.
        int type = recordTypeInfo(i);
        // Check if this record is marked as committed removed, or is an
        // uncommitted record.
        if (type == RawDiagnosticTable.COMMITTED_ADDED) {
          // Insert into the master index
          master_index.uniqueInsertSort(i);
          // Insert into schemes
          for (int n = 0; n < column_count; ++n) {
            cols[n].insert(i);
          }
        }
      }  // if not deleted
    }  // for each row

    // Commit the index store

    // Write the modified index set to the index store
    // (Updates the index file)
    temp_store.commitIndexSet(index_set);
    index_set.dispose();
    temp_store.flush();

    // Close and delete the original index_store
    index_store.close();
    index_store.delete();
    // Close the temporary store
    temp_store.close();
    // Rename temp file to the actual file
    boolean b = temporary_name.renameTo(actual_name);
    if (b == false) {
      throw new IOException("Unable to rename " +
                            temporary_name + " to " + actual_name);
    }
    temp_store = null;

    // Copy and open the new reference
    index_store =  new IndexStore(actual_name, Debug());
    index_store.open(false);
    index_store.init();

  }

  /**
   * This is called by the 'open' method.  It performs a scan of the records
   * and marks any rows that are uncommitted as deleted.  It also checks
   * that the row is not within the master index.
   */
  private synchronized void doOpeningScan() throws IOException {
    long in_time = System.currentTimeMillis();

    // ASSERTION: No root locks and no pending transaction changes,
    //   VERY important we assert there's no pending transactions.
    if (isRootLocked() || hasTransactionChangesPending()) {
      // This shouldn't happen if we are calling from 'open'.
      throw new Error(
                  "Odd, we are root locked or have pending journal changes.");
    }

    // This is pointless if we are in read only mode.
    if (!data_store.isReadOnly()) {
      // A journal of index changes during this scan...
      MasterTableJournal journal = new MasterTableJournal();

      // Get the master index of rows in this table
      IndexSet index_set = index_store.getSnapshotIndexSet();
      IntegerListInterface master_index = index_set.getIndex(0);

      // NOTE: We assume the index information is correct and that the
      //   allocation information is potentially bad.

      int row_count = rawRowCount();
      for (int i = 0 ; i < row_count; ++i) {
        // Is this record marked as deleted?
        if (!recordDeleted(i)) {
          // Get the type flags for this record.
          int type = recordTypeInfo(i);
          // Check if this record is marked as committed removed, or is an
          // uncommitted record.
          if (type == RawDiagnosticTable.COMMITTED_REMOVED ||
              type == RawDiagnosticTable.UNCOMMITTED) {
            // Check it's not in the master index...
            if (!master_index.contains(i)) {
              // Delete it.
              doHardRowRemove(i);
            }
            else {
              Debug().write(Lvl.ERROR, this,
                          "Inconsistant: Row is indexed but marked as " +
                          "removed or uncommitted.");
              Debug().write(Lvl.ERROR, this,
                          "Row: " + i + " Type: " + type +
                          " Table: " + getTableName());
              // Mark the row as committed added because it is in the index.
              data_store.writeRecordType(i + 1, 0x010);

            }
          }
          else {
            // Must be committed added.  Check it's indexed.
            if (!master_index.contains(i)) {
              // Not indexed, so data is inconsistant.
              Debug().write(Lvl.ERROR, this,
                 "Inconsistant: Row committed added but not in master index.");
              Debug().write(Lvl.ERROR, this,
                          "Row: " + i + " Type: " + type +
                          " Table: " + getTableName());
              // Mark the row as committed removed because it is not in the
              // index.
              data_store.writeRecordType(i + 1, 0x020);

            }
          }
        }
        else {  // if deleted
          // Check this record isn't in the master index.
          if (master_index.contains(i)) {
            // It's in the master index which is wrong!  We should remake the
            // indices.
            Debug().write(Lvl.ERROR, this,
                        "Inconsistant: Row is removed but in index.");
            Debug().write(Lvl.ERROR, this,
                        "Row: " + i + " Table: " + getTableName());
            // Mark the row as committed added because it is in the index.
            data_store.writeRecordType(i + 1, 0x010);

          }
        }
      }   // for (int i = 0 ; i < row_count; ++i)

      // Dispose the index set
      index_set.dispose();

    }

    long bench_time = System.currentTimeMillis() - in_time;
    if (Debug().isInterestedIn(Lvl.INFORMATION)) {
      Debug().write(Lvl.INFORMATION, this,
         "Opening scan for " + getFileName() + " took " + bench_time + "ms.");
    }

  }

  /**
   * Copies the persistant information in this table data source to the given
   * directory in the file system.  This makes an exact copy of the table as
   * it currently is.  It is recommended that when this is used, there is a
   * lock to prevent committed changes to the database.
   */
  synchronized void copyTo(File path) throws IOException {
    data_store.copyTo(path);
    index_store.copyTo(path);
  }

  /**
   * Returns an implementation of RawDiagnosticTable that we can use to
   * diagnose problems with the data in this source.
   */
  RawDiagnosticTable getRawDiagnosticTable() {
    return new MRawDiagnosticTable();
  }

  // ---- Optimization that saves some cycles -----

  private short s_run_total_hits = 0;
  private short s_run_file_hits = 0;

  /**
   * Some variables that are used for optimization in the 'getCellContents'
   * method.
   */
  private int OPT_last_row = -1;
  private int OPT_last_col = -1;
  private int OPT_last_skip_offset = -1;

  /**
   * Returns the cell contents of the given cell in the table.  This will
   * look up the cell in the file if it can't be found in the cell cache.  This
   * method is undefined if row has been removed or was not returned by
   * the 'addRow' method.
   */
  synchronized DataCell getCellContents(int column, int row) {

    // NOTES:
    // This is called *A LOT*.  It's a key part of the 20% of the program
    // that's run 80% of the time.
    // This performs very nicely for rows that are completely contained within
    // 1 sector.  However, rows that contain large cells (eg. a large binary
    // or a large string) and spans many sectors will not be utilizing memory
    // as well as it could.
    // The reason is because all the data for a row is read from the store even
    // if only 1 cell of the column is requested.  This will have a big
    // impact on column scans and searches.  The cell cache takes some of this
    // performance bottleneck away.
    // However, a better implementation of this method is made difficult by
    // the fact that sector spans can be compressed.  We should perhaps
    // revise the low level data storage so only sectors can be compressed.

    if (row < 0) {
      throw new Error("'row' is < 0");
    }

    // If the database stats need updating then do so now.
    if (s_run_total_hits >= 400) {
      system.stats().add(s_run_total_hits, total_hits_key);
      system.stats().add(s_run_file_hits, file_hits_key);
      s_run_total_hits = 0;
      s_run_file_hits = 0;
    }
    // Increment the total hits counter
    ++s_run_total_hits;

    // Row 0 is reserved for DataTableDef
    ++row;

    // First check if this is within the cache before we continue.
    DataCell cell;
    if (DATA_CELL_CACHING) {
      cell = cache.get(table_id, row, column);
      if (cell != null) {
        return cell;
      }
    }

    // Increment the file hits counter
    ++s_run_file_hits;

    // We maintain a cache of byte[] arrays that contain the rows read in
    // from the file.  If consequtive reads are made to the same row, then
    // this will cause lots of fast cache hits.

    try {

      // Open a stream for this row.
      InputStream in = data_store.getRecordInputStream(row);
      cell_in.setParentStream(in);

      // NOTE: This is an optimization for a common sequence of pulling cells
      //   from a row.  It remembers the index of the last column read in, and
      //   if the next column requested is > than the last column read, then
      //   it trivially skips the file pointer to the old point.
      //   Variables starting with 'OPT_' are member variables used for
      //   keeping the optimization state information.

      int start_col;
      if (OPT_last_row == row && column >= OPT_last_col) {
        cell_in.skip(OPT_last_skip_offset);
        start_col = OPT_last_col;
      }
      else {
        cell_in.skip(2);
        OPT_last_row = row;
        OPT_last_skip_offset = 2;
        OPT_last_col = 0;
        start_col = 0;
      }

      for (int i = start_col; i < column; ++i) {
        int len = data_cell_serializer.skipSerialization(cell_in);
        if (len <= 0) {
          throw new Error("Corrupt data - cell size is <= 0");
        }
        cell_in.skip(len);
        ++OPT_last_col;
        OPT_last_skip_offset += len + 4;     // ( +4 for the header )
      }
      // Read the cell
      cell = data_cell_serializer.readSerialization(cell_in);

      // And close the reader.
      cell_in.close();

      // And put in the cache and return it.
      if (DATA_CELL_CACHING) {
        cache.put(table_id, row, column, cell);
      }
      return cell;

    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new Error("IOError getting cell at (" + column + ", " +
                      row + ").");
    }

  }

  /**
   * Compares the contents of the given cell with the cell found at the given
   * position in the table and returns either LESS_THAN, GREATER_THAN or
   * EQUAL.
   */
  synchronized int compareCellTo(DataCell object, int column, int row) {

    DataCell object2 = getCellContents(column, row);
    int result = object.compareTo(object2);
    if (result > 0) {
      return Table.GREATER_THAN;
    }
    else if (result == 0) {
      return Table.EQUAL;
    }
    return Table.LESS_THAN;

  }

  /**
   * Grabs a root lock on this table.
   * <p>
   * While a MasterTableDataSource has at least 1 root lock, it may not
   * reclaim deleted space in the data store.  A root lock means that data
   * is still being pointed to in this file (even possibly committed deleted
   * data).
   */
  synchronized void addRootLock() {
    system.stats().increment(root_lock_key);
    ++root_lock;
  }

  /**
   * Removes a root lock from this table.
   * <p>
   * While a MasterTableDataSource has at least 1 root lock, it may not
   * reclaim deleted space in the data store.  A root lock means that data
   * is still being pointed to in this file (even possibly committed deleted
   * data).
   */
  synchronized void removeRootLock() {
    system.stats().decrement(root_lock_key);
    if (root_lock == 0) {
      throw new Error("Too many root locks removed!");
    }
    --root_lock;
    // If the last lock is removed, schedule a possible collection.
    if (root_lock == 0) {
      garbage_collector.scheduleCollection();
    }
  }

  /**
   * Returns true if the table is currently under a root lock (has 1 or more
   * root locks on it).
   */
  synchronized boolean isRootLocked() {
    return root_lock > 0;
  }

  synchronized String transactionChangeString() {
    return table_indices.transactionChangeString();
  }

  /**
   * Returns true if this table has any journal modifications that have not
   * yet been incorporated into master index.
   */
  synchronized boolean hasTransactionChangesPending() {
    return table_indices.hasTransactionChangesPending();
  }

  /**
   * Atomically returns the next 'unique_id' value from this file.
   */
  synchronized long nextUniqueID() {
    return index_store.nextUniqueID();
  }

  /**
   * Sets the unique id for this store.  This must only be used under
   * extraordinary circumstances, such as restoring from a backup, or
   * converting from one file to another.
   */
  synchronized void setUniqueID(long value) {
    index_store.setUniqueID(value);
  }




  // ---------- Diagnostic and repair ----------

  /**
   * Performs a complete check and repair of the table.  The table must not
   * have been opened before this method is called.  The given UserTerminal
   * parameter is an implementation of a user interface that is used to ask
   * any questions and output the results of the check.
   */
  public synchronized void checkAndRepair(File path, String file_name,
                                   UserTerminal terminal) throws IOException {

    // Set read only flag.
    is_read_only = false;

    // Open the store.
    data_store = new VariableSizeDataStore(new File(path, file_name), Debug());
    boolean need_check = data_store.open(is_read_only);
//    if (need_check) {
    data_store.fix(terminal);
//    }

    // Set up the internal state of this object
    // Get the 'table_id' of this table from the reserved area of the data
    // store.
    byte[] reserved_buffer = new byte[64];
    data_store.readReservedBuffer(reserved_buffer, 0, 64);
    table_id = ByteArrayUtil.getInt(reserved_buffer, 0);

    // Set the file name.
    this.file_name = file_name;

    // Load the table definition from the store.
    table_def = loadDataTableDef();




    // Table journal information
    table_indices = new MultiVersionTableIndices(getSystem(),
                           table_def.getTableName(), table_def.columnCount());
    // The column rid list cache
    column_rid_list = new RIDList[table_def.columnCount()];

    // Open table indices
    index_store = new IndexStore(new File(path, file_name + ".iid"), Debug());
    // Open the table index file.
    need_check = index_store.open(is_read_only);
    // Attempt to fix the table index file.
    boolean index_store_stable = index_store.fix(terminal);

    // Load internal state
    loadInternal();

    // Merge all journal changes when we open
    mergeJournalChanges(Integer.MAX_VALUE);

    // If the index store is not stable then clear it and rebuild the
    // indices.
//    if (!index_store_stable) {
      terminal.println("+ Rebuilding all index information for table!");
      rebuildAllIndices(path, file_name);
//    }

    // Do an opening scan of the table.  Any records that are uncommited
    // must be marked as deleted.
    doOpeningScan();

  }



  // ---------- Inner classes ----------

  /**
   * A RawDiagnosticTable implementation that provides direct access to the
   * root data of this table source bypassing any indexing schemes.  This
   * interface allows for the inspection and repair of data files.
   */
  private final class MRawDiagnosticTable implements RawDiagnosticTable {

    // ---------- Implemented from RawDiagnosticTable -----------

    public int physicalRecordCount() {
      try {
        return rawRowCount();
      }
      catch (IOException e) {
        throw new Error(e.getMessage());
      }
    }

    public DataTableDef getDataTableDef() {
      return MasterTableDataSource.this.getDataTableDef();
    }

    public int recordState(int record_index) {
      try {
        return recordTypeInfo(record_index);
      }
      catch (IOException e) {
        throw new Error(e.getMessage());
      }
    }

    public int recordSize(int record_index) {
      try {
        return rawRecordSize(record_index);
      }
      catch (IOException e) {
        throw new Error(e.getMessage());
      }
    }

    public DataCell getCellContents(int column, int record_index) {
      return MasterTableDataSource.this.getCellContents(column, record_index);
    }

    public String recordMiscInformation(int record_index) {
      return null;
    }

  }

  /**
   * A MutableTableDataSource object as returned by the
   * 'createTableDataSourceAtCommit' method.
   */
  private final class MMutableTableDataSource
                                           implements MutableTableDataSource {

    /**
     * The Transaction object that this MutableTableDataSource was
     * generated from.  This reference should be used only to query
     * database constraint information.
     */
    private Transaction transaction;

    /**
     * The 'commit_id' of the transaction that created this data source.
     */
    private long commit_id;

    /**
     * The 'recovery point' to which the row index in this table source has
     * rebuilt to.
     */
    private int row_list_rebuild;

    /**
     * The index that represents the rows that are within this
     * table data source within this transaction.
     */
    private IntegerListInterface row_list;

    /**
     * The 'recovery point' to which the schemes in this table source have
     * rebuilt to.
     */
    private int[] scheme_rebuilds;

    /**
     * The IndexSet for this mutable table source.
     */
    private IndexSet index_set;

    /**
     * The SelectableScheme array that represents the schemes for the
     * columns within this transaction.
     */
    private SelectableScheme[] column_schemes;

    /**
     * A journal of changes to this source since it was created.
     */
    private MasterTableJournal table_journal;

    /**
     * Constructs the data source.
     */
    public MMutableTableDataSource(Transaction transaction) {
      this.transaction = transaction;
      this.commit_id = transaction.getCommitID();
      this.index_set =
                  transaction.getIndexSetForTable(MasterTableDataSource.this);
      int col_count = getDataTableDef().columnCount();
      row_list_rebuild = 0;
      scheme_rebuilds = new int[col_count];
      column_schemes = new SelectableScheme[col_count];
      table_journal = new MasterTableJournal(getTableID());
    }

    /**
     * Returns the entire row list for this table.  This will request this
     * information from the master source.
     */
    private synchronized IntegerListInterface getRowIndexList() {
      if (row_list == null) {
        row_list = index_set.getIndex(0);
      }
      return row_list;
    }

    /**
     * Ensures that the row list is as current as the latest journal change.
     * We can be assured that when this is called, no journal changes will
     * occur concurrently.  However we still need to synchronize because
     * multiple reads are valid.
     */
    private synchronized void ensureRowIndexListCurrent() {
      int rebuild_index = row_list_rebuild;
      int journal_count = table_journal.entries();
      while (rebuild_index < journal_count) {
        byte command = table_journal.getCommand(rebuild_index);
        int row_index = table_journal.getRowIndex(rebuild_index);
        if (command == 1) {
          // Add to 'row_list'.
          boolean b = getRowIndexList().uniqueInsertSort(row_index);
          if (b == false) {
            throw new Error(
                  "Row index already used in this table (" + row_index + ")");
          }
        }
        else if (command == 2) {
          // Remove from 'row_list'
          boolean b = getRowIndexList().removeSort(row_index);
          if (b == false) {
            throw new Error("Row index removed that wasn't in this table!");
          }
        }
        else {
          throw new Error("Unrecognised journal command.");
        }
        ++rebuild_index;
      }
      // It's now current (row_list_rebuild == journal_count);
      row_list_rebuild = rebuild_index;
    }

    /**
     * Creates a TableDataSourceFactory used for constraint checks when
     * adding/removing rows.  The return object will forward the request
     * for the table on to the transaction.
     */
    private TableDataConglomerate.TableDataSourceFactory
                                               getTableDataSourceFactory() {
      return transaction.getTableDataSourceFactory();
    }


    // ---------- Implemented from MutableTableDataSource ----------

    public TransactionSystem getSystem() {
      return MasterTableDataSource.this.system;
    }

    public DataTableDef getDataTableDef() {
      return MasterTableDataSource.this.getDataTableDef();
    }

    public int getRowCount() {
      // Ensure the row list is up to date.
      ensureRowIndexListCurrent();
      return getRowIndexList().size();
    }

    public RowEnumeration rowEnumeration() {
      // Ensure the row list is up to date.
      ensureRowIndexListCurrent();
      // Get an iterator across the row list.
      final IntegerIterator iterator = getRowIndexList().iterator();
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

    public DataCell getCellContents(int column, int row) {
      return MasterTableDataSource.this.getCellContents(column, row);
    }

    public int compareCellTo(DataCell cell, int column, int row) {
      return MasterTableDataSource.this.compareCellTo(cell, column, row);
    }

    // NOTE: Returns an immutable version of the scheme...
    public synchronized SelectableScheme getColumnScheme(int column) {
      SelectableScheme scheme = column_schemes[column];
      // Cache the scheme in this object.
      if (scheme == null) {
//        System.out.println(this);
//        System.out.println(index_set + " - " + column);
        scheme = createSelectableSchemeForColumn(index_set, this, column);
        column_schemes[column] = scheme;
      }

      // NOTE: We should be assured that no write operations can occur over
      //   this section of code because writes are exclusive operations
      //   within a transaction.
      // Are there journal entries pending on this scheme since?
      int rebuild_index = scheme_rebuilds[column];
      int journal_count = table_journal.entries();
      while (rebuild_index < journal_count) {
        byte command = table_journal.getCommand(rebuild_index);
        int row_index = table_journal.getRowIndex(rebuild_index);
        if (command == 1) {
          scheme.insert(row_index);
        }
        else if (command == 2) {
          scheme.remove(row_index);
        }
        else {
          throw new Error("Unrecognised journal command.");
        }
        ++rebuild_index;
      }
      scheme_rebuilds[column] = rebuild_index;

      return scheme;
    }

    // ---------- Table Modification ----------

    public int addRow(RowData row_data) {

      // Check this isn't a read only source
      if (isReadOnly()) {
        throw new Error("Can not add row - table is read only.");
      }

      // Add to the master.
      int row_index;
      try {
        row_index = MasterTableDataSource.this.addRow(row_data);
      }
      catch (IOException e) {
        Debug().writeException(e);
        throw new Error("IO Error: " + e.getMessage());
      }

      // Note this doesn't need to be synchronized because we are exclusive on
      // this table.
      // Add this change to the table journal.
      table_journal.addEntry(MasterTableJournal.TABLE_ADD, row_index);

      // Does this addition violate any constraints on the table?
      try {
        int[] row_indices = new int[] { row_index };

        // Check for any field constraint violations in the added rows
        TableDataConglomerate.checkFieldConstraintViolations(
                                             transaction, this, row_indices);
        // Check this table, adding the given row_index, immediate
        TableDataConglomerate.checkAddConstraintViolations(
            transaction, getTableDataSourceFactory(), this,
            row_indices, Transaction.INITIALLY_IMMEDIATE);
      }
      catch (DatabaseConstraintViolationException e) {
        // There was a constraint violation so remove the row.
        table_journal.addEntry(MasterTableJournal.TABLE_REMOVE, row_index);
        throw e;
      }

      return row_index;
    }

    public void removeRow(int row_index) {

      // Check this isn't a read only source
      if (isReadOnly()) {
        throw new Error("Can not remove row - table is read only.");
      }

      // NOTE: This must <b>NOT</b> call 'removeRow' in MasterTableDataSource.
      //   We do not want to delete a row perminantly from the underlying
      //   file because the transaction using this data source may yet decide
      //   to roll back the change and not delete the row.

      // Note this doesn't need to be synchronized because we are exclusive on
      // this table.
      // Add this change to the table journal.
      table_journal.addEntry(MasterTableJournal.TABLE_REMOVE, row_index);

      // Does this row removal violate any constraints on the table?
      // Check this table, removing the given row_index, immediate
      // NOTE: This will throw DatabaseConstraintViolationException if
      //   contention detected
      TableDataConglomerate.checkRemoveConstraintViolations(
               transaction, getTableDataSourceFactory(), this,
               row_index, Transaction.INITIALLY_IMMEDIATE);

    }

    public int updateRow(int row_index, RowData row_data) {

      // Check this isn't a read only source
      if (isReadOnly()) {
        throw new Error("Can not update row - table is read only.");
      }

      // Note this doesn't need to be synchronized because we are exclusive on
      // this table.
      // Add this change to the table journal.
      table_journal.addEntry(MasterTableJournal.TABLE_REMOVE, row_index);

      // Add to the master.
      int new_row_index;
      try {
        new_row_index = MasterTableDataSource.this.addRow(row_data);
      }
      catch (IOException e) {
        Debug().writeException(e);
        throw new Error("IO Error: " + e.getMessage());
      }

      // Note this doesn't need to be synchronized because we are exclusive on
      // this table.
      // Add this change to the table journal.
      table_journal.addEntry(MasterTableJournal.TABLE_ADD, new_row_index);


      // Does this update violate any constraints on the table?
      // The removed row
      int[] row_indices = new int[] { row_index };

      // Does this row removal violate any constraints on the table?
      // Check this table, removing the given row_index, immediate
      // NOTE: This will throw DatabaseConstraintViolationException if
      //   contention detected
      TableDataConglomerate.checkRemoveConstraintViolations(
               transaction, getTableDataSourceFactory(), this,
               row_index, Transaction.INITIALLY_IMMEDIATE);

      // The new added row
      row_indices = new int[] { new_row_index };

      // Check for any field constraint violations in the added rows
      TableDataConglomerate.checkFieldConstraintViolations(
                                             transaction, this, row_indices);
      // Check this table, adding the given row_index, immediate
      TableDataConglomerate.checkAddConstraintViolations(
            transaction, getTableDataSourceFactory(), this,
            row_indices, Transaction.INITIALLY_IMMEDIATE);

      return new_row_index;
    }


    public MasterTableJournal getJournal() {
      return table_journal;
    }

    public void dispose() {
      // Dispose and invalidate the schemes
      // This is really a safety measure to ensure the schemes can't be
      // used outside the scope of the lifetime of this object.
      for (int i = 0; i < column_schemes.length; ++i) {
        SelectableScheme scheme = column_schemes[i];
        if (scheme != null) {
          scheme.dispose();
          column_schemes[i] = null;
        }
      }
      row_list = null;
      table_journal = null;
      scheme_rebuilds = null;
      index_set = null;
      transaction = null;
    }

    public void addRootLock() {
      MasterTableDataSource.this.addRootLock();
    }

    public void removeRootLock() {
      MasterTableDataSource.this.removeRootLock();
    }

  }

  /**
   * An event that cleans up transaction journals.
   */
  private class TransactionJournalCleanUpEvent implements Runnable {
    public void run() {
      synchronized (MasterTableDataSource.this) {
//        System.out.println("#### SCHEDULED CLEAN UP ####");
        // If not closed then merge journal changes up to the minimum
        // commit id of the open transactions.
        if (!isClosed()) {
          mergeJournalChanges(open_transactions.minimumCommitID(null));
        }
      }
    }
  }
  private Object commit_journals_event;

  /**
   * For diagnostic.
   */
  public String toString() {
    return "[MasterTableDataSource: " + file_name + "]";
  }

}
