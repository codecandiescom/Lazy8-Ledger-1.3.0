/**
 * com.mckoi.database.VariableSizeDataTableFile  26 Jun 2000
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
import java.math.BigDecimal;
import com.mckoi.debug.*;
import com.mckoi.util.Cache;
import com.mckoi.util.ByteArrayUtil;

/**
 * An implementation of DataTableFile, this allows for a variable row width
 * formatted table files.
 * <p>
 * The layout is quite simple.  The first record (record 0) contains a
 * serialization of the DataTableDef object that describes the columns.  All
 * records 1 onwards are rows in the table.  Each row has a header that
 * points to the offset of the cell in the row, and past the header is the
 * serializations of the row contents.
 * <p>
 *
 * @author Tobias Downer
 */

public class VariableSizeDataTableFile implements DataTableFile {


  /**
   * The maximum size of the sectors for the variable size records.
   */
  private static final int MAX_DATA_SECTOR_SIZE = 475;


  /**
   * The TransactionSystem object that this data table file is part of.
   */
  private final TransactionSystem system;

  /**
   * The database path.
   */
  private File database_path;

  /**
   * The name of this table, set when the 'load' method is called.
   */
  private String table_name;

  /**
   * The object that stores all the data.
   */
  private VariableSizeDataStore data_store;

  /**
   * This is a stored array of TableField[] objects that represent the type
   * information of the records in the database (rows in the table).
   */
  private TableField[] table_fields;

  /**
   * The DataTableDef object that describes the table topology.
   */
  private DataTableDef table_def;

  /**
   * This boolean is set to true during the 'load' procedure if when we open
   * the variable data store, there was a repair of the file.
   */
  private boolean dirty_open;

  /**
   * The current unique key that is incremented each time we are accessed.
   */
  private long unique_col_key;

  /**
   * A cache of byte[] arrays that represent rows of the table that have
   * previously been read.
   */
  private Cache row_cache;

  /**
   * The persistant object we use to read information from a byte[] array
   * row.
   */
  private CellBufferInputStream cell_in;


  /**
   * Set to true when the data store is openned.
   */
  private boolean data_store_open = false;

  /**
   * A tally of the number of locks.
   */
  private int lock_count = 0;



  /**
   * Constructs the object.  The File points to the location of the
   * database files.
   */
  public VariableSizeDataTableFile(TransactionSystem system,
                                   File database_path) {
    this.system = system;
    this.database_path = database_path;
    this.cell_in = new CellBufferInputStream();
  }


  /**
   * Returns a DebugLogger object that we use to log debug messages.
   */
  public final DebugLogger Debug() {
    return system.Debug();
  }

  /**
   * Returns a DataTableDef that represents the fields in this table file.
   * Conversion is done depending on what is saved in the file.
   */
  private DataTableDef loadDataTableDef(String name) throws IOException {

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
//    Debug().write(Lvl.MESSAGE, this, "Magic = " + mn);
    // This is the latest format...
    if (mn == 0x0bebb) {
      // Read the DataTableDef object from the input stream,
      def = DataTableDef.read(din);
    }
    else {
//      // Legacy no longer supported...
//      throw new IOException(
//                "Couldn't find magic number for table definition data.");

      // We have to convert from the old system.
      TableField[] fields = loadTableFields();

      // Convert into a DataTableDef object...
      // Make up a table description object for this entry,
      def = new DataTableDef();
      def.setName(name);
      def.setTableClass("com.mckoi.database.VariableSizeDataTableFile");
      try {
        for (int n = 0; n < fields.length; ++n) {
          def.addColumn(fields[n]);
        }
      }
      catch (DatabaseException e) {
        Debug().writeException(e);
        throw new IOException("Database Error: " + e.getMessage());
      }
    }

    table_fields = def.toTableFieldArray();

    return def;
  }

  /**
   * Loads the TableField[] array from the store.
   * <p>
   * @deprecated this is a legacy method.
   */
  private TableField[] loadTableFields() throws IOException {

    // Read record 0 which contains all this info.
    byte[] d = new byte[65536];
    int read = data_store.read(0, d, 0, 65536);
    ByteArrayInputStream bin = new ByteArrayInputStream(d, 0, read);

    // LEGACY: There's a bit of legacy code here.  The TableField array
    //   describing each column used to be stored as a Serializable array but
    //   we converted to a format that's more specifiable.
    //   We are supporting both methods currently, but we want to convert over
    //   to the new format.

    // Try reading the magic number signifying the new TableField format.
    TableField[] c_fields;

    DataInputStream din = new DataInputStream(bin);
    if (din.readInt() == 0x0beba) {
      // This is the new format

      int field_count = din.readInt();
      c_fields = new TableField[field_count];

      for (int i = 0; i < field_count; ++i) {
        String name = din.readUTF();
        int type = din.readInt();
        int size = din.readInt();
        boolean not_null = din.readBoolean();
        boolean unique = din.readBoolean();
        int unique_group = din.readInt();

        byte version = din.readByte();
        int sql_type = din.readInt();
        int scale = din.readInt();
        // Reserved space...
        din.skip(23); //32);

        c_fields[i] = new TableField(name, type, size, not_null);
        if (unique) c_fields[i].setUnique();
        c_fields[i].setUniqueGroup(unique_group);
        if (version > 0) {
          c_fields[i].setSQLType(sql_type);
          c_fields[i].setScale(scale);
        }
      }

    }
    else {
      // This is the old format using classes that were in the original
      // com.dasoft namespace.

      // Reset the underlying stream and wrap around an Object stream
      bin = new ByteArrayInputStream(d, 0, read);
      ObjectInputStream o_in = new ObjectInputStream(bin);

      // Read the array as an Object
      com.dasoft.database.TableField[] table_fields;
      try {
        table_fields = (com.dasoft.database.TableField[]) o_in.readObject();
      }
      catch (Exception e) {
        Debug().writeException(e);
        throw new IOException("Invalid table fields.");
      }
      o_in.close();

      // Convert each element to com.mckoi namespace TableField.
      c_fields = new TableField[table_fields.length];
      for (int i = 0; i < c_fields.length; ++i) {
        String name = table_fields[i].getName();
        int type = table_fields[i].getType();
        int size = table_fields[i].getSize();
        boolean nn = table_fields[i].isNotNull();
        boolean unique = table_fields[i].isUnique();
        int unique_group = table_fields[i].getUniqueGroup();

        c_fields[i] = new TableField(name, type, size, nn);
        if (unique) c_fields[i].setUnique();
        c_fields[i].setUniqueGroup(unique_group);

      }
    }

    return c_fields;
  }

  /**
   * Saves the DataTableDef object to the store.
   */
  private void saveDataTableDef(DataTableDef def) throws IOException {

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);

    dout.writeInt(0x0bebb);
    def.write(dout);

//    dout.writeInt(fields.length);
//
//    for (int i = 0; i < fields.length; ++i) {
//      TableField field = fields[i];
//      dout.writeUTF(field.getName());
//      dout.writeInt(field.getType());
//      dout.writeInt(field.getSize());
//      dout.writeBoolean(field.isNotNull());
//      dout.writeBoolean(field.isUnique());
//      dout.writeInt(field.getUniqueGroup());
//      dout.writeByte(1);  // Version...
//      dout.writeInt(field.getSQLType());
//      dout.writeInt(field.getScale());
//
//      // Reserved space...
//      for (int n = 0; n < 23; ++n) {
//        dout.write(0);
//      }
//    }

    // Write the byte array to the data store,

    byte[] d = bout.toByteArray();
    int rindex = data_store.write(d, 0, d.length);

    // rindex MUST be 0 else we buggered.
    if (rindex != 0) {
      throw new IOException("Couldn't write table fields to record 0.");
    }

  }

  /**
   * Determines if one TableField[] array contains identical entries to another
   * TableField[] array.
   */
  private static boolean areFieldsDifferent(
                                 TableField[] fields1, TableField[] fields2) {
    if (fields1.length != fields2.length) {
      return true;
    }
    for (int i = 0; i < fields1.length; ++i) {
      boolean found = false;
      int n = 0;
      while (found == false && n <= i) {
        if (fields1[i].equals(fields2[n])) {
          found = true;
        }
        ++n;
      }
      if (!found) {
        return true;
      }
    }
    return false;
  }

  /**
   * Finds the index of the field within the array of fields.
   */
  private int findField(TableField[] fields, TableField field) {
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i].getName().equals(field.getName())) {
        return i;
      }
    }
    return -1;
  }



  /**
   * Creates a table file with the given table name and table definition.
   * This method allows for a sector size to be given.  A smaller sector size
   * is best for tables that contain rows with little information.
   */
  private void create(String table_name, DataTableDef def,
                      int sector_size) throws IOException {

    // Ensure the sector size if 91 < sector_size < MAX_DATA_SECTOR_SIZE
    sector_size = Math.min(sector_size, MAX_DATA_SECTOR_SIZE);
    sector_size = Math.max(sector_size, 91);

    // The store instance.
    data_store = new VariableSizeDataStore(
                 new File(database_path, table_name), sector_size, Debug());

    // Does it exist?
    if (data_store.exists()) {
      throw new Error("Can't create table file - it already exists!");
    }

    // If not, open a new instance (in read/write mode)
    data_store.open(false);

    // Write out the table definitions object.
    saveDataTableDef(def);

    // Write 0's into the reserved buffer
    byte[] b = new byte[128];
    data_store.writeReservedBuffer(b, 0, 128);

    // Close the store
    data_store.close();

    // Now load the empty table (in read/write mode)
    load(table_name, false);

  }

  /**
   * Creates a table file with the given table name and table definition.
   * This will calculate a sector size for the new table given the column
   * information.
   */
  private void create(String table_name, DataTableDef def) throws IOException {

    // Calculate a reasonable size for the sector given the limited information
    // we have about the table.
    TableField[] fields = def.toTableFieldArray();

    // Look at table field and predict a size.
    int sector_size = 0;
    for (int i = 0; i < fields.length; ++i) {
      TableField field = fields[i];
      DataCell dummy = DataCellFactory.createDataCell(field);
      sector_size += dummy.sizeof();
    }
    sector_size = Math.min(sector_size / 2, MAX_DATA_SECTOR_SIZE);
    sector_size = Math.max(sector_size, 91);

    // Create the table with this sector size.
    create(table_name, def, sector_size);

  }

  /**
   * Updates this table to the new specification given in the DataTableDef
   * object.  The 'sector_size' argument specifies the sector_size to give the
   * updated table.  If 'sector_size' is -1 then the sector_size from the
   * current table is used.
   */
  public boolean update(DataTableDef def, int sector_size) throws IOException {

    // Don't allow update if we have a lock on this table.
    if (hasRowsLocked()) {
      throw new Error("Can't update table - it is locked.  " +
                      "Close all references to this table.");
    }

    String table_name = def.getName();

    // Perserve the table name
    String original_table_name = table_name;

    // Shut everything down first.
    shutdown();

    if (!VariableSizeDataStore.exists(database_path, original_table_name)) {
      create(def);
      return true;
    }
    else {

      Debug().write(Lvl.MESSAGE, this,
                  "Updating table: " + original_table_name);

      // If sector_size == -1 then sector size is inherited from this table.
      if (sector_size < 0) {
        sector_size = data_store.sectorSize();
      }

      // Open the original table (in read/write mode)
      load(original_table_name, false);

      boolean changed = false;

      // If sector size different, then update changed,
      if (data_store.sectorSize() != sector_size) {
        changed = true;
      }

      // PENDING: Check for changes between given 'def' and current 'table_def'
      //   If no differences then don't bother writing out a new table.
      changed = true;

      // If not changed, then return false.
      if (!changed) {
        return false;
      }

      // Yes different, so we need to create a new table and move all the
      // data from the old table into the new one.
      Debug().write(Lvl.MESSAGE, this,
                                "Detected change in table: " + table_name);

      String temp_table_name = table_name + "MODAMOD";
      // Delete the temp file just incase it exists.
      VariableSizeDataStore.delete(database_path, temp_table_name);
      // Create a new table which is the same name as this with 'MODAMOD'
      // appended to the end, eg. "Part" -> "PartMODAMOD".
      VariableSizeDataTableFile new_table =
                    new VariableSizeDataTableFile(getSystem(), database_path);
      // When creating the new table, use the same sector size from the
      // original table or from the argument.
      new_table.create(temp_table_name, def, sector_size);

      // Retreive the TableField[] in the old definition.
      TableField[] old_fields = table_def.toTableFieldArray();
      // Retreive the TableField[] in the new definition.
      TableField[] fields = def.toTableFieldArray();

      // A count of the rows added to the new table.
      int row_transfer_count = 0;
      // Lock the new table so deleted rows can't be reclaimed,
      new_table.addRowsLock();
      // Run through each row index and copy the data into the new table.
      int raw_row_count = (int) data_store.rawRecordCount() - 1;
      for (int i = 0; i < raw_row_count; ++i) {

        // Only update if the row is valid (not deleted),
        // NOTE: This has the side effect of compacting the table.
        if (isRowValid(i)) {

          int row_cells = fields.length;
          RowData row_data = new RowData(null, row_cells);

          // For each cell in the row.
          for (int n = 0; n < row_cells; ++n) {
            int s_column = findField(old_fields, fields[n]);
            DataCell cell;
            if (s_column != -1) {
              cell = getCellContents(s_column, i);
            }
            else {
              // Need to create a new value for the entry,
              cell = DataCellFactory.createDataCell(fields[n]);
              // If 'not null'
              if (fields[n].isNotNull()) {
                Debug().write(Lvl.WARNING, this,
                 "Fudging 'not null' value for field: " + fields[n].getName());
                if (cell instanceof BooleanDataCell) {
                  cell = new BooleanDataCell(Boolean.FALSE);
                }
                else if (cell instanceof DecimalDataCell) {
                  cell = new DecimalDataCell(new BigDecimal(-1));
                }
                else if (cell instanceof StringDataCell) {
                  cell = new StringDataCell(Integer.MAX_VALUE, "-");
                }
                else {
                  Debug().write(Lvl.ERROR, this,
                   "Forcing 'NotNull' field to 'null' " + fields[n].getName());
                }
              }
            }
            row_data.setColumnData(n, cell);

          }

          // Write row to new table.
          int rec_id = new_table.addRow(row_data);
          // Add 1 to the row transfer count.
          ++row_transfer_count;

        }

      }
      // Remove the lock on the new table.
      new_table.removeRowsLock();

      // Copy across the unique id
      new_table.setUniqueKey(nextUniqueKey());

      // Now shutdown the newly created table.
      new_table.shutdown();

      // Shutdown this table.
      shutdown();

      // Delete this table data.
      VariableSizeDataStore.delete(database_path, original_table_name);

      // Rename the old data to the new data.
      VariableSizeDataStore.rename(database_path, temp_table_name,
                                   database_path, original_table_name);

      // Load the new updated table (in read/write mode)
      load(original_table_name, false);

      // Set up the correct locks,
      for (int i = lock_count; i > 0; --i) {
        data_store.lock();
      }

      // Output some stat information to the log file about the alter command.
      Debug().write(Lvl.INFORMATION, this, "Alter command complete.");
      Debug().write(Lvl.INFORMATION, this,
                  "Rows in original: " + raw_row_count);
      Debug().write(Lvl.INFORMATION, this,
                  "Rows in altered destinstaion: " + row_transfer_count);

      // Return 'true' because we updated the table.
      return true;

    }

  }

  // ---------- Implemented from DataTableFile ----------

  public void create(DataTableDef def) throws IOException {
    create(def.getName(), def);
  }


  public boolean update(DataTableDef def) throws IOException {
    return update(def, -1);
  }

  public boolean doMaintenance() throws IOException {
    // Force maintenance of parent if it was dirty opening any of the
    // database files.
    return dirty_open;
  }

  public void load(String table_name, boolean read_only) throws IOException {

    this.table_name = table_name;
    // Create the row cache
    row_cache = new Cache(90);

    // Open the data store.
    data_store = new VariableSizeDataStore(
                              new File(database_path, table_name), Debug());
    // Set 'dirty_open' to true if the file had to repair itself when it
    // opened.
    dirty_open = data_store.open(read_only);

    // Read the DataTableDef from the file,
    // This has a side effect of setting the table_field object.
    table_def = loadDataTableDef(table_name);

    // Read last unique key from the reserved buffer
    byte[] b = new byte[8];
    data_store.readReservedBuffer(b, 0, 8);
    unique_col_key = ByteArrayUtil.getLong(b, 0);

    data_store_open = true;

//    // Return the fields.
//    return table_def;
  }

  public void shutdown() throws IOException {
    if (data_store_open) {
      data_store.close();
      data_store_open = false;
    }
//    row_cache.removeAll();
    row_cache = null;
    table_def = null;
    this.table_name = null;
  }

  public void drop() {
    if (data_store_open == false) {
      data_store.delete();
    }
    else {
      throw new Error("The data store must be closed before it is shutdown.");
    }
  }

  public void updateFile() throws IOException {
    data_store.synch();
  }

  public void addRowsLock() {
    if (Debug().isInterestedIn(Lvl.INFORMATION)) {
      Debug().write(Lvl.INFORMATION, this, "AddRowsLock: " + table_name);
    }
    data_store.lock();
    ++lock_count;
  }

  public void removeRowsLock() {
    if (Debug().isInterestedIn(Lvl.INFORMATION)) {
      Debug().write(Lvl.INFORMATION, this, "RemoveRowsLock: " + table_name);
    }
    data_store.unlock();
    --lock_count;
  }

  public boolean hasRowsLocked() {
    return data_store.locked();
  }

  public boolean isRowValid(int record_index) throws IOException {
    return !data_store.recordDeleted(record_index + 1);
  }

  public int addRow(RowData data) throws IOException {

    int record_index;

    // Set up a buffered data output stream to stream the information to a
    // byte array.
    CellBufferOutputStream ba_out = new CellBufferOutputStream(2048);
    DataOutputStream temp_out = new DataOutputStream(ba_out);

    // Allocate a header
    int header_size = (table_fields.length * 4);
    int[] header = new int[table_fields.length];

    // Size of the row in the file,
    int row_size = 2 + header_size;

    // Seek past the row header information,
    ba_out.seek(row_size);

    // Write out each cell of the row data.
    int row_cells = data.getColumnCount();
    for (int i = 0; i < row_cells; ++i) {
      DataCell cell = data.getCellData(i);
      int cell_size = DataCellFactory.writeDataCell(cell, temp_out);
      header[i] = row_size;
      row_size += cell_size;
    }

    // Write out the header to the buffer,
    ba_out.seek(0);
    // Reserved for future use.
    temp_out.writeShort(0);
    for (int i = 0; i < header.length; ++i) {
      temp_out.writeInt(header[i]);
    }

    // Write out the buffer
    byte[] row_bytes = ba_out.getByteArray();
    record_index = data_store.write(row_bytes, 0, row_size);

// NOTE: We can't put the row array in the cache for two reasons,
//   1) The size will be incorrect,
//   2) We may want to reuse the array for another write operation.
//
//    // Put the entry in the row cache.
//    row_cache.put(new Integer(record_index), row_bytes);

    // Update the relational model with the addition of this record,
    int row_number = record_index - 1;

    // Return the record index of the new data in the table
    return row_number;

  }

  public void removeRow(int record_index) throws IOException {
    ++record_index;
    data_store.delete(record_index);
    // If this row is in the row cache then remove it.
    // This'll force next cell access to refresh the cache.
    row_cache.remove(new Integer(record_index));
  }







  private byte[] unique_byte = new byte[8];

  public long nextUniqueKey() throws IOException {
    long cur_key = unique_col_key;
    ++unique_col_key;
    ByteArrayUtil.setLong(unique_col_key, unique_byte, 0);
    // If read only, we can't update header...
    if (!data_store.isReadOnly()) {
      data_store.writeReservedBuffer(unique_byte, 0, 8);
    }
    return cur_key;
  }

  // ---------- From TableDataSource ----------

  public TransactionSystem getSystem() {
    return system;
  }

  public int getRowCount() {
    return data_store.usedRecordCount() - 1;
  }

  public DataTableDef getDataTableDef() {
    return table_def;
//    return loadDataTableDef(table_name);
  }

  public RowEnumeration rowEnumeration() {
    return null;
  }

  public DataCell getCellContents(int column, int row) {

    ++row;

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

    // Is this row in the cache?
    byte[] row_bytes;

    // We maintain a cache of byte[] arrays that contain the rows read in
    // from the file.  If consequtive reads are made to the same row, then
    // this will cause lots of fast cache hits.

    try {

      Integer row_integer = new Integer(row);
      row_bytes = (byte[]) row_cache.get(row_integer);
      if (row_bytes == null) {
        // No, the row isn't cached,
        // so read the record in
        row_bytes = data_store.readRecord(row);
        // and put it into the cache
        row_cache.put(row_integer, row_bytes);
      }

      cell_in.setArray(row_bytes);
      // Skip ahead to the header info for this column
      int col_head = 2 + (column * 4);
      cell_in.skip(col_head);
      cell_in.skip(cell_in.readInt() - col_head - 4);

      return DataCellFactory.readDataCell(cell_in);

    }
    catch (IOException e) {
      Debug().writeException(e);
      throw new Error("IOError getting cell at (" + column + ", " +
                      row + ").");
    }

  }

  /**
   * Compares the given DataCell object, with the cell at the given position
   * and returns a value that describes the relation between the two values.
   * This is a helper function.
   */
  public int compareCellTo(DataCell object, int column, int row) {
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
   * Returns the selectable scheme for the given column.
   * <p>
   * NOTE: This isn't implemented in this file yet.
   */
  public SelectableScheme getColumnScheme(int column) {
    throw new Error("Method not implemented yet.");
  }



  // ---------- Package protected ----------

  /**
   * Sets the unique key as returned via 'nextUniqueKey'.<p>
   * <strong>NOTE:</strong> This absolutely should never be used for anything
   * except when converting file types.
   */
  void setUniqueKey(long key) throws IOException {
    ByteArrayUtil.setLong(key, unique_byte, 0);
    data_store.writeReservedBuffer(unique_byte, 0, 8);
  }

  // ---------- Public methods that are used for checking, diagnostics and
  //            statistics specific with VariableSizeDataTable ----------

  /**
   * Performs a series of checks on the given row number.  If a check fails
   * then it returns a string that describes what is wrong with the record.
   * If it returns 'null' then the record passed.
   */
  public String checkRow(int row) throws IOException {
    byte[] row_bytes = data_store.readRecord(row + 1);
    return null;
  }

  /**
   * Returns the raw count of rows that are in this file.  This includes
   * deleted rows.
   */
  public int vsdtRawRowCount() throws IOException {
    return (int) data_store.rawRecordCount() - 1;
  }

  /**
   * Returns the size of the complete row found at the given row offset
   * including the header.
   */
  public int vsdtRowSize(int row) throws IOException {
    byte[] row_bytes = data_store.readRecord(row + 1);
    return row_bytes.length;
  }

  /**
   * Returns the number of sectors this row consumes.
   */
  public int vsdtSectorCount(int row) throws IOException {
    return data_store.recordSectorCount(row + 1);
  }

  /**
   * Returns the size in bytes of a sector of the data store.
   */
  public int vsdtSectorSize() throws IOException {
    return data_store.sectorSize();
  }

  /**
   * Returns true if the given row has been compressed or false if it's not
   * compressed.
   */
  public boolean vsdtIsRowCompressed(int row) throws IOException {
    return data_store.isCompressed(row + 1);
  }

  /**
   * Returns the amount of bytes the data part of this store takes up.  This
   * is an indication of the amount of space in the file system this is
   * using.  However, it does not account for the size of the allocation
   * table, and includes space that isn't directly used for storing the data
   * in this file (headers, etc).
   */
  public long vsdtTotalDataSize() {
    return data_store.totalStoreSize();
  }

}
