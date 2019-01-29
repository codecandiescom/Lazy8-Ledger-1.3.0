/**
 * com.mckoi.database.InsertSearch  14 Mar 1998
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

import com.mckoi.util.IntegerVector;
import com.mckoi.util.BlockIntegerList;
import com.mckoi.util.IntegerListInterface;
import com.mckoi.util.IndexComparator;
import com.mckoi.util.IntegerIterator;
import java.util.Comparator;
import java.util.Arrays;
import java.io.*;
//import com.mckoi.debug.*;

/**
 * This is a SelectableScheme similar in some ways to the binary tree.  When
 * a new row is added, it is inserted into a sorted list of rows.  We can then
 * use this list to select out the sorted list of elements.
 * <p>
 * This requires less memory than the BinaryTree, however it is not as fast.
 * Even though, it should still perform fairly well on medium size data sets.
 * On large size data sets, insert and remove performance may suffer.
 * <p>
 * This object retains knowledge of all set elements unlike BlindSearch which
 * has no memory overhead.
 * <p>
 * Performance should be very comparable to BinaryTree for sets that aren't
 * altered much.
 * <p>
 * @author Tobias Downer
 */

public final class InsertSearch extends SelectableScheme {

  /**
   * Some statics.
   */
  private static final BlockIntegerList EMPTY_LIST;
  private static final BlockIntegerList ONE_LIST;

//  private static int UID_DIFF = 8;


  static {
    EMPTY_LIST = new BlockIntegerList();
    EMPTY_LIST.setImmutable();
    ONE_LIST = new BlockIntegerList();
    ONE_LIST.add(0);
    ONE_LIST.setImmutable();
  }

  /**
   * The sorted list of rows in this set.  This is sorted from min to max
   * (not sorted by row number - sorted by entity row value).
   */
  private IntegerListInterface set_list;

  /**
   * If this is true, then this SelectableScheme records additional rid
   * information that can be used to very quickly identify whether a value is
   * greater, equal or less.
   */
  boolean RECORD_UID;

  /**
   * An object that stores the RID information for this column in the master
   * table.  This object may be shared between other schemes.
   */
  private RIDList rid_list;



//  private String CONST_TRACE;


  /**
   * The IndexComparator that we use to refer elements in the set to actual
   * data objects.
   */
  private IndexComparator set_comparator;

  // ----- DEBUGGING -----

  /**
   * If this is immutable, this stores the number of entries in 'set_list'
   * when this object was made.
   */
  private int DEBUG_immutable_set_size;




  /**
   * The Constructor.
   */
  public InsertSearch(TableDataSource table, int column) {
    super(table, column);
    set_list = new BlockIntegerList();

    // The internal comparator that enables us to sort and lookup on the data
    // in this column.
    setupComparator();


//    StringWriter w = new StringWriter();
//    PrintWriter out = new PrintWriter(w);
//    try {
//      throw new Error("InsertSearch constructor.");
//    }
//    catch (Throwable e) {
//      e.printStackTrace(out);
//    }
//    out.flush();
//    CONST_TRACE = w.toString();

  }

  /**
   * Constructor sets the scheme with a pre-sorted list.  The Vector 'vec'
   * should not be used again after this is called.  'vec' must be sorted from
   * low key to high key.
   */
  public InsertSearch(TableDataSource table, int column, IntegerVector vec) {
    this(table, column);
    for (int i = 0; i < vec.size(); ++i) {
      set_list.add(vec.intAt(i));
    }

    // NOTE: This must be removed in final, this is a post condition check to
    //   make sure 'vec' is infact sorted
    checkSchemeSorted();

  }

  /**
   * Constructor sets the scheme with a pre-sorted list.  The list 'list'
   * should not be used again after this is called.  'list' must be sorted from
   * low key to high key.
   */
  InsertSearch(TableDataSource table, int column, IntegerListInterface list) {
    this(table, column);
    this.set_list = list;

    // NOTE: This must be removed in final, this is a post condition check to
    //   make sure 'vec' is infact sorted
    checkSchemeSorted();

  }

  /**
   * Constructs this as a copy of the given, either mutable or immutable
   * copy.
   */
  private InsertSearch(TableDataSource table, InsertSearch from,
                       boolean immutable) {
    super(table, from.getColumn());

    if (immutable) {
      setImmutable();
    }

    if (immutable) {
      // Immutable is a shallow copy
      set_list = from.set_list;
      DEBUG_immutable_set_size = set_list.size();
    }
    else {
      set_list = new BlockIntegerList(from.set_list);
    }

    // Do we generate lookup caches?
    RECORD_UID = from.RECORD_UID;

    rid_list = from.rid_list;
//    hash_uid_difference = from.hash_uid_difference;

//    // Copy other variables.
//    if (from.uid_list != null) {
//      if (immutable) {
//        // Shallow copy if immutable
//        uid_list = from.uid_list;
//      }
//      else {
//        uid_list = new IntegerVector(from.uid_list);
//      }
//      hash_uid_difference = from.hash_uid_difference;
//    }

    // The internal comparator that enables us to sort and lookup on the data
    // in this column.
    setupComparator();

  }

  /**
   * Set the global RIDList for this scheme for this column.
   */
  void setRIDList(RIDList rid_list) {
    // Do we generate lookup caches?
    RECORD_UID = getSystem().lookupComparisonListEnabled();
    this.rid_list = rid_list;
  }

  /**
   * Sets the internal comparator that enables us to sort and lookup on the
   * data in this column.
   */
  private void setupComparator() {
    set_comparator = new IndexComparator() {

      private int internalCompare(int index, DataCell cell2) {
        DataCell cell1 = getCellContents(index);
        return cell1.compareTo(cell2);
      }

      public int compare(int index, Object val) {
        return internalCompare(index, (DataCell) val);
      }
      public int compare(int index1, int index2) {
        DataCell cell = getCellContents(index2);
        return internalCompare(index1, cell);
      }
    };
  }






  /**
   * Inserts a row into the list.  This will always be thread safe, table
   * changes cause a write lock which prevents reads while we are writing to
   * the table.
   */
  public void insert(int row) {
    if (isImmutable()) {
      throw new Error("Tried to change an immutable scheme.");
    }

    DataCell cell = getCellContents(row);
    set_list.insertSort(cell, row, set_comparator);

//    [ This is now done when we insert a record into the master table ]
//    if (rid_list != null) {
//      rid_list.insertRID(cell, row, set_list);
//    }

//    if (uid_list != null) {
//      // Insert the uid into the list.  In the worse case this may result in
//      // an expensive re-building of the uid table ( which is O(n) )
//      insertUID(cell, row);
//    }

//    // This will force a re-cache of all cells in the column.
//    // NOTE: This could be optimised so that under some circumstances
//    //   we wouldn't need to re-create the whole uid list next access (if
//    //   we give each uid a gap
//    if (uid_list != null) {
//      // The number of uid caches created.
//      Database.stats().increment("InsertSearch.uid_caches_destroyed");
//      uid_list = null;
//    }
  }

  /**
   * Removes a row from the list.  This will always be thread safe, table
   * changes cause a write lock which prevents reads while we are writing to
   * the table.
   */
  public void remove(int row) {
    if (isImmutable()) {
      throw new Error("Tried to change an immutable scheme.");
    }

    DataCell cell = getCellContents(row);
    int removed = set_list.removeSort(cell, row, set_comparator);

    if (removed != row) {
      throw new Error("Removed value different than row asked to remove.  " +
                      "To remove: " + row + "  Removed: " + removed);
    }

//    if (uid_list != null) {
//      // Set this uid marker to blank (0).
//      uid_list.placeIntAt(0, row);
//    }
  }

//  /**
//   * Calculates the 'hash_uid_difference' variable.  This dictates the
//   * difference between hashing entries.
//   */
//  private void calcHashUIDDifference(int size) {
//    if (size == 0) {
//      hash_uid_difference = 32;
//    }
//    else {
//      hash_uid_difference = (65536 * 4096) / size;
//      if (hash_uid_difference > 16384) {
//        hash_uid_difference = 16384;
//      }
//      else if (hash_uid_difference < 8) {
//        hash_uid_difference = 8;
//      }
//    }
////    System.out.println(hash_uid_difference);
//  }
//
//  /**
//   * Rehashes the entire uid list.  This goes through the entire list from
//   * first sorted entry to last and spaces out each uid so that there's 16
//   * numbers between each entry.
//   */
//  private int rehashUIDList(int old_uid_place) {
//    calcHashUIDDifference(set_list.size());
//
//    int new_uid_place = -1;
//
//    int cur_uid = 0;
//    int old_uid = 0;
//    IntegerIterator iterator = set_list.iterator();
//
//    while (iterator.hasNext()) {
//      int row_index = iterator.next();
//      if (row_index >= 0 && row_index < uid_list.size()) {
//        int old_value = uid_list.intAt(row_index);
//        int new_value;
//
//        if (old_value == 0) {
//          cur_uid += hash_uid_difference;
//          new_uid_place = cur_uid;
//        }
//        else {
//          if (old_value != old_uid) {
//            old_uid = old_value;
//            cur_uid += hash_uid_difference;
//            new_value = cur_uid;
//          }
//          else {
//            new_value = cur_uid;
//          }
//          uid_list.placeIntAt(new_value, row_index);
//        }
//      }
//    }
//
//    if (new_uid_place == -1) {
//      throw new Error(
//                "Post condition not correct - new_uid_place shouldn't be -1");
//    }
//
//    Database.stats().increment("InsertSearch.rehash_uid_table");
//
//    return new_uid_place;
//  }
//
//  /**
//   * Algorithm for inserting a new row into the uid table.  For most cases
//   * this should be a very fast method.
//   */
//  private void insertUID(DataCell cell, int row) {
//
//    // Place a zero to mark the new row
//    uid_list.placeIntAt(0, row);
//
//    int given_uid = -1;
////    DataCell next_cell;
//    DataCell previous_cell;
//
//    // The index of this cell in the list
//    int set_index = set_list.searchLast(cell, set_comparator);
//
//    if (set_list.get(set_index) != row) {
//      throw new Error(
//                     "set_list.searchLast(cell) didn't turn up expected row.");
//    }
//
//    int next_set_index = set_index + 1;
//    if (next_set_index >= set_list.size()) {
//      next_set_index = -1;
//    }
//    int previous_set_index = set_index - 1;
//
//    int next_uid;
//    if (next_set_index > -1) {
//      next_uid = uid_list.intAt(set_list.get(next_set_index));
//    }
//    else {
//      if (previous_set_index > -1) {
//        // If at end and there's a previous set then use that as the next
//        // uid.
//        next_uid = uid_list.intAt(set_list.get(previous_set_index)) +
//                  (hash_uid_difference * 2);
//      }
//      else {
//        next_uid = (hash_uid_difference * 2);
//      }
//    }
//    int previous_uid;
//    if (previous_set_index > -1) {
//      previous_uid = uid_list.intAt(set_list.get(previous_set_index));
//    }
//    else {
//      previous_uid = 0;
//    }
//
//    // Are we the same as the previous or next cell in the list?
//    if (previous_set_index > -1) {
//      previous_cell = getCellContents(set_list.get(previous_set_index));
//      if (previous_cell.compareTo(cell) == 0) {
//        given_uid = previous_uid;
//      }
////      else {
////        if (next_set_index > -1) {
////          next_cell = getCellContents(set_list.get(next_set_index));
////          if (next_cell.compareTo(cell) == 0) {
////            given_uid = next_uid;
////          }
////        }
////      }
//    }
//
//    // If not given a uid yet,
//    if (given_uid == -1) {
//      if (previous_uid + 1 == next_uid) {
//        // There's no room so we have to rehash the uid list.
//        given_uid = rehashUIDList(next_uid);
//      }
//      else {
//        given_uid = ((next_uid + 1) + (previous_uid - 1)) / 2;
//      }
//    }
//
//    // Finally (!!) - set the uid for this row.
//    uid_list.placeIntAt(given_uid, row);
//
//  }
//
//  /**
//   * If uid_list is null then create it now.
//   */
//  private void createUIDCache() {
//    // Synchronize on 'uid_lock' incase of synchronization issues (since
//    // we can have multiple threads calling this method).
//    synchronized (uid_lock) {
//      if (uid_list == null) {
//
//        calcHashUIDDifference(set_list.size());
//
//        int set_size = set_list.size();
//        uid_list = new IntegerVector(set_size + 128);
//
//        // Go through 'set_list'.  All entries that are equal are given the
//        // same uid.
//        if (set_size > 0) {
//          int cur_uid = hash_uid_difference;
//          IntegerIterator iterator = set_list.iterator();
//          int row_index = iterator.next();
//          DataCell last_cell = getCellContents(row_index);
//          uid_list.placeIntAt(cur_uid, row_index);
//
//          while (iterator.hasNext()) {
//            row_index = iterator.next();
//            DataCell cur_cell = getCellContents(row_index);
//            int cmp = cur_cell.compareTo(last_cell);
//            if (cmp > 0) {
//              cur_uid += hash_uid_difference;
//            }
//            else if (cmp < 0) {
//              // If current cell is less than last cell then the list ain't
//              // sorted!
//              throw new Error("Internal Database Error: Index is corrupt " +
//                              " - InsertSearch list is not sorted.");
//            }
//            uid_list.placeIntAt(cur_uid, row_index);
//
//            last_cell = cur_cell;
//          }
//        }
//
//        // The number of uid caches created.
//        Database.stats().increment(
//                             "{session} InsertSearch.uid_caches_created");
//        // The total size of all uid indices that we have created.
//        Database.stats().add(uid_list.size(),
//                             "{session} InsertSearch.uid_indices");
//
//      }
//    }
//  }

  /**
   * This needs to be called to access 'set_comparator' in thread busy
   * methods.  Because creating a UID cache will modify set_comparator, we
   * need to make sure we access this variable safely.
   * <p>
   * NOTE: This is a throwback method for an idea I had to speed up the
   *   'select*' methods, but it proved unworkable.  The reason being that
   *   the UID only contains knowledge of relations between rows, and the
   *   'select*' methods find the relationship of a DataCell in the column
   *   set.
   */
  private final IndexComparator safeSetComparator() {
//    synchronized (uid_lock) {
      return set_comparator;
//    }
  }




  /**
   * Returns 'row_set' ordered by this index.  The returned set is stable,
   * meaning if values are equal they stay in the same order as they came in.
   */
  public BlockIntegerList internalOrderIndexSet(final IntegerVector row_set) {
    // The length of the set to order
    int row_set_length = row_set.size();

    // Trivial cases where sorting is not required:
    // NOTE: We use immutable objects to save some memory.
    if (row_set_length == 0) {
      return EMPTY_LIST;
    }
    else if (row_set_length == 1) {
      return ONE_LIST;
    }

    // if 'row_set' is particularly small, then it is best to use the non-
    // RECORD_UID method which in the worse case results in a very expensive
    // cache regeneration algorithm.

    // This stops hash lists being created if we only need to sort through
    // < 250 entries.
    boolean small_row_set = (row_set_length <= 250);
//    boolean small_row_set = false;

    // The condition for requesting a build of a rid list for this column.
    // rid_list not built, and RECORD_UID set, and it's not a small set.
    if (rid_list != null && !rid_list.isBuilt() &&
        RECORD_UID && !small_row_set) {
      rid_list.requestBuildRIDList();
    }

    // Use the standard method for sorting (slower than using rid list but
    // doesn't require the resources a rid list consumes).
    if (rid_list == null || !rid_list.isBuilt()) {

      // This will be 'row_set' sorted by its entry lookup.  This must only
      // contain indices to row_set entries.
      BlockIntegerList new_set = new BlockIntegerList();

      // The comparator we use to sort
      IndexComparator comparator = new IndexComparator() {
        public int compare(int index, Object val) {
          DataCell cell = getCellContents(row_set.intAt(index));
          return cell.compareTo((DataCell) val);
        }
        public int compare(int index1, int index2) {
          throw new Error("Shouldn't be called!");
        }
      };

      // Fill new_set with the set { 0, 1, 2, .... , row_set_length }
      for (int i = 0; i < row_set_length; ++i) {
        DataCell cell = getCellContents(row_set.intAt(i));
        new_set.insertSort(cell, i, comparator);
      }

      return new_set;

    }
    else {

      // This is the record_uid method for sorting.
      // The UID cache is a big map that maps row number to UID number.
      // The UID number is different for each data value that is different.
      // NOTE: This call occurs when we have a search such as,
      //   col1 > 'a' and col2 > 'a'

// [ RID list creation is now done in the background ]
//      // Create the uid cache - (could be really expensive for big lists).
//      // NOTE: This has to be called, don't shortcut and test if uid_list is
//      //   null outside call because synchronization in the called method is
//      //   essential.
//      if (!rid_list.isBuilt()) {
//        rid_list.createRIDCache(set_list);
//      }

//      // This will be 'row_set' sorted by its entry lookup.  This must only
//      // contain indices to row_set entries.
//      BlockIntegerList new_set = new BlockIntegerList();
//
//      // The comparator we use to sort
//      IndexComparator comparator = new IndexComparator() {
//        public int compare(int index, Object val) {
//          int rid_val = rid_list.valueForRow(row_set.intAt(index));
//          int rid_val2 = ((Integer) val).intValue();
//          return rid_val - rid_val2;
//        }
//        public int compare(int index1, int index2) {
//          throw new Error("Shouldn't be called!");
//        }
//      };
//
//      // Fill new_set with the set { 0, 1, 2, .... , row_set_length }
//      synchronized (rid_list) {
//        for (int i = 0; i < row_set_length; ++i) {
//          Integer rid_val =
//                        new Integer(rid_list.valueForRow(row_set.intAt(i)));
//          new_set.insertSort(rid_val, i, comparator);
//        }
//      }


      BlockIntegerList new_set = rid_list.sortedSet(row_set);

      getSystem().stats().increment(
                                 "{session} InsertSearch.rid_based_searches");

      return new_set;

    }

  }

  /**
   * Returns a scheme to handle a sub-set of the rows in this scheme.  This is
   * for when we 'select' a group of etities from a set, and we need to form
   * a new scheme with which to select elements from.
   * <p>
   * This does three things:
   * <p>
   * + Retrieves all the rows from the Table from first to last and resolves
   *   into this Tables domain, retaining the order.
   * + Orders the rows into this scheme ordering.
   * + Returns the data back into its original domain, retaining sorted order.
   */
  public SelectableScheme getSubsetScheme(Table subset_table,
                                          int subset_column) {

    // Resolve table rows in this table scheme domain.
    IntegerVector row_set = new IntegerVector(subset_table.getRowCount());
    RowEnumeration e = subset_table.rowEnumeration();
    while (e.hasMoreRows()) {
      row_set.addInt(e.nextRowIndex());
    }
    subset_table.setToRowTableDomain(subset_column, row_set, getTable());

//    IntegerVector row_set =
//                       subset_table.getResolvedRowSet(subset_column, table);

    // Generates an IntegerVector which contains indices into 'row_set' in
    // sorted order.
    BlockIntegerList new_set = internalOrderIndexSet(row_set);

    // Our 'new_set' should be the same size as 'row_set'
    if (new_set.size() != row_set.size()) {
      throw new RuntimeException("Internal sort error in finding sub-set.");
    }

    // Set up a new SelectableScheme with the sorted index set.
    // Move the sorted index set into the new scheme.
    InsertSearch is = new InsertSearch(subset_table, subset_column, new_set);
    // Don't let subset schemes create uid caches.
    is.RECORD_UID = false;
    return is;

  }

  /**
   * Reads the entire state of the scheme from the input stream.  Throws an
   * exception if the scheme is not empty.
   */
  public void readFrom(InputStream in) throws IOException {
    if (set_list.size() != 0) {
      throw new RuntimeException("Error reading scheme, already a set in the Scheme");
    }
    DataInputStream din = new DataInputStream(in);
    int vec_size = din.readInt();

    int row_count = getTable().getRowCount();
    // Check we read in as many indices as there are rows in the table
    if (row_count != vec_size) {
      throw new IOException(
         "Different table row count to indices in scheme. " +
         "table=" + row_count +
         ", vec_size=" + vec_size);
    }

    for (int i = 0; i < vec_size; ++i) {
      int row = din.readInt();
      if (row < 0) { // || row >= row_count) {
        set_list = new BlockIntegerList();
        throw new IOException("Scheme contains out of table bounds index.");
      }
      set_list.add(row);
    }

    getSystem().stats().add(vec_size, "{session} InsertSearch.read_indices");

    // NOTE: This must be removed in final, this is a post condition check to
    //   make sure 'vec' is infact sorted
    checkSchemeSorted();
  }

  /**
   * Writes the entire state of the scheme to the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dout = new DataOutputStream(out);
    int list_size = set_list.size();
    dout.writeInt(list_size);

    IntegerIterator i = set_list.iterator(0, list_size - 1);
    while (i.hasNext()) {
      dout.writeInt(i.next());
    }
  }

  /**
   * Returns an exact copy of this scheme including any optimization
   * information.  The copied scheme is identical to the original but does not
   * share any parts.  Modifying any part of the copied scheme will have no
   * effect on the original and vice versa.
   */
  public SelectableScheme copy(TableDataSource table, boolean immutable) {
    // ASSERTION: If immutable, check the size of the current set is equal to
    //   when the scheme was created.
    if (isImmutable()) {
      if (DEBUG_immutable_set_size != set_list.size()) {
        throw new Error("Assert failed: " +
                        "Immutable set size is different from when created.");
      }
    }

    // We must create a new InsertSearch object and copy all the state
    // information from this object to the new one.
    return new InsertSearch(table, this, immutable);
  }

  /**
   * Disposes this scheme.
   */
  public void dispose() {
    // Close and invalidate.
    set_list = null;
    rid_list = null;
    set_comparator = null;
  }

  /**
   * Checks that the scheme is in sorted order.  This is a debug check to
   * ensure we maintain a sorted index.
   * NOTE: This *MUST* be removed in a release version because it uses up
   *   many cycles for each check.  It also breaks modifications log recovery.
   */
  private void checkSchemeSorted() {
//    int list_size = set_list.size();
//    DataCell last_cell = null;
//    for (int i = 0; i < list_size; ++i) {
//      int row = set_list.intAt(i);
//      DataCell this_cell = getCellContents(row);
//      if (last_cell != null) {
//        if (this_cell.compareTo(last_cell) < 0) {
//          throw new Error("checkSchemeSorted failed.  Corrupt index.");
//        }
//      }
//      last_cell = this_cell;
//    }
//    if (Debug().isInterestedIn(Lvl.WARNING)) {
//      StringBuffer info_string = new StringBuffer();
//      info_string.append("POST CONDITION CHECK - Checked index of size: ");
//      info_string.append(list_size);
//      info_string.append(".  Sorted correctly (REMOVE THIS CHECK IN FINAL)");
//      Debug().write(Lvl.WARNING, this, new String(info_string));
//    }
  }


  /**
   * Returns an IntegerVector that represents the range of values between
   * the start and end offset of 'set_list' (inclusive).
   */
  private final IntegerVector range(int start, int end) {
//    System.out.println("RANGE");
//    System.out.println(start);
//    System.out.println(end);

    // If either start or end are <0 then empty range.
    if (start < 0 || end < 0) {
      return new IntegerVector(0);
    }

    IntegerIterator i = set_list.iterator(start, end);
    IntegerVector vec = new IntegerVector((end - start) + 2);
    while (i.hasNext()) {
      vec.addInt(i.next());
    }
    return vec;
  }

  /**
   * Adds the set indexes to the list that represent the range of values
   * between the start and end offset (inclusive) given.
   */
  private final IntegerVector addRangeToSet(int start, int end,
                                            IntegerVector ivec) {
    if (ivec == null) {
      ivec = new IntegerVector((end - start) + 2);
    }
    IntegerIterator i = set_list.iterator(start, end);
    while (i.hasNext()) {
      ivec.addInt(i.next());
    }
    return ivec;
  }

  /**
   * Returns an IntegerVector that represents the range of all values
   * <b>not</b> between the start and end offset of 'set_list' (inclusive).
   */
  private final IntegerVector inverseRange(int start, int end) {
//    System.out.println("INVERSE");
//    System.out.println(start);
//    System.out.println(end);

    // If either start or end are <0 then full range.
    if (start < 0 || end < 0) {
      start = 0;
      end = -1;
    }

    int set_size = set_list.size();
    IntegerVector vec = new IntegerVector((set_size - (end - start)) + 2);

    if (start > 0) {
      IntegerIterator i = set_list.iterator(0, start - 1);
      while (i.hasNext()) {
        vec.addInt(i.next());
      }
    }
    IntegerIterator i = set_list.iterator(end + 1, set_size - 1);
    while (i.hasNext()) {
      vec.addInt(i.next());
    }

    return vec;
  }



  /**
   * The select operations for this scheme.
   */

  public IntegerVector selectAll() {
    IntegerVector ivec = new IntegerVector(set_list);
    return ivec;
  }

  public IntegerVector selectFirst() {
    return selectRange(new SelectableRange(
             SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
             SelectableRange.LAST_VALUE, SelectableRange.FIRST_IN_SET));

//    if (set_list.size() == 0) {
//      return new IntegerVector(0);
//    }
//    DataCell first = getCellContents(set_list.get(0));
//    return range(0, set_list.searchLast(first, safeSetComparator()));
  }

  public IntegerVector selectNotFirst() {
    return selectRange(new SelectableRange(
             SelectableRange.AFTER_LAST_VALUE, SelectableRange.FIRST_IN_SET,
             SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));

//    if (set_list.size() == 0) {
//      return new IntegerVector(0);
//    }
//    DataCell first = getCellContents(set_list.get(0));
//    return range(set_list.searchLast(first, safeSetComparator()) + 1,
//                 set_list.size() - 1);
  }

  public IntegerVector selectLast() {
    return selectRange(new SelectableRange(
             SelectableRange.FIRST_VALUE, SelectableRange.LAST_IN_SET,
             SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));

//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//    DataCell last = getCellContents(set_list.get(set_size - 1));
//    return range(set_list.searchFirst(last, safeSetComparator()), set_size - 1);
  }

  public IntegerVector selectNotLast() {
    return selectRange(new SelectableRange(
             SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
             SelectableRange.BEFORE_FIRST_VALUE, SelectableRange.LAST_IN_SET));

//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//    DataCell last = getCellContents(set_list.get(set_size - 1));
//    return range(0, set_list.searchFirst(last, safeSetComparator()) - 1);
  }




//  public IntegerVector selectEqual(DataCell ob) {
//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//
//    IndexComparator safe_comparator = safeSetComparator();
//    return range(set_list.searchFirst(ob, safe_comparator),
//                 set_list.searchLast(ob, safe_comparator));
//  }
//
//  public IntegerVector selectNotEqual(DataCell ob) {
//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//
//    IndexComparator safe_comparator = safeSetComparator();
//    return inverseRange(set_list.searchFirst(ob, safe_comparator),
//                        set_list.searchLast(ob, safe_comparator));
//  }
//
//  public IntegerVector selectGreater(DataCell ob) {
//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//
//    int r1 = set_list.searchLast(ob, safeSetComparator());
//    if (r1 < 0) {
//      r1 = -(r1 + 1);
//    }
//    else {
//      r1 = r1 + 1;
//    }
//
//    return range(r1, set_size - 1);
//  }
//
//  public IntegerVector selectLess(DataCell ob) {
//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//
//    int r1 = set_list.searchFirst(ob, safeSetComparator());
//    if (r1 < 0) {
//      r1 = -(r1 + 1);
//    }
//
//    return range(0, r1 - 1);
//  }
//
//  public IntegerVector selectGreaterOrEqual(DataCell ob) {
//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//
//    int r1 = set_list.searchFirst(ob, safeSetComparator());
//    if (r1 < 0) {
//      r1 = -(r1 + 1);
//    }
//
//    return range(r1, set_size - 1);
//  }
//
//  public IntegerVector selectLessOrEqual(DataCell ob) {
//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//
//    int r1 = set_list.searchLast(ob, safeSetComparator());
//    if (r1 < 0) {
//      r1 = -(r1 + 1) - 1;
//    }
//
//    return range(0, r1);
//  }
//
//  // All rows >= ob1 and < ob2
//  public IntegerVector selectBetween(DataCell ob1, DataCell ob2) {
//    int set_size = set_list.size();
//    if (set_size == 0) {
//      return new IntegerVector(0);
//    }
//
//    IndexComparator safe_comparator = safeSetComparator();
//    int r1 = set_list.searchFirst(ob1, safe_comparator);
//    if (r1 < 0) {
//      r1 = -(r1 + 1);
//    }
//    int r2 = set_list.searchFirst(ob2, safe_comparator);
//    if (r2 < 0) {
//      r2 = -(r2 + 1);
//    }
//
//    return range(r1, r2 - 1);
//  }
//




  /**
   * Given a flag (FIRST_VALUE, LAST_VALUE, BEFORE_FIRST_VALUE or
   * AFTER_LAST_VALUE) and a value which is either a place marker (first, last
   * in set) or a DataCell object, this will determine the position in this
   * set of the range point.  For example, we may want to know the index of
   * the last instance of a particular number in a set of numbers which
   * would be 'positionOfRangePoint(SelectableRange.LAST_VALUE,
   * [number DataCell])'.
   * <p>
   * Note how the position is determined if the value is not found in the set.
   * <p>
   * Conditions:
   *   set_list must not be empty,
   */
  private int positionOfRangePoint(byte flag, Object val) {
    int p;
    DataCell cell;

    switch(flag) {

      case(SelectableRange.FIRST_VALUE):
        if (val == SelectableRange.FIRST_IN_SET) {
          return 0;
        }
        if (val == SelectableRange.LAST_IN_SET) {
          // Get the last value and search for the first instance of it.
          cell = getCellContents(set_list.get(set_list.size() - 1));
        }
        else {
          cell = (DataCell) val;
        }
        p = set_list.searchFirst(cell, safeSetComparator());
        // (If value not found)
        if (p < 0) {
          return -(p + 1);
        }
        return p;

      case(SelectableRange.LAST_VALUE):
        if (val == SelectableRange.LAST_IN_SET) {
          return set_list.size() - 1;
        }
        if (val == SelectableRange.FIRST_IN_SET) {
          // Get the first value.
          cell = getCellContents(set_list.get(0));
        }
        else {
          cell = (DataCell) val;
        }
        p = set_list.searchLast(cell, safeSetComparator());
        // (If value not found)
        if (p < 0) {
          return -(p + 1) - 1;
        }
        return p;

      case(SelectableRange.BEFORE_FIRST_VALUE):
        if (val == SelectableRange.FIRST_IN_SET) {
          return -1;
        }
        if (val == SelectableRange.LAST_IN_SET) {
          // Get the last value and search for the first instance of it.
          cell = getCellContents(set_list.get(set_list.size() - 1));
        }
        else {
          cell = (DataCell) val;
        }
        p = set_list.searchFirst(cell, safeSetComparator());
        // (If value not found)
        if (p < 0) {
          return -(p + 1) - 1;
        }
        return p - 1;

      case(SelectableRange.AFTER_LAST_VALUE):
        if (val == SelectableRange.LAST_IN_SET) {
          return set_list.size();
        }
        if (val == SelectableRange.FIRST_IN_SET) {
          // Get the first value.
          cell = getCellContents(set_list.get(0));
        }
        else {
          cell = (DataCell) val;
        }
        p = set_list.searchLast(cell, safeSetComparator());
        // (If value not found)
        if (p < 0) {
          return -(p + 1);
        }
        return p + 1;

      default:
        throw new Error("Unrecognised flag.");
    }

  }

  /**
   * Adds a range from this set to the given IntegerVector.  IntegerVector may
   * be null if a list has not yet been allocated for the range.
   */
  private IntegerVector addRange(SelectableRange range, IntegerVector ivec) {
    int r1, r2;

    // Select the range specified.
    byte start_flag = range.getStartFlag();
    Object start = range.getStart();
    byte end_flag = range.getEndFlag();
    Object end = range.getEnd();

    r1 = positionOfRangePoint(start_flag, start);
    r2 = positionOfRangePoint(end_flag, end);

    if (r2 < r1) {
      return ivec;
    }

    // Add the range to the set
    return addRangeToSet(r1, r2, ivec);

  }


  public IntegerVector selectRange(SelectableRange range) {
    int set_size = set_list.size();
    // If no items in the set return an empty set
    if (set_size == 0) {
      return new IntegerVector(0);
    }

    IntegerVector ivec = addRange(range, null);
    if (ivec == null) {
      return new IntegerVector(0);
    }

    return ivec;

  }

  public IntegerVector selectRange(SelectableRange[] ranges) {
    int set_size = set_list.size();
    // If no items in the set return an empty set
    if (set_size == 0) {
      return new IntegerVector(0);
    }

    IntegerVector ivec = null;
    for (int i = 0; i < ranges.length; ++i) {
      SelectableRange range = ranges[i];
      ivec = addRange(range, ivec);
    }

    if (ivec == null) {
      return new IntegerVector(0);
    }
    return ivec;

  }


}
