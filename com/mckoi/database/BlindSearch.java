/**
 * com.mckoi.database.BlindSearch  14 Mar 1998
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
import java.util.Arrays;
import java.util.Comparator;
import com.mckoi.util.IntegerVector;
import com.mckoi.util.BlockIntegerList;

/**
 * This is a scheme that performs a blind search of a given set.  It records
 * no information about how a set element relates to the rest.  It blindly
 * searches through the set to find elements that match the given criteria.
 * <p>
 * This scheme performs badly on large sets because it requires that the
 * database is queried often for information.  However since it records no
 * information about the set, memory requirements are non-existant.
 * <p>
 * This scheme should not be used for anything other than small domain sets
 * because the performance suffers very badly with larger sets.  It is ideal
 * for small domain sets because of its no memory overhead.  For any select
 * operation this algorithm must check every element in the set.
 * <p>
 * @author Tobias Downer
 */

public class BlindSearch extends SelectableScheme {

  /**
   * The Constructor.
   */
  public BlindSearch(TableDataSource table, int column) {
    super(table, column);
  }

  /**
   * This scheme doesn't take any notice of insertions or removals.
   */
  public void insert(int row) {
    if (isImmutable()) {
      throw new Error("Tried to change an immutable scheme.");
    }
  }

  /**
   * This scheme doesn't take any notice of insertions or removals.
   */
  public void remove(int row) {
    if (isImmutable()) {
      throw new Error("Tried to change an immutable scheme.");
    }
  }

  /**
   * <p>
   */
  public BlockIntegerList internalOrderIndexSet(IntegerVector row_set) {
    // The length of the set to order
    int row_set_length = row_set.size();

    // This will be 'row_set' sorted by its entry lookup.  This must only
    // contain indices to row_set entries.
    BlockIntegerList new_set = new BlockIntegerList(); //row_set_length);

    // Trivial cases where sorting is not required:
    if (row_set_length == 0) {
      return new_set;
    }
    else if (row_set_length == 1) {
      new_set.add(0);
      return new_set;
    }

    // Make a new set of objects that can be easily sorted.
    SetElement[] elems = new SetElement[row_set_length];
    for (int i = 0; i < elems.length; ++i) {
      SetElement se = new SetElement();
      se.raw_row_value = row_set.intAt(i);
      se.virtual_row_value = i;
      elems[i] = se;
    }

    // Brute force sort algorithm using entry lookup.

    // This is a standard sort.  The elements are sorted by the given
    // comparator which queries the database for the cell contents.

    // Sort the elements by the LookupComparator
    LookupComparator comparator = new LookupComparator();
    Arrays.sort(elems, comparator);

    // Put the sorted elements into 'new_set'
    for (int i = 0; i < elems.length; ++i) {
      new_set.add(elems[i].virtual_row_value);
    }

    // Return the ordered index set
    return new_set;
  }

  /**
   * Asks the Scheme for a SelectableScheme abject that describes a sub-set
   * of the set handled by this Scheme.  The given int[] array is an unsorted
   * list or rows to include in the subset.  Note that any scheme obtained via
   * this method will become invalid if the parent scheme is altered.
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
   * Reads the entire state of the scheme from the input stream.
   * This is a trivial case for BlindSearch which doesn't require any
   * data to be stored.
   */
  public void readFrom(InputStream in) throws IOException {
  }

  /**
   * Writes the entire state of the scheme to the output stream.
   * This is a trivial case for BlindSearch which doesn't require any
   * data to be stored.
   */
  public void writeTo(OutputStream out) throws IOException {
  }

  /**
   * Returns an exact copy of this scheme including any optimization
   * information.  The copied scheme is identical to the original but does not
   * share any parts.  Modifying any part of the copied scheme will have no
   * effect on the original and vice versa.
   */
  public SelectableScheme copy(TableDataSource table, boolean immutable) {
    // Return a fresh object.  This implementation has no state so we can
    // ignore the 'immutable' flag.
    return new BlindSearch(table, getColumn());
  }

  /**
   * Disposes and invalidates the BlindSearch.
   */
  public void dispose() {
    // Nothing to do!
  }

  /**
   * Selection methods for obtaining various sub-sets of information from the
   * set.
   */

  /**
   * We implement an insert sort algorithm here.  Each new row is inserted
   * into our row vector at the sorted corrent position.
   * The algorithm assumes the given vector is already sorted.  We then just
   * subdivide the set until we can insert at the required position.
   */
  private int search(DataCell ob, IntegerVector vec, int lower, int higher) {
    if (lower >= higher) {
      if (compareCellTo(ob, vec.intAt(lower)) == Table.GREATER_THAN) {
        return lower + 1;
      }
      else {
        return lower;
      }
    }

    int mid = lower + ((higher - lower) / 2);
    int comp_result = compareCellTo(ob, vec.intAt(mid));

    if (comp_result == Table.EQUAL) {
      return mid;
    }
    else if (comp_result == Table.LESS_THAN) {
      return search(ob, vec, lower, mid - 1);
    }
    else {
      return search(ob, vec, mid + 1, higher);
    }

  }

  /**
   * Searches for a given DataCell (ob) in the row list between the two
   * bounds.  This will return the highest row of the set of values that are
   * equal to 'ob'.
   * <p>
   * This returns the place to insert ob into the vector, it should not be
   * used to determine if ob is in the list or not.
   */
  private int highestSearch(DataCell ob, IntegerVector vec,
                            int lower, int higher) {

    if ((higher - lower) <= 5) {
      // Start from the bottom up until we find the highest val
      for (int i = higher; i >= lower; --i) {
        int res = compareCellTo(ob, vec.intAt(i));
        if (res == Table.EQUAL || res == Table.GREATER_THAN) {
          return i + 1;
        }
      }
      // Didn't find return lowest
      return lower;
    }

    int mid = (lower + higher) / 2;
    int comp_result = compareCellTo(ob, vec.intAt(mid));

    if (comp_result == Table.EQUAL) {
      // We know the bottom is between 'mid' and 'higher'
      return highestSearch(ob, vec, mid, higher);
    }
    else if (comp_result == Table.LESS_THAN) {
      return highestSearch(ob, vec, lower, mid - 1);
    }
    else {
      return highestSearch(ob, vec, mid + 1, higher);
    }
  }


  private void doInsertSort(IntegerVector vec, int row) {
    int list_size = vec.size();
    if (list_size == 0) {
      vec.addInt(row);
    }
    else {
      int point = highestSearch(getCellContents(row), vec, 0, list_size - 1);
      if (point == list_size) {
        vec.addInt(row);
      }
      else {
        vec.insertIntAt(row, point);
      }
    }
  }



  public IntegerVector selectAll() {
    IntegerVector row_list = new IntegerVector(getTable().getRowCount());
    RowEnumeration e = getTable().rowEnumeration();
    while (e.hasMoreRows()) {
      doInsertSort(row_list, e.nextRowIndex());
    }
    return row_list;
  }

  public IntegerVector selectFirst() {
    IntegerVector row_list = selectAll();
    int size = row_list.size();
    if (size > 0) {
      DataCell cell = getCellContents(row_list.intAt(0));
      int cut_at = 1;
      for (; cut_at < size; ++cut_at) {
        if (compareCellTo(cell, row_list.intAt(cut_at)) != Table.EQUAL) {
          row_list.crop(0, cut_at);
          return row_list;
        }
      }
    }
    return row_list;
  }

  public IntegerVector selectNotFirst() {
    IntegerVector row_list = selectAll();
    int size = row_list.size();
    if (size > 0) {
      DataCell cell = getCellContents(row_list.intAt(0));
      int cut_at = 1;
      for (; cut_at < size; ++cut_at) {
        if (compareCellTo(cell, row_list.intAt(cut_at)) != Table.EQUAL) {
          row_list.crop(cut_at, size);
          return row_list;
        }
      }
    }
    return row_list;
  }

  public IntegerVector selectLast() {
    IntegerVector row_list = selectAll();
    int size = row_list.size();
    if (size > 0) {
      int cut_at = size - 1;
      DataCell cell = getCellContents(row_list.intAt(cut_at));
      --cut_at;
      for (; cut_at >= 0; --cut_at) {
        if (compareCellTo(cell, row_list.intAt(cut_at)) != Table.EQUAL) {
          row_list.crop(cut_at + 1, size);
          return row_list;
        }
      }
    }
    return row_list;
  }

  public IntegerVector selectNotLast() {
    IntegerVector row_list = selectAll();
    int size = row_list.size();
    if (size > 0) {
      int cut_at = size - 1;
      DataCell cell = getCellContents(row_list.intAt(cut_at));
      --cut_at;
      for (; cut_at >= 0; --cut_at) {
        if (compareCellTo(cell, row_list.intAt(cut_at)) != Table.EQUAL) {
          row_list.crop(0, cut_at + 1);
          return row_list;
        }
      }
    }
    return row_list;
  }

//  public IntegerVector selectEqual(DataCell ob) {
//    IntegerVector row_list = new IntegerVector();
//    RowEnumeration e = getTable().rowEnumeration();
//    while (e.hasMoreRows()) {
//      int row = e.nextRowIndex();
//      if (compareCellTo(ob, row) == Table.EQUAL) {
//        doInsertSort(row_list, row);
//      }
//    }
//    return row_list;
//  }
//
//  public IntegerVector selectNotEqual(DataCell ob) {
//    IntegerVector row_list = new IntegerVector();
//    RowEnumeration e = getTable().rowEnumeration();
//    while (e.hasMoreRows()) {
//      int row = e.nextRowIndex();
//      if (compareCellTo(ob, row) != Table.EQUAL) {
//        doInsertSort(row_list, row);
//      }
//    }
//    return row_list;
//  }
//
//  public IntegerVector selectGreater(DataCell ob) {
//    IntegerVector row_list = new IntegerVector();
//    RowEnumeration e = getTable().rowEnumeration();
//    while (e.hasMoreRows()) {
//      int row = e.nextRowIndex();
//      if (compareCellTo(ob, row) == Table.LESS_THAN) {
//        doInsertSort(row_list, row);
//      }
//    }
//    return row_list;
//  }
//
//  public IntegerVector selectLess(DataCell ob) {
//    IntegerVector row_list = new IntegerVector();
//    RowEnumeration e = getTable().rowEnumeration();
//    while (e.hasMoreRows()) {
//      int row = e.nextRowIndex();
//      if (compareCellTo(ob, row) == Table.GREATER_THAN) {
//        doInsertSort(row_list, row);
//      }
//    }
//    return row_list;
//  }
//
//  public IntegerVector selectGreaterOrEqual(DataCell ob) {
//    IntegerVector row_list = new IntegerVector();
//    RowEnumeration e = getTable().rowEnumeration();
//    while (e.hasMoreRows()) {
//      int row = e.nextRowIndex();
//      if (compareCellTo(ob, row) != Table.GREATER_THAN) {
//        doInsertSort(row_list, row);
//      }
//    }
//    return row_list;
//  }
//
//  public IntegerVector selectLessOrEqual(DataCell ob) {
//    IntegerVector row_list = new IntegerVector();
//    RowEnumeration e = getTable().rowEnumeration();
//    while (e.hasMoreRows()) {
//      int row = e.nextRowIndex();
//      if (compareCellTo(ob, row) != Table.LESS_THAN) {
//        doInsertSort(row_list, row);
//      }
//    }
//    return row_list;
//  }
//
//  // All rows >= ob1 and < ob2
//  public IntegerVector selectBetween(DataCell ob1, DataCell ob2) {
//    IntegerVector row_list = new IntegerVector();
//    RowEnumeration e = getTable().rowEnumeration();
//    while (e.hasMoreRows()) {
//      int row = e.nextRowIndex();
//      if (compareCellTo(ob1, row) != Table.GREATER_THAN &&
//          compareCellTo(ob2, row) == Table.GREATER_THAN) {
//        doInsertSort(row_list, row);
//      }
//    }
//    return row_list;
//  }
//


  public IntegerVector selectRange(SelectableRange range) {
    int set_size = getTable().getRowCount();
    // If no items in the set return an empty set
    if (set_size == 0) {
      return new IntegerVector(0);
    }

    return selectRange(new SelectableRange[] { range } );
  }

  public IntegerVector selectRange(SelectableRange[] ranges) {
    int set_size = getTable().getRowCount();
    // If no items in the set return an empty set
    if (set_size == 0) {
      return new IntegerVector(0);
    }

    RangeChecker checker = new RangeChecker(ranges);
    return checker.resolve();
  }


  // ---------- Inner classes ----------

  /**
   * This is a Comparator for SetElement objects that will lookup the raw row
   * and compare the cell contents with the given SetElement.
   */
  final class LookupComparator implements Comparator {

    public int compare(Object o1, Object o2) {
      int row_value1 = ((SetElement) o1).raw_row_value;
      int row_value2 = ((SetElement) o2).raw_row_value;
      DataCell cell1 = getCellContents(row_value1);
      DataCell cell2 = getCellContents(row_value2);
      return cell1.compareTo(cell2);
    }

  };

  /**
   * Set object for using with the 'getSubsetScheme' method.
   */
  static final class SetElement implements Comparable {
    int raw_row_value;
    int virtual_row_value;

    public int compareTo(Object c) {
      SetElement elem = (SetElement) c;
      return raw_row_value - elem.raw_row_value;
    }

    public boolean equals(Object c) {
      SetElement elem = (SetElement) c;
      return raw_row_value == elem.raw_row_value;
    }

  }

  /**
   * Object used to during range check loop.
   */
  final class RangeChecker {

    /**
     * The sorted list of all items in the set created as a cache for finding
     * the first and last values.
     */
    private IntegerVector sorted_set = null;

    /**
     * The list of flags for each check in the range.
     * Either 0 for no check, 1 for < or >, 2 for <= or >=.
     */
    private byte[] lower_flags;
    private byte[] upper_flags;

    /**
     * The DataCell objects to check against.
     */
    private DataCell[] lower_cells;
    private DataCell[] upper_cells;

    /**
     * Constructs the checker.
     */
    public RangeChecker(SelectableRange[] ranges) {
      int size = ranges.length;
      lower_flags = new byte[size];
      upper_flags = new byte[size];
      lower_cells = new DataCell[size];
      upper_cells = new DataCell[size];
      for (int i = 0; i < ranges.length; ++i) {
        setupRange(i, ranges[i]);
      }
    }

    private void resolveSortedSet() {
      if (sorted_set == null) {
//        System.out.println("SLOW RESOLVE SORTED SET ON BLIND SEARCH.");
        sorted_set = selectAll();
      }
    }

    /**
     * Resolves a cell.
     */
    private DataCell resolveCell(Object ob) {
      if (ob == SelectableRange.FIRST_IN_SET) {
        resolveSortedSet();
        return getCellContents(sorted_set.intAt(0));

      }
      else if (ob == SelectableRange.LAST_IN_SET) {
        resolveSortedSet();
        return getCellContents(sorted_set.intAt(sorted_set.size() - 1));
      }
      else {
        return (DataCell) ob;
      }
    }

    /**
     * Set up a range.
     */
    public void setupRange(int i, SelectableRange range) {
      Object l = range.getStart();
      byte lf = range.getStartFlag();
      Object u = range.getEnd();
      byte uf = range.getEndFlag();

      // Handle lower first
      if (l == SelectableRange.FIRST_IN_SET &&
          lf == SelectableRange.FIRST_VALUE) {
        // Special case no lower check
        lower_flags[i] = 0;
      }
      else {
        if (lf == SelectableRange.FIRST_VALUE) {
          lower_flags[i] = 2;  // >=
        }
        else if (lf == SelectableRange.AFTER_LAST_VALUE) {
          lower_flags[i] = 1;  // >
        }
        else {
          throw new Error("Incorrect lower flag.");
        }
        lower_cells[i] = resolveCell(l);
      }

      // Now handle upper
      if (u == SelectableRange.LAST_IN_SET &&
          uf == SelectableRange.LAST_VALUE) {
        // Special case no upper check
        upper_flags[i] = 0;
      }
      else {
        if (uf == SelectableRange.LAST_VALUE) {
          upper_flags[i] = 2;  // <=
        }
        else if (uf == SelectableRange.BEFORE_FIRST_VALUE) {
          upper_flags[i] = 1;  // <
        }
        else {
          throw new Error("Incorrect upper flag.");
        }
        upper_cells[i] = resolveCell(u);
      }

    }

    /**
     * Resolves the ranges.
     */
    public IntegerVector resolve() {
      // The idea here is to only need to scan the column once to find all
      // the cells that meet our criteria.
      IntegerVector ivec = new IntegerVector();
      RowEnumeration e = getTable().rowEnumeration();

      int compare_tally = 0;

      int size = lower_flags.length;
      while (e.hasMoreRows()) {
        int row = e.nextRowIndex();
        // For each range
range_set:
        for (int i = 0; i < size; ++i) {
          boolean result = true;
          byte lf = lower_flags[i];
          if (lf != 0) {
            ++compare_tally;
            int compare = compareCellTo(lower_cells[i], row);
            if (lf == 1) {  // >
              result = (compare == Table.LESS_THAN);
            }
            else if (lf == 2) {  // >=
              result = (compare != Table.GREATER_THAN);
            }
            else {
              throw new Error("Incorrect flag.");
            }
          }
          if (result) {
            byte uf = upper_flags[i];
            if (uf != 0) {
              ++compare_tally;
              int compare = compareCellTo(upper_cells[i], row);
              if (uf == 1) { // <
                result = (compare == Table.GREATER_THAN);
              }
              else if (uf == 2) { // <=
                result = (compare != Table.LESS_THAN);
              }
              else {
                throw new Error("Incorrect flag.");
              }
            }
            // Pick this row
            if (result) {
              doInsertSort(ivec, row);
              break range_set;
            }
          }
        }
      }

//      System.out.println("Blind Search compare tally: " + compare_tally);

      return ivec;
    }

  }

}

