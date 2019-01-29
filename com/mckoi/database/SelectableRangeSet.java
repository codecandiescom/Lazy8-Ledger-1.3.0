/**
 * com.mckoi.database.SelectableRangeSet  18 Nov 2001
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
import java.util.ListIterator;

/**
 * Represents a complex normalized range of a list.  This is essentially a
 * set of SelectableRange objects that make up a complex view of a range.  For
 * example, say we had a query '(a > 10 and a < 20 and a <> 15) or a >= 50',
 * we could represent this range by the following range set;
 * <p><pre>
 * RANGE: AFTER_LAST_VALUE 10, BEFORE_FIRST_VALUE 15
 * RANGE: AFTER_LAST_VALUE 15, BEFORE_FIRST_VALUE 20
 * RANGE: FIRST_VALUE 50, LAST_VALUE LAST_IN_SET
 * </pre><p>
 * The range is constructed by calls to 'intersect', and 'union'.
 *
 * @author Tobias Downer
 */

public class SelectableRangeSet {

  /**
   * The list of ranges.
   */
  private ArrayList range_set;

  /**
   * Constructs the SelectableRangeSet to a full range (a range that encompases
   * all values).
   */
  public SelectableRangeSet() {
    range_set = new ArrayList();
    range_set.add(SelectableRange.FULL_RANGE);
  }

  /**
   * Intersects the given SelectableRange object with the given Operator and
   * value constraint.
   * <p>
   * NOTE: This does not work with the '<>' operator which must be handled
   *   another way.
   */
  private static SelectableRange intersectRange(SelectableRange range,
                                                Operator op, DataCell val) {
    Object start = range.getStart();
    byte start_flag = range.getStartFlag();
    Object end = range.getEnd();
    byte end_flag = range.getEndFlag();

    boolean inclusive = op.is("=") || op.is(">=") || op.is("<=");

    if (op.is("=") || op.is(">") || op.is(">=")) {
      if (start == SelectableRange.FIRST_IN_SET) {
        start = val;
        start_flag = inclusive ? SelectableRange.FIRST_VALUE :
                                 SelectableRange.AFTER_LAST_VALUE;
      }
      else {
        int c = val.compareTo((DataCell) start);
        if ((c == 0 && start_flag == SelectableRange.FIRST_VALUE) || c > 0) {
          start = val;
          start_flag = inclusive ? SelectableRange.FIRST_VALUE :
                                   SelectableRange.AFTER_LAST_VALUE;
        }
      }
    }
    if (op.is("=") || op.is("<") || op.is("<=")) {
      if (end == SelectableRange.LAST_IN_SET) {
        end = val;
        end_flag = inclusive ? SelectableRange.LAST_VALUE :
                               SelectableRange.BEFORE_FIRST_VALUE;
      }
      else {
        int c = val.compareTo((DataCell) end);
        if ((c == 0 && end_flag == SelectableRange.LAST_VALUE) || c < 0) {
          end = val;
          end_flag = inclusive ? SelectableRange.LAST_VALUE :
                                 SelectableRange.BEFORE_FIRST_VALUE;
        }
      }
    }

    if (start instanceof DataCell && end instanceof DataCell) {
      int c = ((DataCell) start).compareTo((DataCell) end);
      // If start is higher than end, return null
      if ((c == 0 && (start_flag == SelectableRange.AFTER_LAST_VALUE ||
                      end_flag == SelectableRange.BEFORE_FIRST_VALUE)) ||
          c > 0) {
        return null;
      }
    }

    // The new intersected range
    return new SelectableRange(start_flag, start, end_flag, end);
  }

  /**
   * Returns true if the two SelectableRange ranges intersect.
   */
  private static boolean rangeIntersectedBy(SelectableRange range1,
                                            SelectableRange range2) {
    byte start_flag_1 = range1.getStartFlag();
    Object start_1 = range1.getStart();
    byte end_flag_1 = range1.getEndFlag();
    Object end_1 = range1.getEnd();

    byte start_flag_2 = range2.getStartFlag();
    Object start_2 = range2.getStart();
    byte end_flag_2 = range2.getEndFlag();
    Object end_2 = range2.getEnd();

    DataCell start_cell_1, end_cell_1;
    DataCell start_cell_2, end_cell_2;

    start_cell_1 = start_1 instanceof DataCell ? (DataCell) start_1 : null;
    end_cell_1 = end_1 instanceof DataCell ? (DataCell) end_1 : null;
    start_cell_2 = start_2 instanceof DataCell ? (DataCell) start_2 : null;
    end_cell_2 = end_2 instanceof DataCell ? (DataCell) end_2 : null;

//    if (start_1 == SelectableRange.FIRST_IN_SET) {
//      if (end_1 == SelectableRange.LAST_IN_SET) {
//        return true;
//      }
//      else {
//        start_cell_1 = null;
//        end_cell_1 = (DataCell) end_1;
//      }
//    }
//    else {
//      start_cell_1 = (DataCell) start_1;
//      if (end_1 != SelectableRange.LAST_IN_SET) {
//        end_cell_1 = (DataCell) end_1;
//      }
//      else {
//        end_cell_1 = null;
//      }
//    }
//
//    if (start_2 == SelectableRange.FIRST_IN_SET) {
//      if (end_2 == SelectableRange.LAST_IN_SET) {
//        return true;
//      }
//      else {
//        start_cell_2 = null;
//        end_cell_2 = (DataCell) end_2;
//      }
//    }
//    else {
//      start_cell_2 = (DataCell) start_2;
//      if (end_2 != SelectableRange.LAST_IN_SET) {
//        end_cell_2 = (DataCell) end_2;
//      }
//      else {
//        end_cell_2 = null;
//      }
//    }

    boolean intersect_1 = false;
    if (start_cell_1 != null && end_cell_2 != null) {
      int c = start_cell_1.compareTo(end_cell_2);
      if (c < 0 ||
          (c == 0 && (start_flag_1 == SelectableRange.FIRST_VALUE ||
                      end_flag_2 == SelectableRange.LAST_VALUE))) {
        intersect_1 = true;
      }
    }
    else {
      intersect_1 = true;
    }

    boolean intersect_2 = false;
    if (start_cell_2 != null && end_cell_1 != null) {
      int c = start_cell_2.compareTo(end_cell_1);
      if (c < 0 ||
          (c == 0 && (start_flag_2 == SelectableRange.FIRST_VALUE ||
                      end_flag_1 == SelectableRange.LAST_VALUE))) {
        intersect_2 = true;
      }
    }
    else {
      intersect_2 = true;
    }

    return (intersect_1 && intersect_2);
  }

  /**
   * Alters the first range so it encompasses the second range.  This assumes
   * that range1 intersects range2.
   */
  private static SelectableRange changeRangeSizeToEncompass(
                            SelectableRange range1, SelectableRange range2) {

    byte start_flag_1 = range1.getStartFlag();
    Object start_1 = range1.getStart();
    byte end_flag_1 = range1.getEndFlag();
    Object end_1 = range1.getEnd();

    byte start_flag_2 = range2.getStartFlag();
    Object start_2 = range2.getStart();
    byte end_flag_2 = range2.getEndFlag();
    Object end_2 = range2.getEnd();

    if (start_1 != SelectableRange.FIRST_IN_SET) {
      if (start_2 != SelectableRange.FIRST_IN_SET) {
        DataCell cell = (DataCell) start_1;
        int c = cell.compareTo((DataCell) start_2);
        if (c > 0 ||
            c == 0 && start_flag_1 == SelectableRange.AFTER_LAST_VALUE &&
                      start_flag_2 == SelectableRange.FIRST_VALUE) {
          start_1 = start_2;
          start_flag_1 = start_flag_2;
        }
      }
      else {
        start_1 = start_2;
        start_flag_1 = start_flag_2;
      }
    }

    if (end_1 != SelectableRange.LAST_IN_SET) {
      if (end_2 != SelectableRange.LAST_IN_SET) {
        DataCell cell = (DataCell) end_1;
        int c = cell.compareTo((DataCell) end_2);
        if (c < 0 ||
            c == 0 && end_flag_1 == SelectableRange.BEFORE_FIRST_VALUE &&
                      end_flag_2 == SelectableRange.LAST_VALUE) {
          end_1 = end_2;
          end_flag_1 = end_flag_2;
        }
      }
      else {
        end_1 = end_2;
        end_flag_1 = end_flag_2;
      }
    }

    return new SelectableRange(start_flag_1, start_1, end_flag_1, end_1);
  }

  /**
   * Intersects this range with the given Operator and value constraint.
   * For example, if a range is 'a' -> [END] and the given operator is '<=' and
   * the value is 'z' the result range is 'a' -> 'z'.
   */
  public void intersect(Operator op, DataCell val) {
    int sz = range_set.size();
    ListIterator i = range_set.listIterator();

    if (op.is("<>")) {

      while (i.hasNext()) {
        SelectableRange range = (SelectableRange) i.next();
        SelectableRange left_range =
                               intersectRange(range, Operator.get("<"), val);
        SelectableRange right_range =
                               intersectRange(range, Operator.get(">"), val);
        i.remove();
        if (left_range != null) {
          i.add(left_range);
        }
        if (right_range != null) {
          i.add(right_range);
        }
      }

    }
    else {

      while (i.hasNext()) {
        SelectableRange range = (SelectableRange) i.next();
        range = intersectRange(range, op, val);
        if (range == null) {
          i.remove();
        }
        else {
          i.set(range);
        }
      }

    }

  }

  /**
   * Unions this range with the given Operator and value constraint.
   */
  public void union(Operator op, DataCell val) {
    throw new Error("PENDING");
  }

  /**
   * Unions the current range set with the given range set.
   */
  public void union(SelectableRangeSet union_to) {
    ArrayList input_set = union_to.range_set;

    int in_sz = input_set.size();
    for (int n = 0; n < in_sz; ++n) {
      // The range to merge in.
      SelectableRange in_range = (SelectableRange) input_set.get(n);

      // For each range in this set
      int sz = range_set.size();
      ListIterator i = range_set.listIterator();
      while (i.hasNext()) {
        SelectableRange range = (SelectableRange) i.next();
        if (rangeIntersectedBy(in_range, range)) {
          i.remove();
          in_range = changeRangeSizeToEncompass(in_range, range);
        }
      }

      // Insert into sorted position
      byte start_flag = in_range.getStartFlag();
      Object start = in_range.getStart();
      byte end_flag = in_range.getEndFlag();
      Object end = in_range.getEnd();

      if (start == SelectableRange.FIRST_IN_SET) {
        range_set.add(0, in_range);
      }
      else {
        DataCell start_cell = (DataCell) start;
        i = range_set.listIterator();
        while (i.hasNext()) {
          SelectableRange range = (SelectableRange) i.next();
          Object cur_start = range.getStart();
          if (cur_start instanceof DataCell &&
              ((DataCell) cur_start).compareTo(start_cell) > 0) {
            i.previous();
            break;
          }
        }
        i.add(in_range);
      }

    }

  }

  /**
   * Returns the range as an array of SelectableRange or an empty array if
   * there is no range.
   */
  public SelectableRange[] toSelectableRangeArray() {
    int sz = range_set.size();
    SelectableRange[] ranges = new SelectableRange[sz];
    for (int i = 0; i < sz; ++i) {
      ranges[i] = (SelectableRange) range_set.get(i);
    }
    return ranges;
  }



  /**
   * Outputs this range as a string, for diagnostic and testing purposes.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    if (range_set.size() == 0) {
      return "(NO RANGE)";
    }
    for (int i = 0; i < range_set.size(); ++i) {
      buf.append(range_set.get(i));
      buf.append(", ");
    }
    return new String(buf);
  }



  /**
   * A test application.
   */
  public static void main(String[] args) {

    SelectableRangeSet range_set = new SelectableRangeSet();
    System.out.println(range_set);
    range_set.intersect(Operator.get(">="), new StringDataCell(10, "f"));
    System.out.println(range_set);
    range_set.intersect(Operator.get("<>"), new StringDataCell(10, "i"));
    System.out.println(range_set);
    range_set.intersect(Operator.get("<>"), new StringDataCell(10, "f"));
    System.out.println(range_set);
    range_set.intersect(Operator.get("<>"), new StringDataCell(10, "g"));
    System.out.println(range_set);
    range_set.intersect(Operator.get("<>"), new StringDataCell(10, "f"));
    System.out.println(range_set);
    range_set.intersect(Operator.get("<>"), new StringDataCell(10, "e"));
    System.out.println(range_set);
    range_set.intersect(Operator.get(">="), new StringDataCell(10, "g"));
    System.out.println(range_set);
    range_set.intersect(Operator.get("<="), new StringDataCell(10, "x"));
    System.out.println(range_set);
    range_set.intersect(Operator.get("<"), new StringDataCell(10, "x"));
    System.out.println(range_set);
    range_set.intersect(Operator.get(">="), new StringDataCell(10, "z"));
    System.out.println(range_set);

    System.out.println("---");
    SelectableRangeSet range1 = new SelectableRangeSet();
    range1.intersect(Operator.get("="), new StringDataCell(10, "k"));
    SelectableRangeSet range2 = new SelectableRangeSet();
    range2.intersect(Operator.get("<>"), new StringDataCell(10, "d"));
    range2.intersect(Operator.get("<"), new StringDataCell(10, "g"));
    SelectableRangeSet range3 = new SelectableRangeSet();
    range3.intersect(Operator.get(">"), new StringDataCell(10, "o"));
    range2.union(range3);
    range1.union(range2);
    System.out.println(range1);


  }


}
