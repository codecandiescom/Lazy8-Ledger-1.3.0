/**
 * com.mckoi.database.SelectableRange  12 Aug 2001
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

/**
 * An object that represents a range of values to select from a list.  A range
 * has a start value, an end value, and whether we should pick inclusive or
 * exclusive of the end value.  The start value may be a concrete value from
 * the set or it may be a flag that represents the start or end of the list.
 * <p>
 * For example, to select the first item from a set the range would be;
 * <pre>
 * RANGE:
 *   start = FIRST_VALUE, first
 *   end   = LAST_VALUE, first
 * </pre>
 * To select the last item from a set the range would be;
 * <pre>
 * RANGE:
 *   start = FIRST_VALUE, last
 *   end   = LAST_VALUE, last
 * </pre>
 * To select the range of values between '10' and '15' then range would be;
 * <pre>
 * RANGE:
 *   start = FIRST_VALUE, '10'
 *   end   = LAST_VALUE, '15'
 * </pre>
 * Note that the the start value may not compare less than the end value.  For
 * example, start can not be 'last' and end can not be 'first'.
 *
 * @author Tobias Downer
 */

public final class SelectableRange {

  // ---------- Statics ----------

  /**
   * An object that represents the first value in the set.
   */
  public static final String FIRST_IN_SET = new String("[FIRST_IN_SET]");

  /**
   * An object that represents the last value in the set.
   */
  public static final String LAST_IN_SET = new String("[LAST_IN_SET]");

  /**
   * Represents the various points in the set on the value to represent the
   * set range.
   */
  public static final byte FIRST_VALUE        = 1,
                           LAST_VALUE         = 2,
                           BEFORE_FIRST_VALUE = 3,
                           AFTER_LAST_VALUE   = 4;

  // ---------- Members ----------

  /**
   * The start of the range to select from the set.
   */
  private Object start;

  /**
   * The end of the range to select from the set.
   */
  private Object end;

  /**
   * Denotes the place for the range to start with respect to the start value.
   * Either FIRST_VALUE or AFTER_LAST_VALUE.
   */
  private byte set_start_flag;

  /**
   * Denotes the place for the range to end with respect to the end value.
   * Either BEFORE_FIRST_VALUE or LAST_VALUE.
   */
  private byte set_end_flag;

  /**
   * Constructs the range.
   */
  public SelectableRange(byte set_start_flag, Object start,
                         byte set_end_flag, Object end) {
    this.start = start;
    this.end = end;
    this.set_start_flag = set_start_flag;
    this.set_end_flag = set_end_flag;
  }

  /**
   * Returns the start of the range.
   * NOTE: This may return FIRST_IN_SET or LAST_IN_SET.
   */
  public Object getStart() {
    return start;
  }

  /**
   * Returns the end of the range.
   * NOTE: This may return FIRST_IN_SET or LAST_IN_SET.
   */
  public Object getEnd() {
    return end;
  }

  /**
   * Returns the place for the range to start (either FIRST_VALUE or
   * AFTER_LAST_VALUE)
   */
  public byte getStartFlag() {
    return set_start_flag;
  }

  /**
   * Returns the place for the range to end (either BEFORE_FIRST_VALUE or
   * LAST VALUE).
   */
  public byte getEndFlag() {
    return set_end_flag;
  }


  /**
   * Outputs this range as a string.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    if (getStartFlag() == FIRST_VALUE) {
      buf.append("FIRST_VALUE ");
    }
    else if (getStartFlag() == AFTER_LAST_VALUE) {
      buf.append("AFTER_LAST_VALUE ");
    }
    buf.append(getStart());
    buf.append(" -> ");
    if (getEndFlag() == LAST_VALUE) {
      buf.append("LAST_VALUE ");
    }
    else if (getEndFlag() == BEFORE_FIRST_VALUE) {
      buf.append("BEFORE_FIRST_VALUE ");
    }
    buf.append(getEnd());
    return new String(buf);
  }

  /**
   * Returns true if this range is equal to the given range.
   */
  public boolean equals(Object ob) {
    if (super.equals(ob)) {
      return true;
    }

    SelectableRange dest_range = (SelectableRange) ob;
    return (getStart().equals(dest_range.getStart()) &&
            getEnd().equals(dest_range.getEnd()) &&
            getStartFlag() == dest_range.getStartFlag() &&
            getEndFlag() == dest_range.getEndFlag());
  }



  // ---------- Statics ----------

  /**
   * The range that represents the entire range.
   */
  public static final SelectableRange FULL_RANGE =
      new SelectableRange(FIRST_VALUE, FIRST_IN_SET, LAST_VALUE, LAST_IN_SET);






}
