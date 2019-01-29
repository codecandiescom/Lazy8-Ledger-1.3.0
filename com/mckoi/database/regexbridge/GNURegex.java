/**
 * com.mckoi.database.regexbridge.GNURegex  14 Oct 2000
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

package com.mckoi.database.regexbridge;

import com.mckoi.database.*;
import com.mckoi.util.IntegerVector;
import gnu.regexp.*;

/**
 * A bridge to the GNU Java regular expression library.  This library is
 * released under the LGPL license which is fully compatible with the GPL
 * license but may be incompatible with other licenses.
 *
 * @author Tobias Downer
 */

public class GNURegex implements com.mckoi.database.RegexLibrary {

  public boolean regexMatch(String regular_expression, String expression_ops,
                            String value) {
    try {
      // PENDING: Compile and cache most commonly used regular expressions...

      int c_flags = 0;
      if (expression_ops != null) {
        if (expression_ops.indexOf('i') != -1) {
          c_flags += RE.REG_ICASE;
        }
        if (expression_ops.indexOf('s') != -1) {
          c_flags += RE.REG_DOT_NEWLINE;
        }
        if (expression_ops.indexOf('m') != -1) {
          c_flags += RE.REG_MULTILINE;
        }
      }

      RE re = new RE(regular_expression, c_flags);
      return re.isMatch(value);
    }
    catch (REException e) {
      // Incorrect syntax means we always match to false,
      return false;
    }
  }

  public IntegerVector regexSearch(Table table, int column,
                           String regular_expression, String expression_ops) {
    // Get the ordered column,
    IntegerVector row_list = table.selectAll(column);
    // The result matched rows,
    IntegerVector result_list = new IntegerVector();

    // Make into a new list that matches the pattern,
    RE re;
    try {
      // PENDING: Compile and cache most commonly used regular expressions...

      int c_flags = 0;
      if (expression_ops != null) {
        if (expression_ops.indexOf('i') != -1) {
          c_flags += RE.REG_ICASE;
        }
        if (expression_ops.indexOf('s') != -1) {
          c_flags += RE.REG_DOT_NEWLINE;
        }
        if (expression_ops.indexOf('m') != -1) {
          c_flags += RE.REG_MULTILINE;
        }
      }

      re = new RE(regular_expression, c_flags);
    }
    catch (REException e) {
      // Incorrect syntax means we always match to an empty list,
      return result_list;
    }

    // For each row in the column, test it against the regular expression.
    int size = row_list.size();
    for (int i = 0; i < size; ++i) {
      int row_index = row_list.intAt(i);
      DataCell cell = table.getCellContents(column, row_index);
      if (!cell.isNull()) {
        Object ob = cell.getCell();
        // Only try and match against non-null cells.
        if (ob != null) {
          String str = ob.toString();
          // If the column matches the regular expression then return it,
          if (re.isMatch(str)) {
            result_list.addInt(row_index);
          }
        }
      }
    }

    return result_list;
  }

}
