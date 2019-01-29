/**
 * com.mckoi.database.parser.ParseState  09 Apr 1998
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

package com.mckoi.database.parser;

import com.mckoi.database.global.ValueSubstitution;

/**
 * Represents the state of a given parse.  This object contains references that
 * provide state information about the database and the query being parsed.
 * <p>
 * @author Tobias Downer
 */

public final class ParseState {

  /**
   * This ValueSubstitution[] array contains the variable substitutions.  When
   * the '%' token is parsed we must substitute in the 'variable' with a value
   * as passed into the higher level part of the query.
   */
  private ValueSubstitution[] substitutions_list;

  /**
   * The constructor.
   */
  public ParseState(ValueSubstitution[] vals) {
    substitutions_list = vals;
  }

  public ParseState() {
    this(new ValueSubstitution[0]);
  }

  /**
   * Returns the ValueSubstitution[] array containing all the substitutions.
   * The substitutions vector contains an object for the value that needs to be
   * substituted in for the given variable number.
   */
  ValueSubstitution[] getSubstitutions() {
    return substitutions_list;
  }

}
