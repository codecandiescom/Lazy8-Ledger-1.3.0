/**
 * com.mckoi.database.parser.ParseException  09 Apr 1998
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

/**
 * This may be thrown when the parser encounters an error in the syntax of
 * some expression.
 * <p>
 * @author Tobias Downer
 */

public final class ParseException extends Exception {

  private final int line_number;

  public ParseException(String message, int line_number) {
    super(message);
    this.line_number = line_number;
  }

  public ParseException(String message) {
    super(message);
    this.line_number = -1;
  }

  public ParseException(int line_number) {
    super();
    this.line_number = line_number;
  }

  public int getLineNumber() {
    return line_number;
  }

}
