/**
 * com.mckoi.database.parser.DefaultParser  04 May 1998
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

import com.mckoi.database.global.Condition;
import com.mckoi.database.global.Types;
import com.mckoi.database.global.AggregateFunction;
import com.mckoi.database.global.ValueSubstitution;
import com.mckoi.database.global.ColumnDescription;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.io.IOException;
import java.util.Vector;
import java.math.BigDecimal;

/**
 * This is a static class that is a helper for a subclass object such as
 * DBQueryParser.  It provides serveral primitive parse methods for parsing
 * Strings, lists, conditions, etc.
 * <p>
 * @author Tobias Downer
 */

public class DefaultParser implements Types {

  /**
   * Parses a relational operation code ('>', '<', '>=', '<=', '==', '=', '<>',
   * '!='), and returns the Condition operation.  It also will also parse 'in',
   * 'between', 'not in', 'not between', 'like' and 'not like'.
   * ISSUE: It will parse silly but logically correct conditions such as
   *   'NOT ==', 'NOT <>' or 'NOT NOT NOT NOT LIKE'
   */
  public static int parseRelational(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {

    int val = t.nextToken();
    if (val == StreamTokenizer.TT_WORD) {

      String word = t.sval;
      if (word.equalsIgnoreCase("not")) {
        return Condition.notOperator(parseRelational(t, state));
      }
      else if (word.equalsIgnoreCase("between")) {
        return Condition.IN_BETWEEN;
      }
      else if (word.equalsIgnoreCase("in")) {
        return Condition.IN;
      }
      else if (word.equalsIgnoreCase("like")) {
        return Condition.LIKE;
      }
      else {
        throw new ParseException("Syntax error in condition", t.lineno());
      }

    }
    else if (val == '>' || val == '<') {
      int val2 = t.nextToken();
      if (val2 == '=') {
        if (val == '>') {
          return Condition.GREATER_THAN_OR_EQUALS;
        }
        else {
          return Condition.LESS_THAN_OR_EQUALS;
        }
      }
      else if (val2 == '>') {
        if (val == '<') {
          return Condition.NOT_EQUALS;
        }
        throw new ParseException("Syntax error in condition", t.lineno());
      }
      else {
        t.pushBack();
        if (val == '>') {
          return Condition.GREATER_THAN;
        }
        else {
          return Condition.LESS_THAN;
        }
      }
    }
    else if (val == '=') {
      val = t.nextToken();
      if (val != '=') {
        t.pushBack();
      }
      return Condition.EQUALS;
    }
    else if (val == '!') {
      val = t.nextToken();
      if (val != '=') {
        throw new ParseException("Syntax error in condition", t.lineno());
      }
      return Condition.NOT_EQUALS;
    }
    else {
      throw new ParseException("Syntax error in condition", t.lineno());
    }
  }

  /**
   * Returns true if the given string is a reserved word and can be resolved
   * into a ValueSubstitution.
   */
  public static boolean reservedWord(String word) {
    return (word.equals("true") ||
            word.equals("false") ||
            word.equals("null"));
  }

  /**
   * Parses the token and returns a ValueSubstitution object that represents
   * it.  This method is used primarily in determining the rhs of a Condition.
   * The returned value perfectly describes the type of data, and puts in it
   * a containing object.
   * NOTE: A string may be a 'variable substitution' as specified in the
   *   ParseState object.  These are specified by a '%' character.
   * BIG NOTE: This currently can not parse numbers with greater precision than
   *   a double even though the database is capable of storing very precise
   *   numbers.  It also does not parse date/time.
   */
  public static ValueSubstitution parseValue(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {

    int val = t.nextToken();
    if (val == '"' || val == '\'') {
      // If its a string, it's a constant.
      return new ValueSubstitution(DB_STRING, t.sval);
    }
    else if (val == '%') {
      // If its a variable substitution.
      val = t.nextToken();
      if (val == StreamTokenizer.TT_NUMBER) {
        ValueSubstitution[] vss = state.getSubstitutions();
        // ISSUE: Precision will cause errors for very large substitution
        //   indices (double is converted to an int).
        int index = (int) t.nval;
        if (index >= 0 && index < vss.length) {
          return vss[(int) t.nval];
        }
        else {
          throw new ParseException("Variable substitution out of range", t.lineno());
        }
      }
      else {
        throw new ParseException("Expecting '%[number]' for variable substitution", t.lineno());
      }
    }
    else if (val == StreamTokenizer.TT_NUMBER) {
      // BIG NOTE: We are converting a double to a string here.  This means
      //   we may lose precision.  We need a modified StreamTokenizer that can
      //   read numbers into BigDecimal format.
      return new ValueSubstitution(DB_NUMERIC, new BigDecimal(t.nval));
    }
    else if (val == StreamTokenizer.TT_WORD) {
      // Parse either the boolean 'true' or 'false' values, or parse 'null'.
      String str = t.sval;
      if (str.equals("true")) {
        return new ValueSubstitution(DB_BOOLEAN, Boolean.TRUE);
      }
      else if (str.equals("false")) {
        return new ValueSubstitution(DB_BOOLEAN, Boolean.FALSE);
      }
      else if (str.equals("null")) {
        return new ValueSubstitution(DB_UNKNOWN, null);
      }
      else {
        throw new ParseException("Syntax error", t.lineno());
      }
    }
    else {
      throw new ParseException("Syntax error", t.lineno());
    }

  }

  /**
   * Parses a condition and returns a new Condition expression.
   * BIG NOTE: This currently can not parse numbers with greater precision than
   *   a double even though the database is capable of storing very precise
   *   numbers.  It also does not parse date/time.
   */
  public static Condition parseCondition(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {

    // Every condition has a left hand side and a right hand side.  The first
    // word always equals the left hand side of the condition.

    int val = t.nextToken();
    if (val == StreamTokenizer.TT_WORD) {

      String lhs = t.sval;

      // Now we read the op part of the condition.

      int op = parseRelational(t, state);

      // Read in the rhs of the condition.
      // NOTE: Currently does not part between multiple values

      val = t.nextToken();
      if (val == StreamTokenizer.TT_WORD &&
          !(t.sval.equals("true") || t.sval.equals("false") ||
            t.sval.equals("null")) ) {
        // If its a word it must be a column reference
        return new Condition(lhs, op, t.sval);
      }
      else {
        t.pushBack();
        ValueSubstitution vs = parseValue(t, state);
        return new Condition(lhs, op, vs);
      }

    }
    else {
      throw new ParseException("Syntax error in condition", t.lineno());
    }

  }

  /**
   * Parses a value array.  A value array must be surrounded by paranthese
   * and each string is deliminated with a comma.
   * eg. ( %0, 123.44, %1, "Hello World" )
   */
  protected static ValueSubstitution[] parseValueArray(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {

    // This will store each string as it is read in.

    Vector val_store = new Vector();
    int string_count = 0;

    // Read in the string array a string at a time.

    int val = t.nextToken();
    if (val == '(') {
      do {
        val = t.nextToken();
        if (val != ')') {
          t.pushBack();
          ValueSubstitution vs = parseValue(t, state);
          val_store.addElement(vs);

          val = t.nextToken();
          if (val != ')' && val != ',') {
            throw new ParseException("Expecting ')' or ','", t.lineno());
          }

        }
      } while (val != ')');

      ValueSubstitution[] list = new ValueSubstitution[val_store.size()];
      val_store.copyInto(list);

      return list;

    }
    else {
      throw new ParseException("Expecting '('", t.lineno());
    }

  }

  /**
   * Parses a column list and returns an array of strings that represent
   * the column and the order they are at.  For example,
   *   ( ID, Name, Address1, Address2, State )
   */
  protected static String[] parseColumnList(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {

    // This will store each string as it is read in.

    Vector val_store = new Vector();
    int string_count = 0;

    // Read in the string array a string at a time.

    int val = t.nextToken();
    if (val == '(') {
      do {
        val = t.nextToken();
        if (val == StreamTokenizer.TT_WORD) {
          val_store.addElement(t.sval);

          val = t.nextToken();
          if (val != ')' && val != ',') {
            throw new ParseException("Expecting ')' or ','", t.lineno());
          }
        }
        else if (val != ')') {
          throw new ParseException("Expecting column name in list", t.lineno());
        }

      } while (val != ')');

      String[] list = new String[val_store.size()];
      val_store.copyInto(list);

      return list;

    }
    else {
      throw new ParseException("Expecting '('", t.lineno());
    }

  }

  /**
   * HELPER for 'parseDeclareColumnList'.  Parses a single type for a column
   * eg. 'NUMERIC' or 'STRING(100)'.  Return int[0] = type, int[1] = size if
   * a string else -1.
   */
  private static int[] parseColumnType(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {
    int[] info = new int[2];
    info[1] = -1;
    int val = t.nextToken();
    if (val == StreamTokenizer.TT_WORD) {
      String type = t.sval;
      if (type.equals("STRING")) {
        val = t.nextToken();
        if (val != '(') {
          throw new ParseException("Expecting '('", t.lineno());
        }
        val = t.nextToken();
        if (val != StreamTokenizer.TT_NUMBER) {
          throw new ParseException("Expecting number after '('", t.lineno());
        }
        // Presision error from casting double to int may cause string size
        // to be in error for very large string allocations.
        info[0] = Types.DB_STRING;
        info[1] = (int) t.nval;
        val = t.nextToken();
        if (val != ')') {
          throw new ParseException("Expecting ')'", t.lineno());
        }
      }
      else if (type.equals("NUMERIC")) {
        info[0] = Types.DB_NUMERIC;
      }
      else if (type.equals("BOOLEAN")) {
        info[0] = Types.DB_BOOLEAN;
      }
      else if (type.equals("TIME")) {
        info[0] = Types.DB_TIME;
      }
      else if (type.equals("BINARY")) {
        info[0] = Types.DB_BINARY;
      }
      else {
        throw new ParseException("Unknown column type '" + type + "'", t.lineno());
      }

      return info;

    }
    else {
      throw new ParseException("Expecting word for column type", t.lineno());
    }

  }

  /**
   * Parses a list of ColumnDescription objects.  This is used in a 'create
   * table' operation.  The list is of the form:
   *   ( [column description 1], [column description 2], .... )
   * where:
   *   column description =
   *     [name] [type] <[NULL | NOT NULL]> <[UNIQUE | NOT UNIQUE]> <UNIQUE_GROUP(n)>
   */
  protected static ColumnDescription[] parseDeclareColumnList(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {

    // This will store each string as it is read in.

    Vector col_store = new Vector();

    int val = t.nextToken();
    if (val == '(') {

      do {
        val = t.nextToken();
        if (val != ')') {

          // Parse a column description object.

          String col_name;
          int col_type;
          int col_size;
          boolean col_not_null = false;
          boolean col_unique = false;
          int col_unique_group = -1;

          if (val != StreamTokenizer.TT_WORD) {
            throw new ParseException("Expecting a word", t.lineno());
          }
          col_name = t.sval;

          // HACK: return type information in an array.
          int[] info = parseColumnType(t, state);
          col_type = info[0];
          col_size = info[1];

          // Parse 'NOT NULL', 'NULL', 'NOT UNIQUE', 'UNIQUE' and
          //       'UNIQUE_GROUP(n)'
          val = t.nextToken();
          while (val == StreamTokenizer.TT_WORD) {

            String str = t.sval;
            if (str.equals("NOT")) {
              val = t.nextToken();
              if (val == StreamTokenizer.TT_WORD) {
                if (t.sval.equals("NULL")) {
                  col_not_null = true;
                }
                else if (t.sval.equals("UNIQUE")) {
                  col_unique = false;
                }
                else {
                  throw new ParseException("Expecting NULL or UNIQUE after NOT", t.lineno());
                }
              }
              else {
                throw new ParseException("Expecting NULL or UNIQUE after NOT", t.lineno());
              }
            }
            else if (str.equals("NULL")) {
              col_not_null = false;
            }
            else if (str.equals("UNIQUE")) {
              col_unique = true;
            }
            else if (str.equals("UNIQUE_GROUP")) {
              // Parse '(' - [unique_group_number] - ')'
              val = t.nextToken();
              if (val != '(') {
                throw new ParseException("Expecting ( after UNIQUE_GROUP", t.lineno());
              }
              val = t.nextToken();
              if (val != StreamTokenizer.TT_NUMBER) {
                throw new ParseException("Expecting number after UNIQUE_GROUP", t.lineno());
              }
              col_unique_group = (int) t.nval;
              val = t.nextToken();
              if (val != ')') {
                throw new ParseException("Expecting ) after UNIQUE_GROUP", t.lineno());
              }
            }
            else {
              throw new ParseException("Expecting NULL, UNIQUE, UNIQUE_GROUP or NOT", t.lineno());
            }

            val = t.nextToken();
          } // while (val == StreamTokenizer.TT_WORD)

          if (col_unique_group < -1 || col_unique_group > 64) {
            throw new ParseException("Invalid UNIQUE_GROUP value (" + col_unique_group + "), must be a number between -1 and 64", t.lineno());
          }

          ColumnDescription cd = new ColumnDescription(col_name, col_type, col_size, col_not_null);
          if (col_unique) {
            cd.setUnique();
          }
          cd.setUniqueGroup(col_unique_group);
          col_store.addElement(cd);

          // Another column description or end?

          if (val != ')' && val != ',') {
            throw new ParseException("Expecting ')' or ','", t.lineno());
          }

        }
      } while (val != ')');

      ColumnDescription[] list = new ColumnDescription[col_store.size()];
      col_store.copyInto(list);

      return list;

    }
    else {
      throw new ParseException("Expecting '('", t.lineno());
    }

  }

  /**
   * Parses a single aggregate function in a list of functions.  An aggregate
   * function is of the form 'fun(column)'.
   */
  protected static AggregateFunction parseAggregateFunction(StreamTokenizer t, ParseState state)
                                         throws IOException, ParseException {

    int val = t.nextToken();
    if (val == StreamTokenizer.TT_WORD) {
      String fun = t.sval;
      String column;
      val = t.nextToken();
      if (val != '(') {
        throw new ParseException("Expecting '('", t.lineno());
      }
      val = t.nextToken();
      if (val == '*') {
        column = "*";
      }
      else if (val == StreamTokenizer.TT_WORD) {
        column = t.sval;
      }
      else {
        throw new ParseException("Expecting '*' or column name", t.lineno());
      }
      val = t.nextToken();
      if (val != ')') {
        throw new ParseException("Expecting ')'", t.lineno());
      }
      return new AggregateFunction(fun, column);
    }
    else {
      throw new ParseException("Expecting aggregate function", t.lineno());
    }

  }

  /**
   * Returns a StreamTokenizer for the given String.
   */
  public static final StreamTokenizer createStreamTokenizer(String str) {
    StreamTokenizer t = new StreamTokenizer(new StringReader(str));
    t.wordChars('_', '_');
    return t;
  }

}
