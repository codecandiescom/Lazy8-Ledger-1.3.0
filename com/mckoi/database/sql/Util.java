/**
 * com.mckoi.database.sql.Util  08 Jul 2000
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

package com.mckoi.database.sql;

import com.mckoi.database.Expression;
import com.mckoi.database.Operator;
import com.mckoi.database.FunctionDef;
import com.mckoi.database.Variable;
import com.mckoi.database.TableName;
import com.mckoi.database.global.*;
import java.math.BigDecimal;

/**
 * Various utility methods for the iterpreter.
 *
 * @author Tobias Downer
 */

public class Util {

  /**
   * Converts an SQL String (eg. 'test') to a ValueSubstitution DB_STRING
   * object.
   */
  public static ValueSubstitution stringToValueSubstitution(String str) {
    if (str.startsWith("\"") && str.endsWith("\"")) {
      String raw_string = str.substring(1, str.length() - 1);
      return new ValueSubstitution(Types.DB_STRING,
                                   escapeTranslated(raw_string));
    }
    throw new Error("String not formatted correctly.");
  }

  /**
   * Returns the Token as a non quoted reference.  For example, a
   * QUOTED_VARIABLE token will have the first and last '"' character
   * removed.  A QUOTED_DELIMINATED_REF will have " removed in each deliminated
   * section.  For example, '"re1"."re2"."a"' becomes 're1.re2.a" and
   * '"re1.re2.a"' becomes 're1.re2.a'.
   */
  public static String asNonQuotedRef(Token token) {
    if (token.kind == SQLConstants.QUOTED_VARIABLE) {
      // Strip " from start and end if a quoted variable
      return token.image.substring(1, token.image.length() - 1);
    }
    else if (token.kind == SQLConstants.QUOTED_DELIMINATED_REF ||
             token.kind == SQLConstants.QUOTEDGLOBVARIABLE) {
      // Remove all " from the string
      String image = token.image;
      StringBuffer b = new StringBuffer();
      int sz = image.length();
      for (int i = 0; i < sz; ++i) {
        char c = image.charAt(i);
        if (c != '\"') {
          b.append(c);
        }
      }
      return new String(b);
    }
    else {
      return token.image;
    }
  }

  /**
   * Converts a Token which is either a STRING_LITERAL, NUMBER_LITERAL or
   * IDENTIFIER into a Java Object.  If 'upper_identifiers' is true then all
   * identifiers are made upper case before being returned (eg. if the
   * object returns is a Variable object).
   */
  public static Object toParamObject(Token token, boolean upper_identifiers) {
    if (token.kind == SQLConstants.STRING_LITERAL) {
      String raw_string = token.image.substring(1, token.image.length() - 1);
      return escapeTranslated(raw_string);
    }
    else if (token.kind == SQLConstants.NUMBER_LITERAL) {
      return new BigDecimal(token.image);
    }
    else if (token.kind == SQLConstants.BOOLEAN_LITERAL) {
      if (token.image.equals("true")) {
        return Boolean.TRUE;
      }
      else {
        return Boolean.FALSE;
      }
    }
    else if (token.kind == SQLConstants.NULL_LITERAL) {
      return NullObject.NULL_OBJ;
    }
    else if (token.kind == SQLConstants.REGEX_LITERAL) {
      // Horrible hack,
      // Get rid of the 'regex' string at the start,
      String str = token.image.substring(5).trim();
      return str;
    }
    else if (token.kind == SQLConstants.QUOTED_VARIABLE ||
             token.kind == SQLConstants.GLOBVARIABLE ||  // eg. Part.*
             token.kind == SQLConstants.IDENTIFIER ||
             token.kind == SQLConstants.DOT_DELIMINATED_REF ||
             token.kind == SQLConstants.QUOTED_DELIMINATED_REF) {
      String name = asNonQuotedRef(token);
//      if (token.kind == SQLConstants.QUOTED_VARIABLE) {
//        name = token.image.substring(1, token.image.length() - 1);
//      }
//      else {
//        name = token.image;
//      }
      if (upper_identifiers) {
        name = name.toUpperCase();
      }
      Variable v;
      int div = name.lastIndexOf(".");
      if (div != -1) {
        // Column represents '[something].[name]'
        // Check if the column name is an alias.
        String column_name = name.substring(div + 1);
        // Make the '[something]' into a TableName
        TableName table_name = TableName.resolve(name.substring(0, div));

        // Set the variable name
        v = new Variable(table_name, column_name);
      }
      else {
        // Column represents '[something]'
        v = new Variable(name);
      }
      return v;
    }
    else {
      throw new Error("Unrecognised parameter type: " + token.kind);
    }
  }

  /**
   * Returns an array of Expression objects as a comma deliminated string.
   */
  public static String expressionListToString(Expression[] list) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < list.length; ++i) {
      buf.append(list[i].text().toString());
      if (i < list.length - 1) {
        buf.append(", ");
      }
    }
    return new String(buf);
  }

  /**
   * Normalizes the Expression by removing all NOT operators and altering
   * the expression as appropriate.  For example, the expression;
   *   not ((a + b) = c and c = 5)
   * would be normalized to;
   *   (a + b) <> c or c <> 5
   */
  public static Expression normalize(final Expression exp) {
    // Only normalize if the expression contains a NOT operator.
    if (exp.containsNotOperator()) {
      return normalize(exp, false);
    }
    return exp;
  }

  /**
   * Normalizes the Expression by removing all NOT operators and altering
   * the expression as appropriate.  For example, the expression;
   *   not ((a + b) = c and c = 5)
   * would be normalized to;
   *   (a + b) <> c or c <> 5
   */
  private static Expression normalize(final Expression exp,
                                     final boolean inverse) {
    if (exp.size() <= 1) {
      if (inverse) {
        return standardInverse(exp);
      }
      else {
        return exp;
      }
    }
    final Operator op = (Operator) exp.last();
    final Expression[] exps = exp.split();

    if (op.isNot()) {
      // If the operator is NOT then return the normalized form of the LHS.
      // We toggle the inverse flag.
      return normalize(exps[0], !inverse);
    }
    else if (op.isNotInversible()) {
      // If the operator is not inversible, return the expression with a
      // '= false' if nothing else is possible
      Expression resolved_expr =
             new Expression(normalize(exps[0], false), op,
                            normalize(exps[1], false));
      if (inverse) {
        return standardInverse(resolved_expr);
      }
      else {
        return resolved_expr;
      }
    }
    else if (op.isLogical()) {
      // If logical we inverse the operator and inverse the left and right
      // side of the operator also.
      if (inverse) {
        return new Expression(normalize(exps[0], inverse), op.inverse(),
                              normalize(exps[1], inverse));
      }
      else {
        return new Expression(normalize(exps[0], inverse), op,
                              normalize(exps[1], inverse));

      }
    }
    else {
      // By this point we can assume the operator is naturally inversible.
      if (inverse) {
        return new Expression(normalize(exps[0], false), op.inverse(),
                              normalize(exps[1], false));
      }
      else {
        return new Expression(normalize(exps[0], false), op,
                              normalize(exps[1], false));
      }
    }

  }

  /**
   * Returns an expression that is (exp) = false which is the natural
   * inverse of all expressions.  This should only be used if the expression
   * can't be inversed in any other way.
   */
  private static Expression standardInverse(Expression exp) {
    return new Expression(exp, Operator.get("="),
                          new Expression(Boolean.FALSE));
  }

  /**
   * Returns a Function object that represents the name and expression list
   * (of parameters) of a function.  Throws an exception if the function
   * doesn't exist.
   */
  public static FunctionDef resolveFunctionName(String name,
                                                Expression[] exp_list) {
    return new FunctionDef(name, exp_list);
  }

  /**
   * Translate a string with escape codes into a un-escaped Java string.  \' is
   * converted to ', \n is a newline, \t is a tab, \\ is \, etc.
   */
  private static String escapeTranslated(String input) {
    StringBuffer result = new StringBuffer();
    int size = input.length();
    boolean last_char_escape = false;
    boolean last_char_quote = false;
    for (int i = 0; i < size; ++i) {
      char c = input.charAt(i);
      if (last_char_quote) {
        last_char_quote = false;
        if (c != '\'') {
          result.append(c);
        }
      }
      else if (last_char_escape) {
        if (c == '\\') {
          result.append('\\');
        }
        else if (c == '\'') {
          result.append('\'');
        }
        else if (c == 't') {
          result.append('\t');
        }
        else if (c == 'n') {
          result.append('\n');
        }
        else if (c == 'r') {
          result.append('\r');
        }
        else {
          result.append('\\');
          result.append(c);
        }
        last_char_escape = false;
      }
      else if (c == '\\') {
        last_char_escape = true;
      }
      else if (c == '\'') {
        last_char_quote = true;
        result.append(c);
      }
      else {
        result.append(c);
      }
    }
    return new String(result);
  }

}
