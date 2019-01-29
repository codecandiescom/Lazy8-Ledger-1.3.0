/**
 * com.mckoi.database.global.Condition  11 May 1998
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

package com.mckoi.database.global;

// This is necessary for the static parse operations
import com.mckoi.database.parser.DefaultParser;
import com.mckoi.database.parser.ParseException;
import com.mckoi.database.parser.ParseState;
import java.io.StreamTokenizer;
import java.io.IOException;

/**
 * Represents a single condition.  A condition contains a variable name, an
 * operator, and either a constant of another variable.  A Condition is used
 * in the 'select' and 'join' methods of a table.
 * <p>
 * @author Tobias Downer
 */

public final class Condition implements java.io.Serializable, Cloneable {

  static final long serialVersionUID = 7602127449759585052L;

  /**
   * Statics representing the different operators.
   */
  public final static int EQUALS                 = 0;
  public final static int GREATER_THAN           = 1;
  public final static int GREATER_THAN_OR_EQUALS = 2;
  public final static int LESS_THAN              = 3;
  public final static int LESS_THAN_OR_EQUALS    = 4;
  public final static int NOT_EQUALS             = 5;
  public final static int IN_BETWEEN             = 6;
  public final static int NOT_IN_BETWEEN         = 7;
  public final static int IN                     = 8;
  public final static int NOT_IN                 = 9;
  public final static int LIKE                   = 10;
  public final static int NOT_LIKE               = 11;

  /**
   * The left hand side variable name.  This is used to refer to a field in a
   * given table.  eg. 'CUSTOMER.CUSTOMERID'
   */
  private String lhs_variable;

  /**
   * The operator for the condition.
   */
  private int operator;

  /**
   * This is set to true if the right hand side is a constant.
   */
  private boolean constant;

  /**
   * The right hand side variable name.  This may only be a variable.
   */
  private String rhs_variable;

  /**
   * The right hand side constant value.  This may only be a constant.
   */
  private ValueSubstitution rhs_constant;

  /**
   * The second right hand side variable name.  This is a hack to provide for
   * the 'in between' clause.
   */
  private ValueSubstitution rhs_constant2;

  /**
   * The Constructor for a condition.  This constructor defines a variable
   * lhs and rhs.  This means this is not a 'constant condition', and will
   * likely be more difficult to perform the query.
   */
  public Condition(String lhs, int op, String rhs) {
    constant = false;
    lhs_variable = lhs;
    operator = op;
    rhs_variable = rhs;
  }

  /**
   * The Constructor for a 'constant condition'.  That is, a condition that
   * has a constant value on the rhs.
   */
  public Condition(String lhs, int op, ValueSubstitution rhs) {
    constant = true;
    lhs_variable = lhs;
    operator = op;
    rhs_constant = rhs;
  }

  /**
   * Hack constructor for a condition that represents the 'in between' set.
   * This is the set between some minimum and maximum.
   */
  public Condition(String lhs, int op, ValueSubstitution rhs1, ValueSubstitution rhs2) {
    this(lhs, op, rhs1);
    rhs_constant2 = rhs2;
  }

  /**
   * Constructs a Condition that is parsed from the given String argument.
   * For example,
   *   [lhs_part]: Part.name LIKE
   *   [val_part]: "PART-1%"
   */
  public Condition(String lhs_part, ValueSubstitution val_part)
                                           throws ParseException, IOException {
    StreamTokenizer t = DefaultParser.createStreamTokenizer(lhs_part);
    // Parse the lhs
    int val = t.nextToken();
    if (val == StreamTokenizer.TT_WORD) {
      String lhs = t.sval;

      // Parse the operator.
      int op = DefaultParser.parseRelational(t, EMPTY_PARSE_STATE);

      // Set up the Condition variables.
      constant = true;
      lhs_variable = lhs;
      operator = op;
      rhs_constant = val_part;
    }
    else {
      throw new ParseException("Couldn't parse condition");
    }
  }

  /**
   * Constructs a Condition that is parsed from the given String argument and
   * Object rhs.  For example,
   *   [lhs_part]: Part.name LIKE
   *   [ob_part]: new String("PART-1%")
   */
  public Condition(String lhs_part, Object ob)
                                          throws ParseException, IOException {
    this(lhs_part, ValueSubstitution.fromObject(ob));
  }

  /**
   * Constructs a Condition expression that is parsed from the given String
   * argument.
   * For example,
   *   Part.name LIKE "PART-1%"
   */
  public Condition(String expression) throws ParseException, IOException {
    // HACK:
    // Lazy implementation, we parse then copy the created conditional into
    // the fields of this new object.
    Condition c = parseCompleteExpression(expression);
    constant = c.constant;
    lhs_variable = c.lhs_variable;
    operator = c.operator;
    rhs_variable = c.rhs_variable;
    rhs_constant = c.rhs_constant;
    rhs_constant2 = c.rhs_constant2;
  }

  /**
   * Returns the left hand side of the condition.
   */
  public String getLHSVariable() {
    return lhs_variable;
  }

  /**
   * Returns the right hand side of the condition.
   */
  public String getRHSVariable() {
    return rhs_variable;
  }

  /**
   * Returns the right has side constant part of the condition.
   */
  public ValueSubstitution getRHSConstant() {
    return rhs_constant;
  }

  /**
   * Returns the second right hand side of the condition for an 'in between'
   * clause.  (Hack)
   */
  public ValueSubstitution getRHS2Constant() {
    return rhs_constant2;
  }

  /**
   * Returns the operation.
   */
  public int getOperator() {
    return operator;
  }

  /**
   * Swaps the left has side and right hand side of the condition.  It also
   * reverses the operator so '>' becomes '<' and '<' becomes '>' etc.  Note
   * that '==' and '!=' stay the same.
   * This method will only work on a not constant condition.
   */
  public Condition doReverse() {
    if (constant) {
      throw new RuntimeException("Can't reverse a constant condition.");
    }

    try {
      Condition c = (Condition) clone();

      c.lhs_variable = rhs_variable;
      c.rhs_variable = lhs_variable;
      switch (operator) {
        case GREATER_THAN:
          c.operator = LESS_THAN;
          break;
        case GREATER_THAN_OR_EQUALS:
          c.operator = LESS_THAN_OR_EQUALS;
          break;
        case LESS_THAN:
          c.operator = GREATER_THAN;
          break;
        case LESS_THAN_OR_EQUALS:
          c.operator = GREATER_THAN_OR_EQUALS;
          break;
      }

      return c;
    }
    catch (CloneNotSupportedException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Reverses the operator.  eg. >= becomes <, > becomes <=.
   */
  public static int reverseOperator(int operator) {
    switch (operator) {
      case GREATER_THAN:
        return LESS_THAN;
      case GREATER_THAN_OR_EQUALS:
        return LESS_THAN_OR_EQUALS;
      case LESS_THAN:
        return GREATER_THAN;
      case LESS_THAN_OR_EQUALS:
        return GREATER_THAN_OR_EQUALS;
      default:
        return operator;
    }
  }

  /**
   * Performs a logical NOT on the operator code.  This will reverse any
   * relation.  eg. >= becomes <, > becomes <=.
   */
  public static int notOperator(int operator) {
    switch (operator) {
      case EQUALS:
        return NOT_EQUALS;
      case NOT_EQUALS:
        return EQUALS;
      case GREATER_THAN:
        return LESS_THAN_OR_EQUALS;
      case LESS_THAN:
        return GREATER_THAN_OR_EQUALS;
      case GREATER_THAN_OR_EQUALS:
        return LESS_THAN;
      case LESS_THAN_OR_EQUALS:
        return GREATER_THAN;
      case IN_BETWEEN:
        return NOT_IN_BETWEEN;
      case NOT_IN_BETWEEN:
        return IN_BETWEEN;
      case IN:
        return NOT_IN;
      case NOT_IN:
        return IN;
      case LIKE:
        return NOT_LIKE;
      case NOT_LIKE:
        return LIKE;
      default:
        throw new RuntimeException("Invalid operator given");
    }
  }

  /**
   * Determines if the right hand side is a variable.  Returns true
   * if variable.
   */
  public boolean isVariable() {
    return !constant;
  }

  /**
   * Determines if the right hand side is a constant.  Returns true if
   * constant.
   */
  public boolean isConstant() {
    return constant;
  }

  /**
   * Determins if the condition is a LIKE type pattern search.  Return true if
   * it is.
   */
  public boolean isLikePatternSearch() {
    return (operator == LIKE || operator == NOT_LIKE);
  }

  /**
   * Returns a new Condition object that is a copy of this Condition only
   * the lhs column is a new name.
   */
  public Condition newLHS(String new_lhs) {
    try {
      Condition c = (Condition) clone();
      c.lhs_variable = new_lhs;
      return c;
    }
    catch (CloneNotSupportedException e) {
      return null;
    }
  }

  /**
   * Returns a new Condition object that is a copy of this Condition only
   * the lhs and rhs column is new.
   */
  public Condition newVars(String new_lhs, String new_rhs) {
    try {
      Condition c = (Condition) clone();
      c.lhs_variable = new_lhs;
      c.rhs_variable = new_rhs;
      return c;
    }
    catch (CloneNotSupportedException e) {
      return null;
    }
  }


  /**
   * Returns a string that represents the given operator enumeration.
   */
  public static String operatorString(int o) {
    switch (o) {
      case EQUALS:
        return "==";
      case GREATER_THAN:
        return ">";
      case GREATER_THAN_OR_EQUALS:
        return ">=";
      case LESS_THAN:
        return "<";
      case LESS_THAN_OR_EQUALS:
        return "<=";
      case NOT_EQUALS:
        return "<>";
      case IN_BETWEEN:
        return "IN BETWEEN";
      case NOT_IN_BETWEEN:
        return "NOT IN BETWEEN";
      case IN:
        return "IN";
      case NOT_IN:
        return "NOT IN";
      case LIKE:
        return "LIKE";
      case NOT_LIKE:
        return "NOT LIKE";
      default:
        return "<UNKNOWN>";
    }
  }

  /**
   * Returns an that represents the given operator string.
   */
  public static int operatorEnum(String str) {
    if (str.equals("==") || str.equals("=")) {
      return EQUALS;
    }
    else if (str.equals(">")) {
      return GREATER_THAN;
    }
    else if (str.equals(">=")) {
      return GREATER_THAN_OR_EQUALS;
    }
    else if (str.equals("<")) {
      return LESS_THAN;
    }
    else if (str.equals("<=")) {
      return LESS_THAN_OR_EQUALS;
    }
    else if (str.equals("<>") || str.equals("!=")) {
      return NOT_EQUALS;
    }
    else if (str.equalsIgnoreCase("like")) {
      return LIKE;
    }
    else if (str.equalsIgnoreCase("not like")) {
      return NOT_LIKE;
    }
    else if (str.equalsIgnoreCase("in")) {
      return IN;
    }
    else if (str.equalsIgnoreCase("not in")) {
      return NOT_IN;
    }
    else {
      throw new Error("Unknown conditional operator.");
    }
  }




  /**
   * Returns a String that represents this Condition.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append(lhs_variable);
    buf.append(' ');
    buf.append(operatorString(operator));
    buf.append(' ');
    if (isConstant()) {
      Object rhs_ob = rhs_constant.getObject();
      if (rhs_ob != null) {
        buf.append(rhs_ob.toString());
      }
      else {
        buf.append("null");
      }
    }
    else {
      buf.append(rhs_variable);
    }
    return new String(buf);
  }


  private static final ValueSubstitution NULL_VS =
                                new ValueSubstitution(Types.DB_UNKNOWN, null);

  /**
   * Returns a Condition that finds all 'null' values in a column.
   */
  public static Condition isNullCondition(String column) {
    return new Condition(column, EQUALS, NULL_VS);
  }

  /**
   * Returns a Condition that finds all non 'null' values in a column.
   */
  public static Condition isNotNullCondition(String column) {
    return new Condition(column, NOT_EQUALS, NULL_VS);
  }


  /**
   * Parses a string and returns a Condition.  For example, the following
   * string:
   *   "Part.number LIKE \"PART-1\""
   * would be parsed into:
   *   new Condition("Part.number", Condition.LIKE, "PART-1");
   */
  public static Condition parseCompleteExpression(String str)
                                           throws IOException, ParseException {
    StreamTokenizer t = DefaultParser.createStreamTokenizer(str);
    return DefaultParser.parseCondition(t, EMPTY_PARSE_STATE);
  }

  /**
   * Parse the LHS part of a conditional, and makes up the rest of the
   * condition with the given argument.  For example:
   *   "Part.number LIKE", "PART-1"
   */
  public static Condition parseExpression(String lhs_part, ValueSubstitution rhs)
                                           throws IOException, ParseException {
    StreamTokenizer t = DefaultParser.createStreamTokenizer(lhs_part);
    // Parse the lhs
    int val = t.nextToken();
    if (val == StreamTokenizer.TT_WORD) {
      String lhs = t.sval;

      // Parse the operator.
      int op = DefaultParser.parseRelational(t, EMPTY_PARSE_STATE);

      // Make it into a Condition and return
      return new Condition(lhs, op, rhs);
    }
    else {
      throw new ParseException("Couldn't parse condition");
    }
  }


  private static final ParseState EMPTY_PARSE_STATE =
                                      new ParseState(new ValueSubstitution[0]);



}
