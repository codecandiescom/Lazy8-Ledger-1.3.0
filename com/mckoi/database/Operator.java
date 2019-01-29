/**
 * com.mckoi.database.Operator  11 Jul 2000
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

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.ArrayList;
//import com.mckoi.database.sql.SelectStatement;
import com.mckoi.database.global.NullObject;

/**
 * An operator for an expression.
 *
 * @author Tobias Downer
 */

public abstract class Operator implements java.io.Serializable {

  // ---------- Statics ----------

  /**
   * The ANY and ALL enumerator.
   */
  public static final int NONE = 0, ANY = 1, ALL = 2;

  // ---------- Member ----------

  /**
   * A string that represents this operator.
   */
  private String op;

  /**
   * If this is a set operator such as ANY or ALL then this is set with the
   * flag type.
   */
  private int set_type;

  /**
   * The precedence of this operator.
   */
  private int precedence;

  /**
   * Constructs the Operator.
   */
  protected Operator(String op) {
    this(op, 0, NONE);
  }

  protected Operator(String op, int precedence) {
    this(op, precedence, NONE);
  }

  protected Operator(String op, int precedence, int set_type) {
    if (set_type != NONE && set_type != ANY && set_type != ALL) {
      throw new Error("Invalid set_type.");
    }
    this.op = op;
    this.precedence = precedence;
    this.set_type = set_type;
  }


  /**
   * Returns true if this operator is equal to the operator string.
   */
  public boolean is(String given_op) {
    return given_op.equals(op);
  }

  public abstract Object eval(Object ob1, Object ob2,
                              GroupResolver group, VariableResolver resolver,
                              QueryContext context);

  public int precedence() {
    return precedence;
  }

  public boolean isCondition() {
    return (equals(eq_op) ||
            equals(neq_op) ||
            equals(g_op) ||
            equals(l_op) ||
            equals(geq_op) ||
            equals(leq_op));
  }

  public boolean isMathematical() {
    return (equals(add_op) ||
            equals(sub_op) ||
            equals(mul_op) ||
            equals(div_op));
  }

  public boolean isPattern() {
    return (equals(like_op) ||
            equals(nlike_op) ||
            equals(regex_op));
  }


  public boolean isLogical() {
    return (equals(and_op) ||
            equals(or_op));
  }

  public boolean isNot() {
    return equals(not_op);
  }

  public boolean isSubQuery() {
    return (set_type != NONE ||
            equals(in_op) ||
            equals(nin_op));
  }

  /**
   * Returns an Operator that is the reverse of this Operator.  This is used
   * for reversing a conditional expression.  eg. 9 > id becomes id < 9.
   */
  public Operator reverse() {
    if (equals(eq_op) || equals(neq_op)) {
      return this;
    }
    else if (equals(g_op)) {
      return l_op;
    }
    else if (equals(l_op)) {
      return g_op;
    }
    else if (equals(geq_op)) {
      return leq_op;
    }
    else if (equals(leq_op)) {
      return geq_op;
    }
    throw new Error("Can't reverse a non conditional operator.");
  }

  /**
   * Returns true if this operator is not inversible.
   */
  public boolean isNotInversible() {
    // The REGEX op, and mathematical operators are not inversible.
    return equals(regex_op) || isMathematical();
  }

  /**
   * Returns the inverse operator of this operator.  For example, = becomes <>,
   * > becomes <=, AND becomes OR.
   */
  public Operator inverse() {
    if (isSubQuery()) {
      int inv_type;
      if (isSubQueryForm(ANY)) {
        inv_type = ALL;
      }
      else if (isSubQueryForm(ALL)) {
        inv_type = ANY;
      }
      else {
        throw new RuntimeException("Can not handle sub-query form.");
      }

      Operator inv_op = Operator.get(op).inverse();

      return inv_op.getSubQueryForm(inv_type);
    }
    else if (equals(eq_op)) {
      return neq_op;
    }
    else if (equals(neq_op)) {
      return eq_op;
    }
    else if (equals(g_op)) {
      return leq_op;
    }
    else if (equals(l_op)) {
      return geq_op;
    }
    else if (equals(geq_op)) {
      return l_op;
    }
    else if (equals(leq_op)) {
      return g_op;
    }
    else if (equals(and_op)) {
      return or_op;
    }
    else if (equals(or_op)) {
      return and_op;
    }
    else if (equals(like_op)) {
      return nlike_op;
    }
    else if (equals(nlike_op)) {
      return like_op;
    }
    else {
      throw new Error("Can't inverse operator '" + op + "'");
    }

  }

  /**
   * Given a parameter of either NONE, ANY, ALL or SINGLE, this returns true
   * if this operator is of the given type.
   */
  public boolean isSubQueryForm(int type) {
    return type == set_type;
  }

  /**
   * Returns the ANY or ALL form of this operator.
   */
  public Operator getSubQueryForm(int type) {
    Operator result_op = null;
    if (type == ANY) {
      result_op = (Operator) any_map.get(op);
    }
    else if (type == ALL) {
      result_op = (Operator) all_map.get(op);
    }
    else if (type == NONE) {
      result_op = get(op);
    }

    if (result_op == null) {
      throw new Error("Couldn't change the form of operator '" + op + "'.");
    }
    return result_op;
  }

  /**
   * Same as above only it handles the type as a string.
   */
  public Operator getSubQueryForm(String type_str) {
    String s = type_str.toUpperCase();
    if (s.equals("SINGLE") || s.equals("ANY") || s.equals("SOME")) {
      return getSubQueryForm(ANY);
    }
    else if (s.equals("ALL")) {
      return getSubQueryForm(ALL);
    }
    throw new Error("Do not understand subquery type '" + type_str + "'");
  }

  /**
   * The class of object this Operator evaluates to.
   */
  public Class returnClass() {
    if (isMathematical()) {
      return BigDecimal.class;
    }
    else {
      return Boolean.class;
    }
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append(op);
    if (set_type == ANY) {
      buf.append(" ANY");
    }
    else if (set_type == ALL) {
      buf.append(" ALL");
    }
    return new String(buf);
  }

  public boolean equals(Object ob) {
    if (this == ob) return true;
    Operator oob = (Operator) ob;
    return op.equals(oob.op) && set_type == oob.set_type;
  }



  /**
   * Returns an Operator with the given string.
   */
  public static Operator get(String op) {
    if (op.equals("+")) { return add_op; }
    else if (op.equals("-")) { return sub_op; }
    else if (op.equals("*")) { return mul_op; }
    else if (op.equals("/")) { return div_op; }

    else if (op.equals("=") | op.equals("==")) { return eq_op; }
    else if (op.equals("<>") | op.equals("!=")) { return neq_op; }
    else if (op.equals(">")) { return g_op; }
    else if (op.equals("<")) { return l_op; }
    else if (op.equals(">=")) { return geq_op; }
    else if (op.equals("<=")) { return leq_op; }

    else if (op.equals("(")) { return par1_op; }
    else if (op.equals(")")) { return par2_op; }

    // Operators that are words, convert to lower case...
    op = op.toLowerCase();
    if (op.equals("like")) { return like_op; }
    else if (op.equals("not like")) { return nlike_op; }
    else if (op.equals("regex")) { return regex_op; }

//    else if (op.equals("between")) { return between_op; }
//    else if (op.equals("not between")) { return nbetween_op; }

    else if (op.equals("in")) { return in_op; }
    else if (op.equals("not in")) { return nin_op; }

    else if (op.equals("not")) { return not_op; }
    else if (op.equals("and")) { return and_op; }
    else if (op.equals("or")) { return or_op; }


    throw new Error("Unrecognised operator type: " + op);
  }

  // ---------- Convenience methods ----------

  private final static BigDecimal BD_ZERO = new BigDecimal(0);
  private final static BigDecimal BD_ONE  = new BigDecimal(1);

  public static BigDecimal toNumber(Object ob) {
    if (ob instanceof String) {
      try {
        return new BigDecimal((String) ob);
      }
      catch (Throwable e) {
        return BD_ZERO;
      }
    }
    if (ob instanceof Boolean) {
      if (((Boolean) ob).booleanValue() == true) {
        return BD_ONE;
      }
      else {
        return BD_ZERO;
      }
    }
    if (ob instanceof Date) {
      return new BigDecimal(((Date) ob).getTime());
    }
    return (BigDecimal) ob;
  }

  public static Boolean toBoolean(Object ob) {
    if (ob instanceof String) {
      String str = (String) ob;
      if (str.equalsIgnoreCase("true")) {
        return Boolean.TRUE;
      }
      else {
        return Boolean.FALSE;
      }
    }
    else if (ob instanceof BigDecimal) {
      if (((BigDecimal) ob).compareTo(BD_ZERO) == 0) {
        return Boolean.FALSE;
      }
      else {
        return Boolean.TRUE;
      }
    }
    return (Boolean) ob;
  }

  public static boolean toBooleanValue(Object ob) {
    return toBoolean(ob).booleanValue();
  }

  public static String toString(Object ob) {
    return ob.toString();
  }

  public static int compareObs(Object ob1, Object ob2) {
    // Handle null objects
    if (ob1 == null || ob1 instanceof NullObject) {
      return (ob2 instanceof NullObject) ? 0 : -1;
    }
    else if (ob2 == null || ob2 instanceof NullObject) {
      return (ob1 instanceof NullObject) ? 0 : 1;
    }

    // If ob1 is a different class to ob2, then throw an error
    if (ob1.getClass() != ob2.getClass()) {
      throw new RuntimeException(
                "Can't compare " + ob1.getClass() + " and " + ob2.getClass());
    }

    // Comparing boolean
    if (ob1 instanceof Boolean) {
      if (ob1.equals(ob2)) {
        return 0;
      }
      else if (ob1.equals(Boolean.TRUE)) {
        return 1;
      }
      else {
        return -1;
      }
    }
    else if (ob1 instanceof Comparable) {
      return ((Comparable) ob1).compareTo(ob2);
    }
    else if (ob1 instanceof String) {
      return ((String) ob1).compareTo((String) ob2);
    }
    else if (ob1 instanceof BigDecimal) {
      return ((BigDecimal) ob1).compareTo((BigDecimal) ob2);
    }
    else if (ob1 instanceof Date) {
      long t1 = ((Date) ob1).getTime();
      long t2 = ((Date) ob2).getTime();
      if (t1 < t2) {
        return -1;
      }
      else if (t1 > t2) {
        return 1;
      }
      return 0;
    }
    else {
      throw new RuntimeException("Unable to compare types.");
    }
  }


  public static Boolean booleanVal(boolean b) {
    if (b) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }



  // ---------- The different types of operator's we can have ----------

  private final static AddOperator add_op = new AddOperator();
  private final static SubtractOperator sub_op = new SubtractOperator();
  private final static MultiplyOperator mul_op = new MultiplyOperator();
  private final static DivideOperator div_op = new DivideOperator();

  private final static EqualOperator eq_op = new EqualOperator();
  private final static NotEqualOperator neq_op = new NotEqualOperator();
  private final static GreaterOperator g_op = new GreaterOperator();
  private final static LesserOperator l_op = new LesserOperator();
  private final static GreaterEqualOperator geq_op =
                                                   new GreaterEqualOperator();
  private final static LesserEqualOperator leq_op = new LesserEqualOperator();

  private final static PatternMatchTrueOperator like_op =
                                     new PatternMatchTrueOperator();
  private final static PatternMatchFalseOperator nlike_op =
                                    new PatternMatchFalseOperator();
  private final static RegexOperator regex_op = new RegexOperator();

  private final static Operator in_op;
  private final static Operator nin_op;

  private final static Operator not_op = new SimpleOperator("not", 3);

  private final static AndOperator and_op = new AndOperator();
  private final static OrOperator or_op = new OrOperator();

  private final static ParenOperator par1_op = new ParenOperator("(");
  private final static ParenOperator par2_op = new ParenOperator(")");

  // Maps from operator to 'any' operator
  private final static HashMap any_map = new HashMap();
  // Maps from operator to 'all' operator.
  private final static HashMap all_map = new HashMap();

  static {
    // Populate the static ANY and ALL mapping
    any_map.put("=", new AnyOperator("="));
    any_map.put("<>", new AnyOperator("<>"));
    any_map.put(">", new AnyOperator(">"));
    any_map.put(">=", new AnyOperator(">="));
    any_map.put("<", new AnyOperator("<"));
    any_map.put("<=", new AnyOperator("<="));

    all_map.put("=", new AllOperator("="));
    all_map.put("<>", new AllOperator("<>"));
    all_map.put(">", new AllOperator(">"));
    all_map.put(">=", new AllOperator(">="));
    all_map.put("<", new AllOperator("<"));
    all_map.put("<=", new AllOperator("<="));

    // The IN and NOT IN operator are '= ANY' and '<> ALL' respectively.
    in_op = (Operator) any_map.get("=");
    nin_op = (Operator) all_map.get("<>");
  }


  static class AddOperator extends Operator {
    public AddOperator() { super("+", 10); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
        return NullObject.NULL_OBJ;
      }
      return (Operator.toNumber(ob1).add(Operator.toNumber(ob2)));
    }
  };

  static class SubtractOperator extends Operator {
    public SubtractOperator() { super("-", 15); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
        return NullObject.NULL_OBJ;
      }
      return (Operator.toNumber(ob1).subtract(Operator.toNumber(ob2)));
    }
  };

  static class MultiplyOperator extends Operator {
    public MultiplyOperator() { super("*", 20); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
        return NullObject.NULL_OBJ;
      }
      return (Operator.toNumber(ob1).multiply(Operator.toNumber(ob2)));
    }
  };

  static class DivideOperator extends Operator {
    public DivideOperator() { super("/", 20); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
        return NullObject.NULL_OBJ;
      }
      BigDecimal div_by = Operator.toNumber(ob2);
      if (!div_by.equals(BD_ZERO)) {
        return Operator.toNumber(ob1).divide(
                                 div_by, 10, BigDecimal.ROUND_HALF_UP);
      }
      else {
        return NullObject.NULL_OBJ;
      }
    }
  };




  static class EqualOperator extends Operator {
    public EqualOperator() { super("=", 4); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      return Operator.booleanVal(Operator.compareObs(ob1, ob2) == 0);
    }
  }

  static class NotEqualOperator extends Operator {
    public NotEqualOperator() { super("<>", 4); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      return Operator.booleanVal(Operator.compareObs(ob1, ob2) != 0);
    }
  }

  static class GreaterOperator extends Operator {
    public GreaterOperator() { super(">", 4); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      return Operator.booleanVal(Operator.compareObs(ob1, ob2) > 0);
    }
  }

  static class LesserOperator extends Operator {
    public LesserOperator() { super("<", 4); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      return Operator.booleanVal(Operator.compareObs(ob1, ob2) < 0);
    }
  }

  static class GreaterEqualOperator extends Operator {
    public GreaterEqualOperator() { super(">=", 4); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      return Operator.booleanVal(Operator.compareObs(ob1, ob2) >= 0);
    }
  }

  static class LesserEqualOperator extends Operator {
    public LesserEqualOperator() { super("<=", 4); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      return Operator.booleanVal(Operator.compareObs(ob1, ob2) <= 0);
    }
  }


  static class AnyOperator extends Operator {
    public AnyOperator(String op) {
      super(op, 8, ANY);
    }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob2 instanceof QueryPlanNode) {
        // The sub-query plan
        QueryPlanNode plan = (QueryPlanNode) ob2;
        // Discover the correlated variables for this plan.
        ArrayList list = plan.discoverCorrelatedVariables(1, new ArrayList());

        if (list.size() > 0) {
          // Set the correlated variables from the VariableResolver
          for (int i = 0; i < list.size(); ++i) {
            ((CorrelatedVariable) list.get(i)).setFromResolver(resolver);
          }
          // Clear the cache in the context
          context.clearCache();
        }

        // Evaluate the plan,
        Table t = plan.evaluate(context);

        // The ANY operation
        Operator rev_plain_op = getSubQueryForm(NONE).reverse();
        if (t.columnMatchesValue(0, rev_plain_op, ob1)) {
          return Boolean.TRUE;
        }
        return Boolean.FALSE;

//        Operator rev_plain_op = getSubQueryForm(NONE).reverse();
//        Queriable select = (Queriable) ob2;
//        Table t = select.evaluateQuery(null);
//        if (t.columnMatchesValue(0, rev_plain_op, ob1)) {
//          return Boolean.TRUE;
//        }
//        return Boolean.FALSE;
      }
      else if (ob2 instanceof Expression[]) {
        Operator plain_op = getSubQueryForm(NONE);
        Expression[] exp_list = (Expression[]) ob2;
        for (int i = 0; i < exp_list.length; ++i) {
          Object exp_item = exp_list[i].evaluate(group, resolver, context);
          if (plain_op.eval(ob1, exp_item,
                            null, null, null).equals(Boolean.TRUE)) {
            return Boolean.TRUE;
          }
        }
        return Boolean.FALSE;
      }
      else {
        throw new Error("Unknown RHS of ANY.");
      }
    }
  }

  static class AllOperator extends Operator {
    public AllOperator(String op) {
      super(op, 8, ALL);
    }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob2 instanceof QueryPlanNode) {

        // The sub-query plan
        QueryPlanNode plan = (QueryPlanNode) ob2;
        // Discover the correlated variables for this plan.
        ArrayList list = plan.discoverCorrelatedVariables(1, new ArrayList());

        if (list.size() > 0) {
          // Set the correlated variables from the VariableResolver
          for (int i = 0; i < list.size(); ++i) {
            ((CorrelatedVariable) list.get(i)).setFromResolver(resolver);
          }
          // Clear the cache in the context
          context.clearCache();
        }

        // Evaluate the plan,
        Table t = plan.evaluate(context);

        Operator rev_plain_op = getSubQueryForm(NONE).reverse();
        if (t.allColumnMatchesValue(0, rev_plain_op, ob1)) {
          return Boolean.TRUE;
        }
        return Boolean.FALSE;

//        Operator rev_plain_op = getSubQueryForm(NONE).reverse();
//        Queriable select = (Queriable) ob2;
//        Table t = select.evaluateQuery(null);
//        if (t.allColumnMatchesValue(0, rev_plain_op, ob1)) {
//          return Boolean.TRUE;
//        }
//        return Boolean.FALSE;
      }
      else if (ob2 instanceof Expression[]) {
        Operator plain_op = getSubQueryForm(NONE);
        Expression[] exp_list = (Expression[]) ob2;
        for (int i = 0; i < exp_list.length; ++i) {
          Object exp_item = exp_list[i].evaluate(group, resolver, context);
          // If it doesn't match return false
          if (plain_op.eval(ob1, exp_item,
                            null, null, null).equals(Boolean.FALSE)) {
            return Boolean.FALSE;
          }
        }
        // Otherwise return true.
        return Boolean.TRUE;
      }
      else {
        throw new Error("Unknown RHS of ALL.");
      }
    }
  }



//  static class BetweenOperator extends Operator {
//    public BetweenOperator() { super("between", 8); }
//    public Object eval(Object ob1, Object ob2,
//                       GroupResolver group, VariableResolver resolver) {
//      // ob2 must me an array of expression
//      Expression[] between_target = (Expression[]) ob2;
//      Object lower = between_target[0].evaluate(group, resolver);
//      Object upper = between_target[1].evaluate(group, resolver);
//      return Operator.booleanVal( (Operator.compareObs(ob1, lower) >= 0) &&
//                                  (Operator.compareObs(ob1, upper) <= 0) );
//    }
//  }
//
//  static class NotBetweenOperator extends Operator {
//    public NotBetweenOperator() { super("not between", 8); }
//    public Object eval(Object ob1, Object ob2,
//                       GroupResolver group, VariableResolver resolver) {
//      // ob2 must me an array of expression
//      Expression[] between_target = (Expression[]) ob2;
//      Object lower = between_target[0].evaluate(group, resolver);
//      Object upper = between_target[1].evaluate(group, resolver);
//      return Operator.booleanVal( (Operator.compareObs(ob1, lower) < 0) ||
//                                  (Operator.compareObs(ob1, upper) > 0) );
//    }
//  }



  static class RegexOperator extends Operator {
    public RegexOperator() { super("regex", 8); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
        return NullObject.NULL_OBJ;
      }
      String val = ob1.toString();
      String pattern = ob2.toString();
      return new Boolean(PatternSearch.regexMatch(
                                        context.getSystem(), pattern, val));
    }
  }

  static class PatternMatchTrueOperator extends Operator {
    public PatternMatchTrueOperator() { super("like", 8); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
        return NullObject.NULL_OBJ;
      }
      String val = ob1.toString();
      String pattern = ob2.toString();
      return new Boolean(PatternSearch.fullPatternMatch(pattern, val, '\\'));
    }
  }

  static class PatternMatchFalseOperator extends Operator {
    public PatternMatchFalseOperator() { super("not like", 8); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
        return NullObject.NULL_OBJ;
      }
      String val = ob1.toString();
      String pattern = ob2.toString();
      return new Boolean(!PatternSearch.fullPatternMatch(pattern, val, '\\'));
    }
  }

//  static class InOperator extends Operator {
//    public InOperator() { super("in", 8); }
//    public Object eval(Object ob1, Object ob2,
//                       GroupResolver group, VariableResolver resolver,
//                       QueryContext context) {
//      if (ob2 instanceof Queriable) {
//        Queriable select = (Queriable) ob2;
//        Table t = select.evaluateQuery(null);
//        if (t.columnContainsValue(0, ob1)) {
//          return Boolean.TRUE;
//        }
//      }
//      else if (ob2 instanceof Expression[]) {
//        Expression[] exp_list = (Expression[]) ob2;
//        for (int i = 0; i < exp_list.length; ++i) {
//          if (exp_list[i].evaluate(group, resolver, context).equals(ob1)) {
//            return Boolean.TRUE;
//          }
//        }
//      }
//      else {
//        throw new Error("Unknown RHS of 'in'");
//      }
//
//      return Boolean.FALSE;
//    }
//  }
//
//  static class NotInOperator extends Operator {
//    public NotInOperator() { super("not in", 8); }
//    public Object eval(Object ob1, Object ob2,
//                       GroupResolver group, VariableResolver resolver,
//                       QueryContext context) {
//      if (ob2 instanceof Queriable) {
//        Queriable select = (Queriable) ob2;
//        Table t = select.evaluateQuery(null);
//        if (t.columnContainsValue(0, ob1)) {
//          return Boolean.FALSE;
//        }
//      }
//      else if (ob2 instanceof Expression[]) {
//        Expression[] exp_list = (Expression[]) ob2;
//        for (int i = 0; i < exp_list.length; ++i) {
//          if (exp_list[i].evaluate(group, resolver, context).equals(ob1)) {
//            return Boolean.FALSE;
//          }
//        }
//      }
//
//      return Boolean.TRUE;
//    }
//  }

  // and/or have lowest precedence
  static class AndOperator extends Operator {
    public AndOperator() { super("and", 2); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      // If either ob1 or ob2 are null
      if (ob1 instanceof NullObject) {
        if (!(ob2 instanceof NullObject)) {
          Boolean b = Operator.toBoolean(ob2);
          if (b.equals(Boolean.FALSE)) {
            return b;
          }
        }
        return ob1;
      }
      else if (ob2 instanceof NullObject) {
        Boolean b = Operator.toBoolean(ob1);
        if (b.equals(Boolean.FALSE)) {
          return b;
        }
        return ob2;
      }

      // If both true.
      return Operator.booleanVal(
               Operator.toBoolean(ob1).equals(Boolean.TRUE) &&
               Operator.toBoolean(ob2).equals(Boolean.TRUE) );
    }
  }

  static class OrOperator extends Operator {
    public OrOperator() { super("or", 1); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      // If either ob1 or ob2 are null
      if (ob1 instanceof NullObject) {
        if (!(ob2 instanceof NullObject)) {
          Boolean b = Operator.toBoolean(ob2);
          if (b.equals(Boolean.TRUE)) {
            return b;
          }
        }
        return ob1;
      }
      else if (ob2 instanceof NullObject) {
        Boolean b = Operator.toBoolean(ob1);
        if (b.equals(Boolean.TRUE)) {
          return b;
        }
        return ob2;
      }

      // If either true.
      return Operator.booleanVal(
               Operator.toBoolean(ob1).equals(Boolean.TRUE) ||
               Operator.toBoolean(ob2).equals(Boolean.TRUE) );
    }
  }






  static class ParenOperator extends Operator {
    public ParenOperator(String paren) { super(paren); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      throw new Error("Parenthese should never be evaluated!");
    }
  }

  static class SimpleOperator extends Operator {
    public SimpleOperator(String str) { super(str); }
    public SimpleOperator(String str, int prec) { super(str, prec); }
    public Object eval(Object ob1, Object ob2,
                       GroupResolver group, VariableResolver resolver,
                       QueryContext context) {
      throw new Error("SimpleOperator should never be evaluated!");
    }
  }



}
