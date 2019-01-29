/**
 * com.mckoi.database.Expression  11 Jul 2000
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

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.io.StringReader;
import com.mckoi.database.sql.SQL;

/**
 * An expression that can be evaluated in a statement.  This is used as a more
 * complete and flexible version of 'Condition' as well as representing column
 * and aggregate functions.
 * <p>
 * This class can represent constant expressions (expressions that have no
 * variable input), as well as variable expressions.  Optimizations may be
 * possible when evaluating constant expressions.
 * <p>
 * Some examples of constant expressions;<p><pre>
 *   ( 9 + 3 ) * 90
 *   ( ? * 9 ) / 1
 *   lower("CaPS MUMma")
 *   40 & 0x0FF != 39
 * </pre>
 * Some examples of variable expressions;<p><pre>
 *   upper(Part.description)
 *   Part.id >= 50
 *   VendorMakesPart.part_id == Part.id
 *   Part.value_of <= Part.cost_of / 4
 * </pre>
 * <p>
 * <strong>NOTE:</strong> the expression is stored in postfix orientation.  eg.
 *   "8 + 9 * 3" becomes "8,9,3,*,+"
 * <p>
 * <strong>NOTE:</strong> This class is <b>NOT</b> thread safe.  Do not use an
 *   expression instance between threads.
 *
 * @author Tobias Downer
 */

public final class Expression implements java.io.Serializable, Cloneable {

  /**
   * Serialization UID.
   */
  static final long serialVersionUID = 6981261114471924028L;

  /**
   * The list of elements followed by operators in our expression.  The
   * expression elements may be of any type represented by the database
   * (see 'addElement' method for the accepted objects).  The expression
   * operators may be '+', '-', '*', '*', '/', '=', '>=', '<>', etc (as an
   * Operator object (see the Operator class for details)).
   * <p>
   * This list is stored in postfix order.
   */
  private ArrayList elements = new ArrayList();

  /**
   * The evaluation stack for when the expression is evaluated.
   */
  private transient ArrayList eval_stack;

  /**
   * The expression as a plain human readable string.  This is in a form that
   * can be readily parsed to an Expression object.
   */
  private StringBuffer text;


  /**
   * Constructs a new Expression.
   */
  public Expression() {
    text = new StringBuffer();
  }

  /**
   * Constructs a new Expression with a single object element.
   */
  public Expression(Object ob) {
    this();
    addElement(ob);
  }

  /**
   * Constructs a copy of the given Expression.
   */
  public Expression(Expression exp) {
    concat(exp);
    text = new StringBuffer(new String(exp.text));
  }

  /**
   * Constructs a new Expression from the concatination of expression1 and
   * expression2 and the operator for them.
   */
  public Expression(Expression exp1, Operator op, Expression exp2) {
    // Remember, this is in postfix notation.
    elements.addAll(exp1.elements);
    elements.addAll(exp2.elements);
    elements.add(op);
  }

  /**
   * Returns the StringBuffer that we can use to append plain text
   * representation as we are parsing the expression.
   */
  public StringBuffer text() {
    return text;
  }

  /**
   * Copies the text from the given expression.
   */
  public void copyTextFrom(Expression e) {
    this.text = new StringBuffer(new String(e.text()));
  }

  /**
   * Static method that parses the given string which contains an expression
   * into an Expression object.  For example, this will parse strings such
   * as '(a + 9) * 2 = b' or 'upper(concat('12', '56', id))'.
   * <p>
   * Care should be taken to not use this method inside an inner loop because
   * it creates a lot of objects.
   */
  public static Expression parse(String expression) {
    synchronized (expression_parser) {
      try {
        expression_parser.ReInit(new StringReader(expression));
        expression_parser.reset();
        Expression exp = expression_parser.parseExpression();

        exp.text().setLength(0);
        exp.text().append(expression);
        return exp;
      }
      catch (com.mckoi.database.sql.ParseException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  /**
   * A static expression parser.  To use this we must first synchronize over
   * the object.
   */
  private final static SQL expression_parser = new SQL(new StringReader(""));

  /**
   * Generates a simple expression from two objects and an operator.
   */
  public static Expression simple(Object ob1, Operator op, Object ob2) {
    Expression exp = new Expression(ob1);
    exp.addElement(ob2);
    exp.addElement(op);
    return exp;
  }


  /**
   * Adds a new element into the expression.  String, BigDecimal, Boolean and
   * Variable are the types of elements allowed.
   * <p>
   * Must be added in postfix order.
   */
  public void addElement(Object ob) {
    if (ob == null) {
      ob = NULL_OBJ;
      elements.add(ob);
    }
    else if (ob instanceof String ||
             ob instanceof BigDecimal ||
             ob instanceof Boolean ||
             ob instanceof Date ||
             ob instanceof com.mckoi.database.global.NullObject ||
             ob instanceof com.mckoi.database.global.ByteLongObject ||
             ob instanceof ParameterSubstitution ||
             ob instanceof CorrelatedVariable ||
             ob instanceof Variable ||
             ob instanceof FunctionDef ||
             ob instanceof Function ||
             ob instanceof Operator ||
             ob instanceof Expression[] ||
             ob instanceof StatementTreeObject ||
             ob instanceof QueryPlanNode
//             ob instanceof Table ||
//             ob instanceof Queriable
        ) {
      elements.add(ob);
    }
    else {
      throw new Error("Unknown element type added to expression: " +
                      ob.getClass());
    }
  }

  /**
   * Merges an expression with this expression.  For example, given the
   * expression 'ab', if the expression 'abc+-' was added the expression would
   * become 'ababc+-'.
   * <p>
   * This method is useful when copying parts of other expressions when forming
   * an expression.
   * <p>
   * This always returns this expression.  This does not change 'text()'.
   */
  public Expression concat(Expression expr) {
    elements.addAll(expr.elements);
    return this;
  }

  /**
   * Adds a new operator into the expression.  Operators are represented as
   * an Operator (eg. ">", "+", "<<", "!=" )
   * <p>
   * Must be added in postfix order.
   */
  public void addOperator(Operator op) {
    elements.add(op);
  }

  /**
   * Returns the number of elements and operators that are in this postfix
   * list.
   */
  public int size() {
    return elements.size();
  }

  /**
   * Returns the element at the given position in the postfix list.  Note, this
   * can return Operator's.
   */
  public Object elementAt(int n) {
    return elements.get(n);
  }

  /**
   * Returns the element at the end of the postfix list (the last element).
   */
  public Object last() {
    return elements.get(size() - 1);
  }


  /**
   * Sets the element at the given position in the postfix list.  This should
   * be called after the expression has been setup to alter variable alias
   * names, etc.
   */
  public void setElementAt(int n, Object ob) {
    elements.set(n, ob);
  }

  /**
   * Pushes an element onto the evaluation stack.
   */
  private final void push(Object ob) {
    eval_stack.add(ob);
  }

  /**
   * Pops an element from the evaluation stack.
   */
  private final Object pop() {
    return eval_stack.remove(eval_stack.size() - 1);
  }


  /**
   * Returns a complete List of Variable objects in this expression not
   * including correlated variables.
   */
  public List allVariables() {
    ArrayList vars = new ArrayList();
    for (int i = 0; i < elements.size(); ++i) {
      Object ob = elements.get(i);
      if (ob instanceof Variable) {
        vars.add(ob);
      }
      else if (ob instanceof Function) {
        vars.addAll(((Function) ob).allVariables());
      }
      else if (ob instanceof FunctionDef) {
        Expression[] params = ((FunctionDef)ob).getParameters();
        for (int n = 0; n < params.length; ++n) {
          vars.addAll(params[n].allVariables());
        }
      }
      else if (ob instanceof Expression[]) {
        Expression[] exp_list = (Expression[]) ob;
        for (int n = 0; n < exp_list.length; ++n) {
          vars.addAll(exp_list[n].allVariables());
        }
      }
    }
    return vars;
  }

  /**
   * Returns a complete list of all element objects that are in this expression
   * and in the parameters of the functions of this expression.
   */
  public List allElements() {
    ArrayList elems = new ArrayList();
    for (int i = 0; i < elements.size(); ++i) {
      Object ob = elements.get(i);
      if (ob instanceof Operator) {
      }
      else if (ob instanceof Function) {
        elems.addAll(((Function) ob).allElements());
      }
      else if (ob instanceof FunctionDef) {
        Expression[] params = ((FunctionDef) ob).getParameters();
        for (int n = 0; n < params.length; ++n) {
          elems.addAll(params[n].allElements());
        }
      }
      else if (ob instanceof Expression[]) {
        Expression[] exp_list = (Expression[]) ob;
        for (int n = 0; n < exp_list.length; ++n) {
          elems.addAll(exp_list[n].allElements());
        }
      }
      else {
        elems.add(ob);
      }
    }
    return elems;
  }

  /**
   * A general prepare that cascades through the expression and its parents and
   * substitutes an elements that the preparer wants to substitute.
   * <p>
   * NOTE: This will not cascade through to the parameters of Function objects
   *   however it will cascade through FunctionDef parameters.  For this
   *   reason you MUST call 'prepareFunctions' after this method.
   */
  public void prepare(ExpressionPreparer preparer) throws DatabaseException {
    for (int n = 0; n < elements.size(); ++n) {
      Object ob = elements.get(n);

      // If the preparer will prepare this type of object then set the
      // entry with the prepared object.
      if (preparer.canPrepare(ob)) {
        elements.set(n, preparer.prepare(ob));
      }

      Expression[] exp_list = null;
      if (ob instanceof FunctionDef) {
        FunctionDef func = (FunctionDef) ob;
        exp_list = func.getParameters();
      }
      else if (ob instanceof Function) {
        Function func = (Function) ob;
        func.prepareParameters(preparer);
      }
      else if (ob instanceof Expression[]) {
        exp_list = (Expression[]) ob;
      }
      else if (ob instanceof StatementTreeObject) {
        StatementTreeObject stree = (StatementTreeObject) ob;
        stree.prepareExpressions(preparer);
      }

      if (exp_list != null) {
        for (int p = 0; p < exp_list.length; ++p) {
          exp_list[p].prepare(preparer);
        }
      }

    }
  }


  /**
   * Returns true if the expression doesn't include any variables or non
   * constant functions (is constant).  Note that a correlated variable is
   * considered a constant.
   */
  public boolean isConstant() {
    for (int n = 0; n < elements.size(); ++n) {
      Object ob = elements.get(n);
      if (ob instanceof Variable ||
          ob instanceof QueryPlanNode) {
        return false;
      }
      else if (ob instanceof Function) {
        if (((Function) ob).allVariables().size() > 0) {
          return false;
        }
      }
      else if (ob instanceof FunctionDef) {
        Expression[] params = ((FunctionDef) ob).getParameters();
        for (int p = 0; p < params.length; ++p) {
          if (!params[p].isConstant()) {
            return false;
          }
        }
      }
      else if (ob instanceof Expression[]) {
        Expression[] exp_list = (Expression[]) ob;
        for (int p = 0; p < exp_list.length; ++p) {
          if (!exp_list[p].isConstant()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Returns true if the expression has a subquery (eg 'in ( select ... )')
   * somewhere in it (this cascades through function parameters also).
   */
  public boolean hasSubQuery() {
    List list = allElements();
    int len = list.size();
    for (int n = 0; n < len; ++n) {
      Object ob = list.get(n);
      if (ob instanceof QueryPlanNode) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the expression contains a NOT operator somewhere in it.
   */
  public boolean containsNotOperator() {
    for (int n = 0; n < elements.size(); ++n) {
      Object ob = elements.get(n);
      if (ob instanceof Operator) {
        if (((Operator) ob).isNot()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Discovers all the correlated variables in this expression.  If this
   * expression contains a sub-query plan, we ask the plan to find the list of
   * correlated variables.  The discovery process increments the 'level'
   * variable for each sub-plan.
   */
  public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
    List elems = allElements();
    int sz = elems.size();
    // For each element
    for (int i = 0; i < sz; ++i) {
      Object ob = elems.get(i);
      if (ob instanceof CorrelatedVariable) {
        CorrelatedVariable v = (CorrelatedVariable) ob;
        if (v.getQueryLevelOffset() == level) {
          list.add(v);
        }
      }
      else if (ob instanceof QueryPlanNode) {
        QueryPlanNode node = (QueryPlanNode) ob;
        list = node.discoverCorrelatedVariables(level + 1, list);
      }
    }
    return list;
  }

  /**
   * Discovers all the tables in the sub-queries of this expression.  This is
   * used for determining all the tables that a query plan touches.
   */
  public ArrayList discoverTableNames(ArrayList list) {
    List elems = allElements();
    int sz = elems.size();
    // For each element
    for (int i = 0; i < sz; ++i) {
      Object ob = elems.get(i);
      if (ob instanceof QueryPlanNode) {
        QueryPlanNode node = (QueryPlanNode) ob;
        list = node.discoverTableNames(list);
      }
    }
    return list;
  }

  /**
   * Returns the QueryPlanNode object in this expression if it evaluates to a
   * single QueryPlanNode, otherwise returns null.
   */
  public QueryPlanNode getQueryPlanNode() {
    Object ob = elementAt(0);
    if (size() == 1 && ob instanceof QueryPlanNode) {
      return (QueryPlanNode) ob;
    }
    return null;
  }

  /**
   * Returns the Variable if this expression evaluates to a single variable,
   * otherwise returns null.  A correlated variable will not be returned.
   */
  public Variable getVariable() {
    Object ob = elementAt(0);
    if (size() == 1 && ob instanceof Variable) {
      return (Variable) ob;
    }
    return null;
  }

  /**
   * Returns an array of two Expression objects that represent the left hand
   * and right and side of the last operator in the post fix notation.
   * For example, (a + b) - (c + d) will return { (a + b), (c + d) }.  Or
   * more a more useful example is;<p><pre>
   *   id + 3 > part_id - 2 will return ( id + 3, part_id - 2 }
   * </pre>
   */
  public Expression[] split() {
    if (size() <= 1) {
      throw new Error("Can only split expressions with more than 1 element.");
    }

    int midpoint = -1;
    int stack_size = 0;
    for (int n = 0; n < size() - 1; ++n) {
      Object ob = elementAt(n);
      if (ob instanceof Operator) {
        --stack_size;
      }
      else {
        ++stack_size;
      }

      if (stack_size == 1) {
        midpoint = n;
      }
    }

    if (midpoint == -1) {
      throw new Error("postfix format error: Midpoint not found.");
    }

    Expression lhs = new Expression();
    for (int n = 0; n <= midpoint; ++n) {
      lhs.addElement(elementAt(n));
    }

    Expression rhs = new Expression();
    for (int n = midpoint + 1; n < size() - 1; ++n) {
      rhs.addElement(elementAt(n));
    }

    return new Expression[] { lhs, rhs };
  }

  /**
   * Returns the end Expression of this expression.  For example, an expression
   * of 'ab' has an end expression of 'b'.  The expression 'abc+=' has an end
   * expression of 'abc+='.
   * <p>
   * This is a useful method to call in the middle of an Expression object
   * being formed.  It allows for the last complete expression to be
   * returned.
   * <p>
   * If this is called when an expression is completely formed it will always
   * return the complete expression.
   */
  public Expression getEndExpression() {

    int stack_size = 1;
    int end = size() - 1;
    for (int n = end; n > 0; --n) {
      Object ob = elementAt(n);
      if (ob instanceof Operator) {
        ++stack_size;
      }
      else {
        --stack_size;
      }

      if (stack_size == 0) {
        // Now, n .. end represents the new expression.
        Expression new_exp = new Expression();
        for (int i = n; i <= end; ++i) {
          new_exp.addElement(elementAt(i));
        }
        return new_exp;
      }
    }

    return new Expression(this);
  }

  /**
   * Breaks this expression into a list of sub-expressions that are split
   * by the given operator.  For example, given the expression;
   * <p><pre>
   *   (a = b AND b = c AND (a = 2 OR c = 1))
   * </pre><p>
   * Calling this method with logical_op = "and" will return a list of the
   * three expressions.
   * <p>
   * This is a common function used to split up an expressions into logical
   * components for processing.
   */
  public ArrayList breakByOperator(ArrayList list, final String logical_op) {
    // The last operator must be 'and'
    Object ob = last();
    if (ob instanceof Operator) {
      Operator op = (Operator) ob;
      if (op.is(logical_op)) {
        // Last operator is 'and' so split and recurse.
        Expression[] exps = split();
        list = exps[0].breakByOperator(list, logical_op);
        list = exps[1].breakByOperator(list, logical_op);
        return list;
      }
    }
    // If no last expression that matches then add this expression to the
    // list.
    list.add(this);
    return list;
  }

  /**
   * Evaluates this expression and returns an Object that represents the
   * result of the evaluation.  The passed VariableResolver object is used
   * to resolve the variable name to a value.  The GroupResolver object is
   * used if there are any aggregate functions in the evaluation - this can be
   * null if evaluating an expression without grouping aggregates.  The query
   * context object contains contextual information about the environment of
   * the query.
   * <p>
   * NOTE: This method is gonna be called a lot, so we need it to be optimal.
   * <p>
   * NOTE: This method is <b>not</b> thread safe!  The reason it's not safe
   *   is because of the evaluation stack.
   */
  public Object evaluate(GroupResolver group, VariableResolver resolver,
                         QueryContext context) {
    // Optimization - trivial case of 'a' or 'ab*' postfix are tested for
    //   here.
    int element_count = elements.size();
    if (element_count == 1) {
      return elementToObject(0, group, resolver, context);
    }
    else if (element_count == 3) {
      Object o1 = elementToObject(0, group, resolver, context);
      Object o2 = elementToObject(1, group, resolver, context);
      Operator op = (Operator) elements.get(2);
      return op.eval(o1, o2, group, resolver, context);
    }

    if (eval_stack == null) {
      eval_stack = new ArrayList();
    }

    for (int n = 0; n < element_count; ++n) {
      Object val = elementToObject(n, group, resolver, context);
      if (val instanceof Operator) {
        // Pop the last two values off the stack, evaluate them and push
        // the new value back on.
        Operator op = (Operator) val;

        Object v2 = pop();
        Object v1 = pop();

        push(op.eval(v1, v2, group, resolver, context));
      }
      else {
        push(val);
      }
    }
    // We should end with a single value on the stack.
    return pop();
  }

  /**
   * Evaluation without a grouping table.
   */
  public Object evaluate(VariableResolver resolver, QueryContext context) {
    return evaluate(null, resolver, context);
  }

  /**
   * Returns the element at the given position in the expression list.  If
   * the element is a variable then it is resolved on the VariableResolver.
   * If the element is a function then it is evaluated and the result is
   * returned.
   */
  private Object elementToObject(int n, GroupResolver group,
                           VariableResolver resolver, QueryContext context) {
    Object ob = elements.get(n);
    if (ob instanceof String ||
        ob instanceof BigDecimal ||
        ob instanceof Boolean ||
        ob instanceof Date ||
        ob instanceof com.mckoi.database.global.NullObject ||
        ob instanceof com.mckoi.database.global.ByteLongObject ||
        ob instanceof Operator ||
        ob instanceof Expression[] ||
        ob instanceof QueryPlanNode) {
//        ob instanceof Table ||
//        ob instanceof Queriable) {
      return ob;
    }
    else if (ob instanceof Variable) {
      return resolver.resolve((Variable) ob);
    }
    else if (ob instanceof CorrelatedVariable) {
      return ((CorrelatedVariable) ob).getEvalResult();
    }
    else if (ob instanceof Function) {
      return ((Function) ob).evaluate(group, resolver, context);
    }
    else if (ob instanceof FunctionDef) {
      throw new Error(
               "Expression contains FunctionDef which can not be evaluated.");
    }
    else {
      if (ob == null) {
        throw new NullPointerException("Null element in expression");
      }
      throw new Error("Unknown element type: " + ob.getClass());
    }
  }

  /**
   * Cascades through the expression and if any aggregate functions are found
   * returns true, otherwise returns false.
   */
  public boolean hasAggregateFunction() {
    for (int n = 0; n < elements.size(); ++n) {
      Object ob = elements.get(n);
      if (ob instanceof Function) {
        if (((Function) ob).isAggregate()) {
          return true;
        }
      }
      else if (ob instanceof Expression[]) {
        Expression[] list = (Expression[]) ob;
        for (int i = 0; i < list.length; ++i) {
          if (list[i].hasAggregateFunction()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Determines the class of object this expression evaluates to.  We determine
   * this by looking at the last element of the expression.  If the last
   * element is a Java object stored in the database, it returns the class
   * of that object.  If the last element is a Function, Operator or Variable
   * then it returns the class that these objects have set as their result
   * classes.
   */
  public Class returnClass(VariableResolver resolver, QueryContext context) {
    Object ob = elements.get(elements.size() - 1);
    if (ob instanceof Function) {
      return ((Function) ob).returnClass(resolver, context);
    }
    else if (ob instanceof String) {
      return String.class;
    }
    else if (ob instanceof Boolean) {
      return Boolean.class;
    }
    else if (ob instanceof BigDecimal) {
      return BigDecimal.class;
    }
    else if (ob instanceof java.util.Date) {
      return java.util.Date.class;
    }
    else if (ob instanceof com.mckoi.database.global.NullObject) {
      return com.mckoi.database.global.NullObject.class;
    }
    else if (ob instanceof com.mckoi.database.global.ByteLongObject) {
      return com.mckoi.database.global.ByteLongObject.class;
    }
    else if (ob instanceof Operator) {
      Operator op = (Operator) ob;
      return op.returnClass();
    }
    else if (ob instanceof Variable) {
      Variable variable = (Variable) ob;
      return resolver.classType(variable);
    }
    else if (ob instanceof CorrelatedVariable) {
      CorrelatedVariable variable = (CorrelatedVariable) ob;
      return variable.getEvalClass();
    }
    else if (ob instanceof FunctionDef) {
      throw new Error(
               "Expression contains FunctionDef which can not be evaluated.");
    }
    else {
//      System.out.println("Class type: " + ob.getClass());
      throw new Error("Unable to determine return class for expression.");
    }
  }



  /**
   * Performs a deep clone of this object, calling 'clone' on any elements
   * that are mutable or shallow copying immutable members.
   */
  public Object clone() throws CloneNotSupportedException {
    // Shallow clone
    Expression v = (Expression) super.clone();
    v.eval_stack = null;
//    v.text = new StringBuffer(new String(text));
    int size = elements.size();
    ArrayList cloned_elements = new ArrayList(size);
    v.elements = cloned_elements;

    // Clone items in the elements list
    for (int i = 0; i < size; ++i) {
      Object element = elements.get(i);
      // These are immutable members
      if (element instanceof Operator ||
          element instanceof BigDecimal ||
          element instanceof String ||
          element instanceof Boolean ||
          element instanceof java.util.Date ||
          element instanceof com.mckoi.database.global.ByteLongObject ||
          element instanceof com.mckoi.database.global.NullObject ||
          element instanceof ParameterSubstitution) {
        // immutable so we do not need to clone these
      }
      else if (element instanceof CorrelatedVariable) {
        element = ((CorrelatedVariable) element).clone();
      }
      else if (element instanceof Variable) {
        element = ((Variable) element).clone();
      }
      else if (element instanceof QueryPlanNode) {
        element = ((QueryPlanNode) element).clone();
      }
      else if (element instanceof FunctionDef) {
        element = ((FunctionDef) element).clone();
      }
      else if (element instanceof Expression[]) {
        Expression[] exps = (Expression[]) ((Expression[]) element).clone();
        // Clone each element of the array
        for (int n = 0; n < exps.length; ++n) {
          exps[n] = (Expression) exps[n].clone();
        }
        element = exps;
      }
      else if (element instanceof StatementTreeObject) {
        element = ((StatementTreeObject) element).clone();
      }
      else {
        throw new CloneNotSupportedException(element.getClass().toString());
      }
      cloned_elements.add(element);
    }

    return v;
  }

  /**
   * Returns a string representation of this object for diagnostic
   * purposes.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[ Expression ");
    if (text() != null) {
      buf.append("[");
      buf.append(text().toString());
      buf.append("]");
    }
    buf.append(": ");
    for (int n = 0; n < elements.size(); ++n) {
      buf.append(elements.get(n));
      if (n < elements.size() - 1) {
        buf.append(",");
      }
    }
    buf.append(" ]");
    return new String(buf);
  }


  // This is here purely for legacy reasons.  We should probably get rid of
  // it.  'global.NullObject' should be used instead.

  static final Object NULL_OBJ = NullObject.NULL_OBJ;

  public static class NullObject extends com.mckoi.database.global.NullObject {
  }

}
