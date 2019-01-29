/**
 * com.mckoi.database.InternalFunctionFactory  19 Sep 2000
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

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Arrays;
import java.text.*;
import com.mckoi.util.Cache;
import com.mckoi.database.global.SQLTypes;
import com.mckoi.database.global.CastHelper;
import com.mckoi.database.global.NullObject;
import com.mckoi.database.global.ByteLongObject;

/**
 * A FunctionFactory for all internal SQL functions (including aggregate,
 * mathematical, string functions).  This FunctionFactory is registered with
 * the DatabaseSystem during initialization.
 *
 * @author Tobias Downer
 */

final class InternalFunctionFactory extends FunctionFactory {

  /**
   * Registers the function classes with this factory.
   */
  public void init() {

    // Parses a date/time/timestamp string
    addFunction("dateob", DateObFunction.class);
    addFunction("timeob", TimeObFunction.class);
    addFunction("timestampob", TimeStampObFunction.class);
    addFunction("dateformat", DateFormatFunction.class);

    // Casting functions
    addFunction("tonumber", ToNumberFunction.class);
    // String functions
    addFunction("lower", LowerFunction.class);
    addFunction("upper", UpperFunction.class);
    addFunction("concat", ConcatFunction.class);
    addFunction("length", LengthFunction.class);
    addFunction("substring", SubstringFunction.class);
    addFunction("sql_trim", SQLTrimFunction.class);
    addFunction("ltrim", LTrimFunction.class);
    addFunction("rtrim", RTrimFunction.class);
    // Security
    addFunction("user", UserFunction.class);
    addFunction("privgroups", PrivGroupsFunction.class);
    // Aggregate
    addFunction("count", CountFunction.class, FunctionInfo.AGGREGATE);
    addFunction("distinct_count",
                DistinctCountFunction.class, FunctionInfo.AGGREGATE);
    addFunction("avg", AvgFunction.class, FunctionInfo.AGGREGATE);
    addFunction("sum", SumFunction.class, FunctionInfo.AGGREGATE);
    addFunction("min", MinFunction.class, FunctionInfo.AGGREGATE);
    addFunction("max", MaxFunction.class, FunctionInfo.AGGREGATE);
    // Mathematical
    addFunction("abs", AbsFunction.class);
    addFunction("sign", SignFunction.class);
    addFunction("mod", ModFunction.class);
    addFunction("round", RoundFunction.class);
    addFunction("pow", PowFunction.class);
    addFunction("sqrt", SqrtFunction.class);
    // Misc
    addFunction("uniquekey",
                UniqueKeyFunction.class, FunctionInfo.STATE_BASED);
    addFunction("hextobinary", HexToBinaryFunction.class);
    addFunction("binarytohex", BinaryToHexFunction.class);
    // Lists
    addFunction("least", LeastFunction.class);
    addFunction("greatest", GreatestFunction.class);
    // Branch
    addFunction("if", IfFunction.class);
    addFunction("coalesce", CoalesceFunction.class);

    // Object instantiation (Internal)
    addFunction("_new_JavaObject", JavaObjectInstantiation.class);

    // Internal functions
    addFunction("i_frule_convert", ForeignRuleConvert.class);
    addFunction("i_sql_type", SQLTypeString.class);

  }


  // ---------- The internal functions ----------

  private static final BigDecimal BD_ZERO = new BigDecimal(0);

  // ---------- Grouping functions ----------

  private static class CountFunction extends AbstractFunction {

    public CountFunction(Expression[] params) {
      super("count", params);
      setAggregate(true);

      if (parameterCount() != 1) {
        throw new Error("'count' function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      if (group == null) {
        throw new Error("'count' can only be used as an aggregate function.");
      }

      int size = group.size();
      BigDecimal result;
      // if, count(*)
      if (size == 0 || isGlob()) {
        result = new BigDecimal(size);
      }
      else {
        // Otherwise we need to count the number of non-null entries in the
        // columns list(s).

        int total_count = size;

        Expression exp = getParameter(0);
        for (int i = 0; i < size; ++i) {
          Object val =
                    exp.evaluate(null, group.getVariableResolver(i), context);
          if (val == null || val instanceof NullObject) {
            --total_count;
          }
        }

        result = new BigDecimal(total_count);
      }

      return result;
    }

  }

  // --

  private static class DistinctCountFunction extends AbstractFunction {

    public DistinctCountFunction(Expression[] params) {
      super("distinct_count", params);
      setAggregate(true);

      if (parameterCount() <= 0) {
        throw new Error(
               "'distinct_count' function must have at least one argument.");
      }

    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      // There's some issues with implementing this function.
      // For this function to be efficient, we need to have access to the
      // underlying Table object(s) so we can use table indexing to sort the
      // columns.  Otherwise, we will need to keep in memory the group
      // contents so it can be sorted.  Or alternatively (and probably worst
      // of all) don't store in memory, but use an expensive iterative search
      // for non-distinct rows.
      //
      // An iterative search will be terrible for large groups with mostly
      // distinct rows.  But would be okay for large groups with few distinct
      // rows.

      if (group == null) {
        throw new Error("'count' can only be used as an aggregate function.");
      }

      final int rows = group.size();
      if (rows <= 1) {
        // If count of entries in group is 0 or 1
        return new BigDecimal(rows);
      }

      // Make an array of all cells in the group that we are finding which
      // are distinct.
      final int cols = parameterCount();
      final Object[] group_r = new Object[rows * cols];
      int n = 0;
      for (int i = 0; i < rows; ++i) {
        VariableResolver vr = group.getVariableResolver(i);
        for (int p = 0; p < cols; ++p) {
          Expression exp = getParameter(p);
          group_r[n + p] = exp.evaluate(null, vr, context);
        }
        n += cols;
      }

      // A comparator that sorts this set,
      Comparator c = new Comparator() {
        public int compare(Object ob1, Object ob2) {
          int r1 = ((Integer) ob1).intValue();
          int r2 = ((Integer) ob2).intValue();

          // Compare row r1 with r2
          int index1 = r1 * cols;
          int index2 = r2 * cols;
          for (int n = 0; n < cols; ++n) {
            int v = Operator.compareObs(group_r[index1 + n],
                                        group_r[index2 + n]);
            if (v != 0) {
              return v;
            }
          }

          // If we got here then rows must be equal.
          return 0;
        }
      };

      // The list of indexes,
      Object[] list = new Object[rows];
      for (int i = 0; i < rows; ++i) {
        list[i] = new Integer(i);
      }

      // Sort the list,
      Arrays.sort(list, c);

      // The count of distinct elements, (there will always be at least 1)
      int distinct_count = 1;
      for (int i = 1; i < rows; ++i) {
        int v = c.compare(list[i], list[i - 1]);
        // If v == 0 then entry is not distinct with the previous element in
        // the sorted list therefore the distinct counter is not incremented.
        if (v > 0) {
          // If current entry is greater than previous then we've found a
          // distinct entry.
          ++distinct_count;
        }
        else if (v < 0) {
          // The current element should never be less if list is sorted in
          // ascending order.
          throw new Error("Assertion failed - the distinct list does not " +
                          "appear to be sorted.");
        }
      }

      return new BigDecimal(distinct_count);
    }

  }

  // --

  private static class AvgFunction extends AbstractAggregateFunction {

    public AvgFunction(Expression[] params) {
      super("avg", params);
    }

    public Object evalAggregate(GroupResolver group, QueryContext context,
                                Object ob1, Object ob2) {
      // This will sum,
      if (ob1 != null) {
        if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
          return ob1;
        }
        return Operator.toNumber(ob1).add(Operator.toNumber(ob2));
      }
      return ob2;
    }

    public Object postEvalAggregate(GroupResolver group, QueryContext context,
                                    Object result) {
      // Find the average from the sum result
      if (result instanceof NullObject) {
        return result;
      }
      return Operator.toNumber(result).divide(
                  new BigDecimal(group.size()), 10, BigDecimal.ROUND_HALF_UP);
    }

  }

  // --

  private static class SumFunction extends AbstractAggregateFunction {

    public SumFunction(Expression[] params) {
      super("sum", params);
    }

    public Object evalAggregate(GroupResolver group, QueryContext context,
                                Object ob1, Object ob2) {
      // This will sum,
      if (ob1 != null) {
        if (ob1 instanceof NullObject || ob2 instanceof NullObject) {
          return ob1;
        }
        return Operator.toNumber(ob1).add(Operator.toNumber(ob2));
      }
      return ob2;
    }


  }

  // --

  private static class MinFunction extends AbstractAggregateFunction {

//    // Cache for aggregates within this group.
//    HashMap group_cache = new HashMap();

    public MinFunction(Expression[] params) {
      super("min", params);
    }

    public Object evalAggregate(GroupResolver group, QueryContext context,
                                Object ob1, Object ob2) {
      // This will find min,
      if (ob1 != null) {
        int c = Operator.compareObs(ob2, ob1);
        if (c < 0) {
          return ob2;
        }
        return ob1;
      }
      return ob2;
    }

//    public Object evaluate(GroupResolver group, VariableResolver resolver) {
//      if (group == null) {
//        throw new Error("This is a grouping function.");
//      }
//
//      Comparable min = null;
//      Integer id = new Integer(group.groupID());
//      min = (Comparable) group_cache.get(id);
//      if (min == null) {
//
//        int size = group.size();
//        for (int i = 0; i < size; ++i) {
//          Comparable ob = (Comparable) group.resolve(v, i);
//          if (ob != null) {
//            if (min == null || ob.compareTo(min) < 0) {
//              min = ob;
//            }
//          }
//        }
//
//        group_cache.put(id, min);
//      }
//
//      return min;
//    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      // Set to return the same type object as this variable.
      return getParameter(0).returnClass(resolver, context);
    }

  }

  // --

  private static class MaxFunction extends AbstractAggregateFunction {

    public MaxFunction(Expression[] params) {
      super("max", params);
    }

    public Object evalAggregate(GroupResolver group, QueryContext context,
                                Object ob1, Object ob2) {
      // This will find min,
      if (ob1 != null) {
        int c = Operator.compareObs(ob2, ob1);
        if (c > 0) {
          return ob2;
        }
        return ob1;
      }
      return ob2;
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      // Set to return the same type object as this variable.
      return getParameter(0).returnClass(resolver, context);
    }

  }




  // ---------- User functions ----------

  // Returns the user name
  private static class UserFunction extends AbstractFunction {

    public UserFunction(Expression[] params) {
      super("user", params);

      if (parameterCount() > 0) {
        throw new Error("'user' function must have no arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      return context.getUserName();
    }

    public Class returnClass() {
      return String.class;
    }

  }

  // Returns the comma (",") deliminated priv groups the user belongs to.
  private static class PrivGroupsFunction extends AbstractFunction {

    public PrivGroupsFunction(Expression[] params) {
      super("privgroups", params);

      if (parameterCount() > 0) {
        throw new Error("'privgroups' function must have no arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      throw new Error("'PrivGroups' function currently not working.");
    }

    public Class returnClass() {
      return String.class;
    }

  }






  // ---------- String functions ----------

  private static class LowerFunction extends AbstractFunction {

    public LowerFunction(Expression[] params) {
      super("lower", params);

      if (parameterCount() != 1) {
        throw new Error("Lower function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      return ob.toString().toLowerCase();
    }

    public Class returnClass() {
      return String.class;
    }

  }

  // --

  private static class UpperFunction extends AbstractFunction {

    public UpperFunction(Expression[] params) {
      super("upper", params);

      if (parameterCount() != 1) {
        throw new Error("Upper function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      return ob.toString().toUpperCase();
    }

    public Class returnClass() {
      return String.class;
    }

  }

  // --

  private static class ConcatFunction extends AbstractFunction {

    public ConcatFunction(Expression[] params) {
      super("concat", params);
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      StringBuffer cc = new StringBuffer();
      for (int i = 0; i < parameterCount(); ++i) {
        Object ob = getParameter(i).evaluate(group, resolver, context);
        if (!(ob instanceof NullObject)) {
          cc.append(ob.toString());
        }
        else {
          return ob;
        }
      }
      return new String(cc);
    }

    public Class returnClass() {
      return String.class;
    }

  }

  // --

  private static class LengthFunction extends AbstractFunction {

    public LengthFunction(Expression[] params) {
      super("length", params);

      if (parameterCount() != 1) {
        throw new Error("Length function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      if (ob instanceof ByteLongObject) {
        return new BigDecimal(((ByteLongObject) ob).length());
      }
      return new BigDecimal(ob.toString().length());
    }

  }

  // --

  private static class SubstringFunction extends AbstractFunction {

    public SubstringFunction(Expression[] params) {
      super("substring", params);

      if (parameterCount() < 1 || parameterCount() > 3) {
        throw new Error("Substring function needs one to three arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      String str = ob.toString();
      int pcount = parameterCount();
      int str_length = str.length();
      int arg1 = 0;
      int arg2 = str_length;
      if (pcount >= 2) {
        arg1 = Operator.toNumber(
               getParameter(1).evaluate(group, resolver, context)).intValue();
      }
      if (pcount >= 3) {
        arg2 = Operator.toNumber(
               getParameter(2).evaluate(group, resolver, context)).intValue();
      }

      // Make sure this call is safe for all lengths of string.
      if (arg1 < 0) {
        arg1 = 0;
      }
      if (arg1 >= str_length) {
        return "";
      }
      if (arg2 >= str_length) {
        arg2 = str_length;
      }
      if (arg2 <= arg1) {
        return "";
      }

      return str.substring(arg1, arg2);
    }

    public Class returnClass() {
      return String.class;
    }

  }

  // --

  private static class SQLTrimFunction extends AbstractFunction {

    public SQLTrimFunction(Expression[] params) {
      super("sql_trim", params);

//      System.out.println(parameterCount());
      if (parameterCount() != 3) {
        throw new Error("SQL Trim function must have three parameters.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      // The type of trim (leading, both, trailing)
      Object ttype = getParameter(0).evaluate(group, resolver, context);
      // Characters to trim
      Object cob = getParameter(1).evaluate(group, resolver, context);
      if (cob instanceof NullObject) {
        return cob;
      }
      String characters = cob.toString();
      // The content to trim.
      Object ob = getParameter(2).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      String str = ob.toString();

      int skip = characters.length();
      // Do the trim,
      if (ttype.equals("leading") || ttype.equals("both")) {
        // Trim from the start.
        int scan = 0;
        while (scan < str.length() &&
               str.indexOf(characters, scan) == scan) {
          scan += skip;
        }
        str = str.substring(Math.min(scan, str.length()));
      }
      if (ttype.equals("trailing") || ttype.equals("both")) {
        // Trim from the end.
        int scan = str.length() - 1;
        int i = str.lastIndexOf(characters, scan);
        while (scan >= 0 && i != -1 && i == scan - skip + 1) {
          scan -= skip;
          i = str.lastIndexOf(characters, scan);
        }
        str = str.substring(0, Math.max(0, scan + 1));
      }

      return str;
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return String.class;
    }

  }

  // --

  private static class LTrimFunction extends AbstractFunction {

    public LTrimFunction(Expression[] params) {
      super("ltrim", params);

      if (parameterCount() != 1) {
        throw new Error("ltrim function may only have 1 parameter.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      String str = ob.toString();

      // Do the trim,
      // Trim from the start.
      int scan = 0;
      while (scan < str.length() &&
             str.indexOf(' ', scan) == scan) {
        scan += 1;
      }
      str = str.substring(Math.min(scan, str.length()));

      return str;
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return String.class;
    }

  }

  // --

  private static class RTrimFunction extends AbstractFunction {

    public RTrimFunction(Expression[] params) {
      super("rtrim", params);

      if (parameterCount() != 1) {
        throw new Error("rtrim function may only have 1 parameter.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      String str = ob.toString();

      // Do the trim,
      // Trim from the end.
      int scan = str.length() - 1;
      int i = str.lastIndexOf(" ", scan);
      while (scan >= 0 && i != -1 && i == scan - 2) {
        scan -= 1;
        i = str.lastIndexOf(" ", scan);
      }
      str = str.substring(0, Math.max(0, scan + 1));

      return str;
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return String.class;
    }

  }








  // ---------- Mathematical functions ----------

  private static class AbsFunction extends AbstractFunction {

    public AbsFunction(Expression[] params) {
      super("abs", params);

      if (parameterCount() != 1) {
        throw new Error("Abs function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      BigDecimal num = Operator.toNumber(ob);
      return num.abs();
    }

  }

  // --

  private static class SignFunction extends AbstractFunction {

    public SignFunction(Expression[] params) {
      super("sign", params);

      if (parameterCount() != 1) {
        throw new Error("Sign function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }
      BigDecimal num = Operator.toNumber(ob);
      return new BigDecimal(num.signum());
    }

  }

  // --

  private static class ModFunction extends AbstractFunction {

    public ModFunction(Expression[] params) {
      super("mod", params);

      if (parameterCount() != 2) {
        throw new Error("Mod function must have two arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob1 = getParameter(0).evaluate(group, resolver, context);
      Object ob2 = getParameter(1).evaluate(group, resolver, context);
      if (ob1 instanceof NullObject) {
        return ob1;
      }
      else if (ob2 instanceof NullObject) {
        return ob2;
      }

      double v = Operator.toNumber(ob1).doubleValue();
      double m = Operator.toNumber(ob2).doubleValue();
      return new BigDecimal(v % m);
    }

  }

  // --

  private static class RoundFunction extends AbstractFunction {

    public RoundFunction(Expression[] params) {
      super("round", params);

      if (parameterCount() < 1 || parameterCount() > 2) {
        throw new Error("Round function must have one or two arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob1 = getParameter(0).evaluate(group, resolver, context);
      if (ob1 instanceof NullObject) {
        return ob1;
      }

      BigDecimal v = Operator.toNumber(ob1);
      int d = 0;
      if (parameterCount() == 2) {
        Object ob2 = getParameter(1).evaluate(group, resolver, context);
        if (ob2 instanceof NullObject) {
          d = 0;
        }
        else {
          d = Operator.toNumber(ob2).intValue();
        }
      }
      return v.setScale(d, BigDecimal.ROUND_HALF_UP);
    }

  }

  // --

  private static class PowFunction extends AbstractFunction {

    public PowFunction(Expression[] params) {
      super("pow", params);

      if (parameterCount() != 2) {
        throw new Error("Pow function must have two arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob1 = getParameter(0).evaluate(group, resolver, context);
      Object ob2 = getParameter(1).evaluate(group, resolver, context);
      if (ob1 instanceof NullObject) {
        return ob1;
      }
      else if (ob2 instanceof NullObject) {
        return ob2;
      }

      double v = Operator.toNumber(ob1).doubleValue();
      double w = Operator.toNumber(ob2).doubleValue();
      return new BigDecimal(Math.pow(v, w));
    }

  }

  // --

  private static class SqrtFunction extends AbstractFunction {

    public SqrtFunction(Expression[] params) {
      super("sqrt", params);

      if (parameterCount() != 1) {
        throw new Error("Sqrt function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return ob;
      }

      double v = Operator.toNumber(ob).doubleValue();
      v = Math.sqrt(v);
      if (v != v) {
        return NullObject.NULL_OBJ;
      }
      return new BigDecimal(v);
    }

  }

  // --

  private static class LeastFunction extends AbstractFunction {

    public LeastFunction(Expression[] params) {
      super("least", params);

      if (parameterCount() < 1) {
        throw new Error("Least function must have at least 1 argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object least = null;
      for (int i = 0; i < parameterCount(); ++i) {
        Object ob = getParameter(i).evaluate(group, resolver, context);
        if (least == null ||
            (ob != null && Operator.compareObs(ob, least) < 0)) {
          least = ob;
        }
      }
      return least;
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return getParameter(0).returnClass(resolver, context);
    }

  }

  // --

  private static class GreatestFunction extends AbstractFunction {

    public GreatestFunction(Expression[] params) {
      super("greatest", params);

      if (parameterCount() < 1) {
        throw new Error("Greatest function must have at least 1 argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object great = null;
      for (int i = 0; i < parameterCount(); ++i) {
        Object ob = getParameter(i).evaluate(group, resolver, context);
        if ((great == null && ob != null) ||
            (ob != null && Operator.compareObs(ob, great) > 0)) {
          great = ob;
        }
      }
      return great;
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return getParameter(0).returnClass(resolver, context);
    }

  }

  // --

  private static class UniqueKeyFunction extends AbstractFunction {

    BigDecimal unique_key = null;

    public UniqueKeyFunction(Expression[] params) {
      super("uniquekey", params);

      // The parameter is the name of the table you want to bring the unique
      // key in from.
      if (parameterCount() != 1) {
        throw new Error("'uniquekey' function must have only 1 argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      // NOTE: Unique key is unique to the function object that created it.
      if (unique_key != null) {
        return unique_key;
      }
      else {
        String str =
               getParameter(0).evaluate(group, resolver, context).toString();

        return new BigDecimal(context.nextUniqueID(str));
      }


    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return BigDecimal.class;
    }

  }

  // --

  private static class HexToBinaryFunction extends AbstractFunction {

    public HexToBinaryFunction(Expression[] params) {
      super("hextobinary", params);

      // One parameter - our hex string.
      if (parameterCount() != 1) {
        throw new Error("'hextobinary' function must have only 1 argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      String str =
              getParameter(0).evaluate(group, resolver, context).toString();

      int str_len = str.length();
      if (str_len == 0) {
        return new ByteLongObject(new byte[0]);
      }
      // We translate the string to a byte array,
      byte[] buf = new byte[(str_len + 1) / 2];
      int index = 0;
      if (buf.length * 2 != str_len) {
        buf[0] = (byte) Character.digit(str.charAt(0), 16);
        ++index;
      }
      int v = 0;
      for (int i = index; i < str_len; i += 2) {
        v = (Character.digit(str.charAt(i), 16) << 4) |
            (Character.digit(str.charAt(i + 1), 16));
        buf[index] = (byte) (v & 0x0FF);
        ++index;
      }

      return new ByteLongObject(buf);
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return ByteLongObject.class;
    }

  }

  // --

  private static class BinaryToHexFunction extends AbstractFunction {

    final static char[] digits = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
      'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
      'u', 'v', 'w', 'x', 'y', 'z'
    };

    public BinaryToHexFunction(Expression[] params) {
      super("binarytohex", params);

      // One parameter - our hex string.
      if (parameterCount() != 1) {
        throw new Error("'binarytohex' function must have only 1 argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object ob = getParameter(0).evaluate(group, resolver, context);
      if (ob instanceof NullObject) {
        return null;
      }
      else if (ob instanceof ByteLongObject) {
        StringBuffer buf = new StringBuffer();
        ByteLongObject blob = (ByteLongObject) ob;
        byte[] arr = blob.getByteArray();
        for (int i = 0; i < arr.length; ++i) {
          buf.append(digits[((arr[i] >> 4) & 0x0F)]);
          buf.append(digits[(arr[i] & 0x0F)]);
        }
        return buf.toString();
      }
      else {
        throw new Error(
                      "'binarytohex' parameter type is not a binary object.");
      }

    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return String.class;
    }

  }





  // --

  static class DateObFunction extends AbstractFunction {

    /**
     * The date format object that handles the conversion of Date objects to a
     * string readable representation of the given date.
     * <p>
     * NOTE: Due to bad design these objects are not thread-safe.
     */
    private final static DateFormat date_format_sho;
    private final static DateFormat date_format_sql;
    private final static DateFormat date_format_med;
    private final static DateFormat date_format_lon;
    private final static DateFormat date_format_ful;

    static {
      date_format_med = DateFormat.getDateInstance(DateFormat.MEDIUM);
      date_format_sho = DateFormat.getDateInstance(DateFormat.SHORT);
      date_format_lon = DateFormat.getDateInstance(DateFormat.LONG);
      date_format_ful = DateFormat.getDateInstance(DateFormat.FULL);

      // The SQL date format
      date_format_sql = new SimpleDateFormat("yyyy-MM-dd");
    }

    public DateObFunction(Expression[] params) {
      super("dateob", params);

      if (parameterCount() > 1) {
        throw new Error(
                  "'dateob' function must have only one or zero parameters.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {

      // No parameters so return the current date.
      if (parameterCount() == 0) {
        return new Date();
      }

      Object exp_res = getParameter(0).evaluate(group, resolver, context);
      // If expression resolves to 'null' then return current date
      if (exp_res == null || exp_res instanceof NullObject) {
        return new Date();
      }
      // If expression resolves to a BigDecimal, then treat as number of
      // seconds since midnight Jan 1st, 1970
      else if (exp_res instanceof BigDecimal) {
        return new Date(((BigDecimal) exp_res).longValue());
      }

      String date_str = exp_res.toString();

//      java.util.Date d = new java.util.Date();
//      System.out.println(date_format_med.format(d));
//      System.out.println(date_format_sho.format(d));
//      System.out.println(date_format_lon.format(d));
//      System.out.println(date_format_ful.format(d));

      // We need to synchronize here unfortunately because the Java
      // DateFormat objects are not thread-safe.
      synchronized (date_format_sho) {
        // Try and parse date
        try {
          return date_format_sql.parse(date_str);
        }
        catch (ParseException e) {}
        try {
          return date_format_sho.parse(date_str);
        }
        catch (ParseException e) {}
        try {
          return date_format_med.parse(date_str);
        }
        catch (ParseException e) {}
        try {
          return date_format_lon.parse(date_str);
        }
        catch (ParseException e) {}
        try {
          return date_format_ful.parse(date_str);
        }
        catch (ParseException e) {}

        throw new Error("Unable to parse date string '" + date_str + "'");
//        return NullObject.NULL_OBJ;
      }

    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return Date.class;
    }

  }

  // --

  static class TimeObFunction extends AbstractFunction {

    /**
     * The time format object that handles the conversion of Date objects to a
     * string readable representation of the given time.
     * <p>
     * NOTE: Due to bad design these objects are not thread-safe.
     */
    private final static DateFormat time_format_sql1;
    private final static DateFormat time_format_sql2;

    static {
      // The SQL format
      time_format_sql1 = new SimpleDateFormat("HH:mm:ss.S");
      time_format_sql2 = new SimpleDateFormat("HH:mm:ss");
    }

    public TimeObFunction(Expression[] params) {
      super("timeob", params);

      if (parameterCount() > 1) {
        throw new Error(
                  "'timeob' function must have only one or zero parameters.");
      }
    }

    private Date timeNow() {
      Calendar c = Calendar.getInstance();
      c.setLenient(false);
      int hour = c.get(Calendar.HOUR_OF_DAY);
      int minute = c.get(Calendar.MINUTE);
      int second = c.get(Calendar.SECOND);
      int millisecond = c.get(Calendar.MILLISECOND);

      c.set(1970, 0, 1);
      return c.getTime();
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {

      // No parameters so return the current time.
      if (parameterCount() == 0) {
        return timeNow();
      }

      Object exp_res = getParameter(0).evaluate(group, resolver, context);
      // If expression resolves to 'null' then return current date
      if (exp_res == null || exp_res instanceof NullObject) {
        return timeNow();
      }

      String date_str = exp_res.toString();

      // We need to synchronize here unfortunately because the Java
      // DateFormat objects are not thread-safe.
      synchronized (time_format_sql1) {
        // Try and parse time
        try {
          return time_format_sql1.parse(date_str);
        }
        catch (ParseException e) {}
        try {
          return time_format_sql2.parse(date_str);
        }
        catch (ParseException e) {}

        throw new Error("Unable to parse time string '" + date_str + "'");
      }

    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return Date.class;
    }

  }

  // --

  static class TimeStampObFunction extends AbstractFunction {

    /**
     * The time format object that handles the conversion of Date objects to a
     * string readable representation of the given time.
     * <p>
     * NOTE: Due to bad design these objects are not thread-safe.
     */
    private final static DateFormat ts_format_sql1;
    private final static DateFormat ts_format_sql2;

    static {
      // The SQL format
      ts_format_sql1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S ");
      ts_format_sql2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    public TimeStampObFunction(Expression[] params) {
      super("timestampob", params);

      if (parameterCount() > 1) {
        throw new Error(
             "'timestampob' function must have only one or zero parameters.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {

      // No parameters so return the current time.
      if (parameterCount() == 0) {
        return new Date();
      }

      Object exp_res = getParameter(0).evaluate(group, resolver, context);
      // If expression resolves to 'null' then return current date
      if (exp_res == null || exp_res instanceof NullObject) {
        return new Date();
      }

      String date_str = exp_res.toString();

      // We need to synchronize here unfortunately because the Java
      // DateFormat objects are not thread-safe.
      synchronized (ts_format_sql1) {
        // Try and parse time
        try {
          return ts_format_sql1.parse(date_str);
        }
        catch (ParseException e) {}
        try {
          return ts_format_sql2.parse(date_str);
        }
        catch (ParseException e) {}

        throw new Error("Unable to parse timestamp string '" + date_str + "'");
      }

    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return Date.class;
    }

  }

  // --

  // A function that formats an input java.sql.Date object to the format
  // given using the java.text.SimpleDateFormat class.

  static class DateFormatFunction extends AbstractFunction {

    final static Cache formatter_cache = new Cache(127, 90, 10);

    public DateFormatFunction(Expression[] params) {
      super("dateformat", params);

      if (parameterCount() != 2) {
        throw new Error(
             "'dateformat' function must have exactly two parameters.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {

      Object datein = getParameter(0).evaluate(group, resolver, context);
      Object format = getParameter(1).evaluate(group, resolver, context);
      // If expression resolves to 'null' then return null
      if (datein == null || datein instanceof NullObject) {
        return datein;
      }

      Date d;
      if (!(datein instanceof Date)) {
        throw new Error("Date to format must be DATE, TIME or TIMESTAMP");
      }
      else {
        d = (Date) datein;
      }

      String format_string = format.toString();
      synchronized(formatter_cache) {
        SimpleDateFormat formatter =
                       (SimpleDateFormat) formatter_cache.get(format_string);
        if (formatter == null) {
          formatter = new SimpleDateFormat(format_string);
          formatter_cache.put(format_string, formatter);
        }
        return formatter.format(d);
      }
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return String.class;
    }

  }









  // --

  // Casts the expression to a BigDecimal number.  Useful in conjunction with
  // 'dateob'
  private static class ToNumberFunction extends AbstractFunction {

    public ToNumberFunction(Expression[] params) {
      super("tonumber", params);

      if (parameterCount() != 1) {
        throw new Error("TONUMBER function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      return Operator.toNumber(
                       getParameter(0).evaluate(group, resolver, context));
    }

  }

  // --

  // Conditional - IF(a < 0, NULL, a)
  private static class IfFunction extends AbstractFunction {

    public IfFunction(Expression[] params) {
      super("if", params);
      if (parameterCount() != 3) {
        throw new Error("IF function must have exactly three arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      Object res = getParameter(0).evaluate(group, resolver, context);
      if (res instanceof Boolean) {
        if (res.equals(Boolean.TRUE)) {
          // Resolved to true so evaluate the first argument
          return getParameter(1).evaluate(group, resolver, context);
        }
        else {
          // Resolved to false so evaluate the second argument
          return getParameter(2).evaluate(group, resolver, context);
        }
      }
      // Result was not a boolean so return null
      return NullObject.NULL_OBJ;
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      // It's impossible to know the return type of this function until runtime
      // because either comparator could be returned.  We could assume that
      // both branch expressions result in the same type of object but this
      // currently is not enforced.

      // Returns type of first argument
      Class c = getParameter(1).returnClass(resolver, context);
      // This is a hack for null values.  If the first parameter is null
      // then return the type of the second parameter which hopefully isn't
      // also null.
      if (c.isAssignableFrom(NullObject.class)) {
        return getParameter(2).returnClass(resolver, context);
      }
      return c;
    }

  }

  // --

  // Coalesce - COALESCE(address2, CONCAT(city, ', ', state, '  ', zip))
  private static class CoalesceFunction extends AbstractFunction {

    public CoalesceFunction(Expression[] params) {
      super("coalesce", params);
      if (parameterCount() < 1) {
        throw new Error("COALESCE function must have at least 1 parameter.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      int count = parameterCount();
      for (int i = 0; i < count - 1; ++i) {
        Object res = getParameter(i).evaluate(group, resolver, context);
        if (res != null && !(res instanceof NullObject)) {
          return res;
        }
      }
      return getParameter(count - 1).evaluate(group, resolver, context);
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      // It's impossible to know the return type of this function until runtime
      // because either comparator could be returned.  We could assume that
      // both branch expressions result in the same type of object but this
      // currently is not enforced.

      // Go through each argument until we find the first parameter we can
      // deduce the class of.
      int count = parameterCount();
      for (int i = 0; i < count; ++i) {
        Class c = getParameter(i).returnClass(resolver, context);
        if (!c.isAssignableFrom(NullObject.class)) {
          return c;
        }
      }
      // Can't work it out so return null object class
      return NullObject.class;
    }

  }





  // --

  // Instantiates a new java object.
  private static class JavaObjectInstantiation extends AbstractFunction {

    public JavaObjectInstantiation(Expression[] params) {
      super("_new_JavaObject", params);

      if (parameterCount() < 1) {
        throw new Error("_new_JavaObject function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      // Resolve the parameters...
      final int arg_len = parameterCount() - 1;
      Object[] args = new Object[arg_len];
      for (int i = 0; i < args.length; ++i) {
        args[i] = getParameter(i + 1).evaluate(group, resolver, context);
      }
      Object[] casted_args = new Object[arg_len];

      try {
        String clazz =
                 (String) getParameter(0).evaluate(null, resolver, context);
        Class c = Class.forName(clazz);

        Constructor[] constructs = c.getConstructors();
        // Search for the first constructor that we can use with the given
        // arguments.
search_constructs:
        for (int i = 0; i < constructs.length; ++i) {
          Class[] construct_args = constructs[i].getParameterTypes();
          if (construct_args.length == arg_len) {
            for (int n = 0; n < arg_len; ++n) {
              // If we are dealing with a primitive,
              if (construct_args[n].isPrimitive()) {
                String class_name = construct_args[n].getName();
                // If the given argument is a number,
                if (args[n] instanceof Number) {
                  Number num = (Number) args[n];
                  if (class_name.equals("byte")) {
                    casted_args[n] = new Byte(num.byteValue());
                  }
                  else if (class_name.equals("char")) {
                    casted_args[n] = new Character((char) num.intValue());
                  }
                  else if (class_name.equals("double")) {
                    casted_args[n] = new Double(num.doubleValue());
                  }
                  else if (class_name.equals("float")) {
                    casted_args[n] = new Float(num.floatValue());
                  }
                  else if (class_name.equals("int")) {
                    casted_args[n] = new Integer(num.intValue());
                  }
                  else if (class_name.equals("long")) {
                    casted_args[n] = new Long(num.longValue());
                  }
                  else if (class_name.equals("short")) {
                    casted_args[n] = new Short(num.shortValue());
                  }
                  else {
                    // Can't cast the primitive type to a number so break,
                    break search_constructs;
                  }

                }
                // If we are a boolean, we can cast to primitive boolean
                else if (args[n] instanceof Boolean) {
                  // If primitive type constructor arg is a boolean also
                  if (class_name.equals("boolean")) {
                    casted_args[n] = args[n];
                  }
                  else {
                    break search_constructs;
                  }
                }
                // Otherwise we can't cast,
                else {
                  break search_constructs;
                }

              }
              // Not a primitive type constructor arg,
              else {
                // PENDING: Allow string -> char conversion
                if (construct_args[n].isInstance(args[n])) {
                  casted_args[n] = args[n];
                }
                else {
                  break search_constructs;
                }
              }
            }  // for (int n = 0; n < arg_len; ++n)
            // If we get here, we have a match...
            Object ob = constructs[i].newInstance(casted_args);
            return ob;
          }
        }

        throw new Error(
                "Unable to find a constructor for '" + clazz +
                "' that matches given arguments.");

      }
      catch (ClassNotFoundException e) {
        throw new RuntimeException("Class not found: " + e.getMessage());
      }
      catch (InstantiationException e) {
        throw new RuntimeException("Instantiation Error: " + e.getMessage());
      }
      catch (IllegalAccessException e) {
        throw new RuntimeException("Illegal Access Error: " + e.getMessage());
      }
      catch (IllegalArgumentException e) {
        throw new RuntimeException(
                                 "Illegal Argument Error: " + e.getMessage());
      }
      catch (InvocationTargetException e) {
        throw new RuntimeException(
                                "Invocation Target Error: " + e.getMessage());
      }

    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      try {
        String clazz =
                 (String) getParameter(0).evaluate(null, resolver, context);
        return Class.forName(clazz);
      }
      catch (ClassNotFoundException e) {
        throw new RuntimeException("Class not found: " + e.getMessage());
      }
    }

  }

  // --

  // Used in the 'getxxxKeys' methods in DatabaseMetaData to convert the
  // update delete rule of a foreign key to the JDBC short enum.
  private static class ForeignRuleConvert extends AbstractFunction {

    public ForeignRuleConvert(Expression[] params) {
      super("i_frule_convert", params);

      if (parameterCount() != 1) {
        throw new Error("i_frule_convert function must have one argument.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      // The parameter should be a variable reference that is resolved
      Object ob = getParameter(0).evaluate(group, resolver, context);
      String str = null;
      if (!(ob instanceof NullObject)) {
        str = ob.toString();
      }
      int v;
      if (str == null || str.equals("") || str.equals("NO ACTION")) {
        v = java.sql.DatabaseMetaData.importedKeyNoAction;
      }
      else if (str.equals("CASCADE")) {
        v = java.sql.DatabaseMetaData.importedKeyCascade;
      }
      else if (str.equals("SET NULL")) {
        v = java.sql.DatabaseMetaData.importedKeySetNull;
      }
      else if (str.equals("SET DEFAULT")) {
        v = java.sql.DatabaseMetaData.importedKeySetDefault;
      }
      else if (str.equals("RESTRICT")) {
        v = java.sql.DatabaseMetaData.importedKeyRestrict;
      }
      else {
        throw new Error("Unrecognised foreign key rule: " + str);
      }
      // Return the correct enumeration
      return new BigDecimal(v);
    }

  }

  // --

  // Used to form an SQL type string that describes the SQL type and any
  // size/scale information together with it.
  private static class SQLTypeString extends AbstractFunction {

    public SQLTypeString(Expression[] params) {
      super("i_sql_type", params);

      if (parameterCount() != 3) {
        throw new Error("i_sql_type function must have three arguments.");
      }
    }

    public Object evaluate(GroupResolver group, VariableResolver resolver,
                           QueryContext context) {
      // The parameter should be a variable reference that is resolved
      Object type_string = getParameter(0).evaluate(group, resolver, context);
      Object type_size = getParameter(1).evaluate(group, resolver, context);
      Object type_scale = getParameter(2).evaluate(group, resolver, context);

      StringBuffer result_str = new StringBuffer();
      result_str.append(type_string.toString());
      long size = -1;
      long scale = -1;
      if (!(type_size instanceof NullObject)) {
        size = Operator.toNumber(type_size).longValue();
      }
      if (!(type_scale instanceof NullObject)) {
        scale = Operator.toNumber(type_scale).longValue();
      }

      if (size != -1) {
        result_str.append('(');
        result_str.append(size);
        if (scale != -1) {
          result_str.append(',');
          result_str.append(scale);
        }
        result_str.append(')');
      }

      return new String(result_str);
    }

    public Class returnClass(VariableResolver resolver, QueryContext context) {
      return String.class;
    }

  }




}
