/**
 * com.mckoi.database.OpenTransactionList  26 Nov 2000
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

import java.util.ArrayList;

/**
 * The list of all currently open transactions.  This is a thread safe
 * object that is shared between a TableDataConglomerate and its children
 * MasterDataTableSource objects.  It is used for maintaining a list of
 * transactions that are currently open in the system.  It also provides
 * various utility methods around the list.
 * <p>
 * This class is thread safe and can safely be accessed by multiple threads.
 * This is so threads accessing table source information as well as
 * conglomerate 'commit' stages can safely access this object.
 *
 * @author Tobias Downer
 */

final class OpenTransactionList {

  /**
   * The system that this transaction list is part of.
   */
  private TransactionSystem system;

  /**
   * The list of open transactions.
   * (Transaction).
   */
  private ArrayList open_transactions;

  /**
   * The minimum commit id of the current list.
   */
  private long minimum_commit_id;

  /**
   * The maximum commit id of the current list.
   */
  private long maximum_commit_id;

  /**
   * Creates the list.
   */
  OpenTransactionList(TransactionSystem system) {
    this.system = system;
    open_transactions = new ArrayList();
    minimum_commit_id = Long.MAX_VALUE;
    maximum_commit_id = 0;
  }

  /**
   * Adds a new open transaction to the list.  Transactions must be added
   * in order of commit_id.
   */
  synchronized void addTransaction(Transaction transaction) {
    long current_commit_id = transaction.getCommitID();
    if (current_commit_id >= maximum_commit_id) {
      open_transactions.add(transaction);
      system.stats().increment("OpenTransactionList.count");
      maximum_commit_id = current_commit_id;
    }
    else {
      throw new Error(
                   "Added a transaction with a lower than maximum commit_id");
    }
  }

  /**
   * Removes an open transaction from the list.
   */
  synchronized void removeTransaction(Transaction transaction) {
    int size = open_transactions.size();
    int i = open_transactions.indexOf(transaction);
    if (i == 0) {
      // First in list.
      if (i == size - 1) {
        // And last.
        minimum_commit_id = Integer.MAX_VALUE;
        maximum_commit_id = 0;
      }
      else {
        minimum_commit_id =
                   ((Transaction) open_transactions.get(i + 1)).getCommitID();
      }
    }
    else if (i == open_transactions.size() - 1) {
      // Last in list.
      maximum_commit_id =
                   ((Transaction) open_transactions.get(i - 1)).getCommitID();
    }
    else if (i == -1) {
      throw new Error("Unable to find transaction in the list.");
    }
    open_transactions.remove(i);
    system.stats().decrement("OpenTransactionList.count");
  }

  /**
   * Returns the number of transactions that are open on the conglomerate.
   */
  synchronized int count() {
    return open_transactions.size();
  }

  /**
   * Returns the minimum commit id discluding the given transaction object.
   * Returns Integer.MAX_VALUE if there are no open transactions in the list
   * (not including the given transaction).
   */
  synchronized long minimumCommitID(Transaction transaction) {
//    if (transaction == null || transaction != null) {
//      return 0;
//    }

    long minimum_commit_id = Long.MAX_VALUE;
    if (open_transactions.size() > 0) {
//      System.out.println("open_transactions.size() = " +
//                                                  open_transactions.size());
      // If the bottom transaction is this transaction, then go to the
      // next up from the bottom (we don't count this transaction as the
      // minimum commit_id).
      Transaction test_transaction = (Transaction)open_transactions.get(0);
      if (test_transaction != transaction) {
        minimum_commit_id = test_transaction.getCommitID();
      }
      else if (open_transactions.size() > 1) {
        minimum_commit_id =
                        ((Transaction) open_transactions.get(1)).getCommitID();
      }
    }

    return minimum_commit_id;

  }

  public synchronized String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[ OpenTransactionList: ");
    for (int i = 0; i < open_transactions.size(); ++i) {
      Transaction t = (Transaction) open_transactions.get(i);
      buf.append(t.getCommitID());
      buf.append(", ");
    }
    buf.append(" ]");
    return new String(buf);
  }

}
