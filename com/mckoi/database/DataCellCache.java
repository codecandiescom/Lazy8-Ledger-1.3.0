/**
 * com.mckoi.database.DataCellCache  21 Mar 1998
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

import com.mckoi.util.Cache;
import com.mckoi.debug.*;
import java.util.HashMap;

/**
 * This object represents a cache for accesses to the the data cells within
 * a Table.  Whenever a column/row index to a cell is accessed, the cache is
 * first checked.  If the cell is not in the cache then it may go ahead and
 * read the cell from the file.
 * <p>
 * ISSUE: We may need to keep track of memory used.  Since a StringDataCell may
 * use up much memory, we may need a cap on the maximum size the cache can grow
 * to.  For example, we wouldn't want to cache a large document.  This could
 * be handled at a higher level?
 *
 * @author Tobias Downer
 */

final class DataCellCache {

  /**
   * The TransactionSystem that this cache is from.
   */
  private TransactionSystem system;

  /**
   * The maximum size of a DataCell that is allowed to go in the cache.
   */
  private int MAX_CELL_SIZE;

  /**
   * The actual cache.
   */
  private DCCache cache;

  /**
   * The current size of the cache.
   */
  private long current_cache_size;

  /**
   * The Constructors.
   *
   * @param max_cache_size the maximum size in bytes that the cache is allowed
   *   to grow to (eg. 4000000).
   * @param max_cell_size the maximum size of an object that can be stored in
   *   the cache.
   * @param hash_size the number of elements in the hash (should be a prime
   *   number).
   */
  DataCellCache(TransactionSystem system,
                int max_cache_size, int max_cell_size, int hash_size) {
    this.system = system;
    MAX_CELL_SIZE = max_cell_size;

    cache = new DCCache(hash_size, max_cache_size);
  }

  DataCellCache(TransactionSystem system,
                int max_cache_size, int max_cell_size) {
    this(system,
         max_cache_size, max_cell_size, 88547);  // Good prime number hash size
  }

  /**
   * Dynamically resizes the data cell cache so it can store more/less data.
   * This is used to change cache dynamics at runtime.
   */
  public synchronized void alterCacheDynamics(
                                      int max_cache_size, int max_cell_size) {
    MAX_CELL_SIZE = max_cell_size;
    cache.setCacheSize(max_cache_size);
  }

  /**
   * Inner class that creates an object that hashes nicely over the cache
   * source.
   */
  private final static class DCCacheKey {
    private final int row;
    private final short column;
    private final int table_id;
    DCCacheKey(final int table_id, final short column, final int row) {
      this.table_id = table_id;
      this.column = column;
      this.row = row;
    }
    public boolean equals(Object ob) {
      DCCacheKey dest_key = (DCCacheKey) ob;
      return row == dest_key.row &&
             column == dest_key.column &&
             table_id == dest_key.table_id;
    }
    public int hashCode() {
//      return (((int) column + table_id + ( row * 50021 ))) << 4;
      // Yicks - this one is the best by far!
      return (((int) column + table_id + (row * 189977)) * 50021) << 4;
    }
  }

  /**
   * Returns a key to use for this table/row/cell.
   * <p>
   * NOTE: Current limitation is 65536 columns.
   */
  private final Object generateKey(int table_key, int row, int column) {
    // NOTE: Cache limitations:
    //   number of columns = 65536 columns
    //   number of table keys = 4 billion
    //   number of row keys = 4 billion
    return new DCCacheKey(table_key, (short) column, row);

//    // NOTE: Limitations:
//    //   number of columns = 2^15    = 32768 columns
//    //   number of table keys = 2^12 = 4096 tables
//    //   number of rows = 2^37       = 1.3743e11 rows
//    return new DCCacheKey(((long)row << 27) +
//                          ((long)table_key << 15) + (long)column);
  }

  /**
   * Returns an approximation of the amount of memory taken by a DataCell.
   */
  private final int amountMemory(DataCell cell) {
    return 16 + cell.currentSizeOf();
  }

  /**
   * Puts a DataCell on the cache for the given row/column of the table.
   * Ignores any cells that are larger than the maximum size.
   */
  public synchronized void put(int table_key, int row, int column,
                               DataCell cell) {
    int memory_use = amountMemory(cell);
    if (memory_use <= MAX_CELL_SIZE) {
      cache.put(generateKey(table_key, row, column), cell);
      current_cache_size += memory_use;
    }
  }

  /**
   * Gets a DataCell from the cache.  If the row/column is not in the cache
   * then it returns null.
   */
  public synchronized DataCell get(int table_key, int row, int column) {
    return (DataCell) cache.get(generateKey(table_key, row, column));
  }

  /**
   * Removes a DataCell from the cache.  This is used when we need to notify
   * the cache that an object has become outdated.  This should be used when
   * the cell has been removed or changed.
   * Returns the cell that was removed, or null if there was no cell at the
   * given location.
   */
  public synchronized DataCell remove(int table_key, int row, int column) {
    DataCell cell =
              (DataCell) cache.remove(generateKey(table_key, row, column));

    if (cell != null) {
      current_cache_size -= amountMemory(cell);
    }
    return cell;
  }

  /**
   * Completely wipe the cache of all entries.
   */
  public synchronized void wipe() {
    if (cache.nodeCount() == 0 && current_cache_size != 0) {
      system.Debug().write(Lvl.ERROR, this,
          "Assertion failed - if nodeCount = 0 then current_cache_size " +
          "must also be 0.");
    }
    if (cache.nodeCount() != 0) {
      cache.removeAll();
      system.stats().increment("DataCellCache.total_cache_wipe");
    }
    current_cache_size = 0;
  }

  /**
   * Returns an estimation of the current cache size in bytes.
   */
  public long getCurrentCacheSize() {
    return current_cache_size;
  }

  /**
   * Reduce the cache size by the given amount.
   */
  void reduceCacheSize(long val) {
    current_cache_size -= val;
  }

  // ---------- Primes ----------

  /**
   * Returns a prime number from PRIME_LIST that is the closest prime greater
   * or equal to the given value.
   */
  static int closestPrime(int value) {
    for (int i = 0; i < PRIME_LIST.length; ++i) {
      if (PRIME_LIST[i] >= value) {
        return PRIME_LIST[i];
      }
    }
    // Return the last prime
    return PRIME_LIST[PRIME_LIST.length - 1];
  }

  /**
   * A list of primes ordered from lowest to highest.
   */
  private final static int[] PRIME_LIST = new int[] {
     3001, 4799, 13999, 15377, 21803, 24247, 35083, 40531, 43669, 44263, 47387,
     50377, 57059, 57773, 59399, 59999, 75913, 96821, 140551, 149011, 175633,
     176389, 183299, 205507, 209771, 223099, 240259, 258551, 263909, 270761,
     274679, 286129, 290531, 296269, 298021, 300961, 306407, 327493, 338851,
     351037, 365489, 366811, 376769, 385069, 410623, 430709, 433729, 434509,
     441913, 458531, 464351, 470531, 475207, 479629, 501703, 510709, 516017,
     522211, 528527, 536311, 539723, 557567, 593587, 596209, 597451, 608897,
     611069, 642547, 670511, 677827, 679051, 688477, 696743, 717683, 745931,
     757109, 760813, 763957, 766261, 781559, 785597, 788353, 804493, 813559,
     836917, 854257, 859973, 883217, 884789, 891493, 902281, 910199, 915199,
     930847, 939749, 940483, 958609, 963847, 974887, 983849, 984299, 996211,
     999217, 1007519, 1013329, 1014287, 1032959, 1035829, 1043593, 1046459,
     1076171, 1078109, 1081027, 1090303, 1095613, 1098847, 1114037, 1124429,
     1125017, 1130191, 1159393, 1170311, 1180631, 1198609, 1200809, 1212943,
     1213087, 1226581, 1232851, 1287109, 1289867, 1297123, 1304987, 1318661,
     1331107, 1343161, 1345471, 1377793, 1385117, 1394681, 1410803, 1411987,
     1445261, 1460497, 1463981, 1464391, 1481173, 1488943, 1491547, 1492807,
     1528993, 1539961, 1545001, 1548247, 1549843, 1551001, 1553023, 1571417,
     1579099, 1600259, 1606153, 1606541, 1639751, 1649587, 1657661, 1662653,
     1667051, 1675273, 1678837, 1715537, 1718489, 1726343, 1746281, 1749107,
     1775489, 1781881, 1800157, 1806859, 1809149, 1826753, 1834607, 1846561,
     1849241, 1851991, 1855033, 1879931, 1891133, 1893737, 1899137, 1909513,
     1916599, 1917749, 1918549, 1919347, 1925557, 1946489, 1961551, 1965389,
     2011073, 2033077, 2039761, 2054047, 2060171, 2082503, 2084107, 2095099,
     2096011, 2112193, 2125601, 2144977, 2150831, 2157401, 2170141, 2221829,
     2233019, 2269027, 2270771, 2292449, 2299397, 2303867, 2309891, 2312407,
     2344301, 2348573, 2377007, 2385113, 2386661, 2390051, 2395763, 2422999,
     2448367, 2500529, 2508203, 2509841, 2513677, 2516197, 2518151, 2518177,
     2542091, 2547469, 2549951, 2556991, 2563601, 2575543, 2597629, 2599577,
     2612249, 2620003, 2626363, 2626781, 2636773, 2661557, 2674297, 2691571,
     2718269, 2725691, 2729381, 2772199, 2774953, 2791363, 2792939, 2804293,
     2843021, 2844911, 2851313, 2863519, 2880797, 2891821, 2897731, 2904887,
     2910251, 2928943, 2958341, 2975389
  };

  // ---------- Inner classes ----------

  /**
   * This extends the 'Cache' class.
   */
  private final class DCCache extends Cache {

    /**
     * The maximum size that the cache can grow to in bytes.
     */
    private int MAX_CACHE_SIZE;

    /**
     * The Constructor.
     */
    public DCCache(int cache_hash_size, int max_cache_size) {
      super(cache_hash_size, -1, 20);
//      super(88547, -1, 20);     // cache size is a prime number
//      super(101207, -1, 20);     // cache size is a prime number
//      super(max_cache_size / 32, -1, 20);
      MAX_CACHE_SIZE = max_cache_size;
    }

    /**
     * Used to dynamically alter the size of the cache.  May cause a cache
     * clean if the size is over the limit.
     */
    public void setCacheSize(int cache_size) {
      this.MAX_CACHE_SIZE = cache_size;
      checkClean();
    }

    // ----- Overwritten from Cache -----

//    protected int getHashSize() {
//      // Create a really large hash for this cache.
//      // NOTE: Our hashing algorithm was only tested to work well on 65536 and
//      //   (65536 * 2) size hash tables.
////      return 65536 * 2;
//      return HASH_SIZE;
//    }



    protected void checkClean() {

      if (getCurrentCacheSize() >= MAX_CACHE_SIZE) {

        // Update the current cache size (before we wiped).
        system.stats().set((int) getCurrentCacheSize(),
                             "DataCellCache.current_cache_size");
        clean();

        // The number of times we've cleared away old data cell nodes.
        system.stats().increment("DataCellCache.cache_clean");

      }
    }

    protected boolean shouldWipeMoreNodes() {
      return (getCurrentCacheSize() >= (MAX_CACHE_SIZE * 100) / 115);
    }

    protected void notifyWipingNode(Object ob) {
      super.notifyWipingNode(ob);

      // Update our memory indicator accordingly.
      DataCell cell = (DataCell) ob;
      reduceCacheSize(amountMemory(cell));
    }

    protected void notifyGetWalks(long total_walks, long total_get_ops) {
      int avg = (int) ((total_walks * 1000000L) / total_get_ops);
      system.stats().set(avg, "DataCellCache.avg_hash_get_mul_1000000");
      system.stats().set((int) getCurrentCacheSize(),
                                      "DataCellCache.current_cache_size");
      system.stats().set(nodeCount(), "DataCellCache.current_node_count");
    }

  }

}
