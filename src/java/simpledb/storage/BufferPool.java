package simpledb.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */

class LockManager {
       private Map<PageId, TransactionId> exclusiveLocks = new HashMap<PageId, TransactionId>();
       private Map<PageId, Set<TransactionId>> sharedLocks = new HashMap<PageId, Set<TransactionId>>();
       private Map<TransactionId, Set<PageId>> transactionsWithLocks = new HashMap<TransactionId, Set<PageId>>();
       private Map<TransactionId, Set<TransactionId>> waitForGraph = new HashMap<>();

       public synchronized void acquireLock(TransactionId tid, PageId pid, boolean isExclusiveLock)
                     throws TransactionAbortedException {
              while (true) {
                     if (isExclusiveLock) {
                            if (exclusiveLocks.containsKey(pid)) {
                                   TransactionId holder = exclusiveLocks.get(pid);
                                   if (!holder.equals(tid)) {
                                          addToWaitForGraph(tid, holder);
                                          if (hasDeadlock(tid)) {
                                                 removeFromWaitForGraph(tid);
                                                 throw new TransactionAbortedException();
                                          }
                                          try {
                                                 wait(10);
                                          } catch (InterruptedException e) {
                                                 throw new TransactionAbortedException();
                                          }
                                          continue;
                                   }
                            }
                            
                            if (sharedLocks.containsKey(pid) &&
                                          !(sharedLocks.get(pid).size() == 1 && sharedLocks.get(pid).contains(tid))) {
                                   Set<TransactionId> holders = sharedLocks.get(pid);
                                   for (TransactionId holder : holders) {
                                          if (!holder.equals(tid)) {
                                                 addToWaitForGraph(tid, holder);
                                          }
                                   }
                                   if (hasDeadlock(tid)) {
                                          removeFromWaitForGraph(tid);
                                          throw new TransactionAbortedException();
                                   }
                                   try {
                                          wait(10);
                                   } catch (InterruptedException e) {
                                          throw new TransactionAbortedException();
                                   }
                                   continue;
                            }
                            
                            // Grant the lock
                            exclusiveLocks.put(pid, tid);
                            removeFromWaitForGraph(tid);
                            if (!transactionsWithLocks.containsKey(tid)) {
                                   transactionsWithLocks.put(tid, new HashSet<>());
                            }
                            transactionsWithLocks.get(tid).add(pid);
                            return;
                            
                     } else { // Shared lock request
                            if (exclusiveLocks.containsKey(pid) && !exclusiveLocks.get(pid).equals(tid)) {
                                   TransactionId holder = exclusiveLocks.get(pid);
                                   addToWaitForGraph(tid, holder);
                                   if (hasDeadlock(tid)) {
                                          removeFromWaitForGraph(tid);
                                          throw new TransactionAbortedException();
                                   }
                                   try {
                                          wait(10);
                                   } catch (InterruptedException e) {
                                          throw new TransactionAbortedException();
                                   }
                                   continue;
                            }
                            
                            // Grant the shared lock
                            if (!sharedLocks.containsKey(pid)) {
                                   sharedLocks.put(pid, new HashSet<>());
                            }
                            sharedLocks.get(pid).add(tid);
                            removeFromWaitForGraph(tid);
                            if (!transactionsWithLocks.containsKey(tid)) {
                                   transactionsWithLocks.put(tid, new HashSet<>());
                            }
                            transactionsWithLocks.get(tid).add(pid);
                            return;
                     }
              }
       }

       public synchronized void releaseLock(TransactionId tid, PageId pid) {
              if (tid.equals(exclusiveLocks.get(pid))) {
                     exclusiveLocks.remove(pid);
              }
              Set<TransactionId> sharedLock = sharedLocks.get(pid);
              if (sharedLock != null && sharedLock.contains(tid)) {
                     sharedLock.remove(tid);
                     if (sharedLock.isEmpty()) {
                            sharedLocks.remove(pid);
                     }
              }
              Set<PageId> lockedPages = transactionsWithLocks.get(tid);
              if (lockedPages != null && !lockedPages.isEmpty()) {
                     lockedPages.remove(pid);
              }
              if (lockedPages != null && lockedPages.isEmpty()) {
                     transactionsWithLocks.remove(tid);
              }
              removeFromWaitForGraph(tid);
              notifyAll();
       }

       public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
              Set<PageId> lockedPages = transactionsWithLocks.get(tid);
              if (lockedPages != null && !lockedPages.isEmpty()
                            && lockedPages.contains(pid)) {
                     return true;
              }
              return false;
       }

       public synchronized void releaseAllLocks(TransactionId tid) {
              Set<PageId> lockedPages = transactionsWithLocks.get(tid);
              if (lockedPages != null && !lockedPages.isEmpty()) {
                     Set<PageId> lockedPagesCopy = new HashSet<>(lockedPages);
                     for (PageId pid : lockedPagesCopy) {
                            releaseLock(tid, pid);
                     }
              }
       }
       
       private synchronized void addToWaitForGraph(TransactionId waiter, TransactionId holder) {
              waitForGraph.putIfAbsent(waiter, new HashSet<>());
              waitForGraph.get(waiter).add(holder);
       }
       
       private synchronized void removeFromWaitForGraph(TransactionId tid) {
              waitForGraph.remove(tid);
              for (Set<TransactionId> edges : waitForGraph.values()) {
                     edges.remove(tid);
              }
       }
       
       private synchronized boolean hasDeadlock(TransactionId start) {
              Set<TransactionId> visited = new HashSet<>();
              Set<TransactionId> recStack = new HashSet<>();
              return detectCycle(start, visited, recStack);
       }
       
       private boolean detectCycle(TransactionId current, Set<TransactionId> visited, 
                                 Set<TransactionId> recStack) {
              if (!visited.contains(current)) {
                     visited.add(current);
                     recStack.add(current);
                     
                     Set<TransactionId> waitingFor = waitForGraph.get(current);
                     if (waitingFor != null) {
                            for (TransactionId next : waitingFor) {
                                   if (!visited.contains(next) && detectCycle(next, visited, recStack)) {
                                         return true;
                                   } else if (recStack.contains(next)) {
                                         return true;
                                   }
                            }
                     }
              }
              recStack.remove(current);
              return false;
       }
}

public class BufferPool {
       /** Bytes per page, including header. */
       private static final int DEFAULT_PAGE_SIZE = 4096;

       private static int pageSize = DEFAULT_PAGE_SIZE;

       /**
        * Default number of pages passed to the constructor. This is used by
        * other classes. BufferPool should use the numPages argument to the
        * constructor instead.3333
        */
       public static final int DEFAULT_PAGES = 50;

       /**
        * Creates a BufferPool that caches up to numPages pages.
        *
        * @param numPages maximum number of pages in this buffer pool.
        */
       public BufferPool(int numPages) {
              this.maxPages = numPages;
              // use linked hashmap for lru
              this.pageCache = new LinkedHashMap<PageId, Page>(numPages, 0.75f, true) {
                     @Override
                     protected boolean removeEldestEntry(Map.Entry<PageId, Page> eldest) {
                            return false;
                     }
              };
       }

       public static int getPageSize() {
              return pageSize;
       }

       // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
       public static void setPageSize(int pageSize) {
              BufferPool.pageSize = pageSize;
       }

       // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
       public static void resetPageSize() {
              BufferPool.pageSize = DEFAULT_PAGE_SIZE;
       }

       /**
        * Retrieve the specified page with the associated permissions.
        * Will acquire a lock and may block if that lock is held by another
        * transaction.
        * <p>
        * The retrieved page should be looked up in the buffer pool. If it
        * is present, it should be returned. If it is not present, it should
        * be added to the buffer pool and returned. If there is insufficient
        * space in the buffer pool, a page should be evicted and the new page
        * should be added in its place.
        *
        * @param tid  the ID of the transaction requesting the page
        * @param pid  the ID of the requested page
        * @param perm the requested permissions on the page
        */
       private final int maxPages;
       private final LinkedHashMap<PageId, Page> pageCache;
       private final LockManager lockManager = new LockManager();

       public Page getPage(TransactionId tid, PageId pid, Permissions perm)
                     throws TransactionAbortedException, DbException {
              if (perm == Permissions.READ_WRITE) {
                     lockManager.acquireLock(tid, pid, true);
              }
              if (perm == Permissions.READ_ONLY) {
                     lockManager.acquireLock(tid, pid, false);
              }
              synchronized (this) {
                     if (pageCache.containsKey(pid)) {
                            return pageCache.get(pid);
                     }

                     if (pageCache.size() >= maxPages) {
                            evictPage();
                     }

                     DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
                     Page p = f.readPage(pid);
                     pageCache.put(pid, p);
                     return p;
              }
       }

       /**
        * Releases the lock on a page.
        * Calling this is very risky, and may result in wrong behavior. Think hard
        * about who needs to call this and why, and why they can run the risk of
        * calling it.
        *
        * @param tid the ID of the transaction requesting the unlock
        * @param pid the ID of the page to unlock
        */
       public void unsafeReleasePage(TransactionId tid, PageId pid) {
              lockManager.releaseLock(tid, pid);
       }

       /**
        * Release all locks associated with a given transaction.
        *
        * @param tid the ID of the transaction requesting the unlock
        */
       public void transactionComplete(TransactionId tid) {
              // some code goes here
              // not necessary for lab1|lab2
              
              // Always commit when no explicit commit flag is provided
              transactionComplete(tid, true);
       }

       /** Return true if the specified transaction has a lock on the specified page */
       public boolean holdsLock(TransactionId tid, PageId p) {
              return lockManager.holdsLock(tid, p);
       }

       /**
        * Commit or abort a given transaction; release all locks associated to
        * the transaction.
        *
        * @param tid    the ID of the transaction requesting the unlock
        * @param commit a flag indicating whether we should commit or abort
        */
       public void transactionComplete(TransactionId tid, boolean commit) {
              synchronized (this) {
                     try {
                            if (commit) {
                                   // Flush all pages modified by this transaction
                                   flushPages(tid);
                            } else {
                                   // Abort: restore dirty pages from disk
                                   PageId[] pageIds = pageCache.keySet().toArray(new PageId[pageCache.size()]);
                                   for (PageId pid : pageIds) {
                                          Page page = pageCache.get(pid);
                                          if (page != null && tid.equals(page.isDirty())) {
                                                 // Remove the dirty page
                                                 pageCache.remove(pid);
                                                 // Restore clean version from disk
                                                 DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                                                 Page cleanPage = dbFile.readPage(pid);
                                                 pageCache.put(pid, cleanPage);
                                          }
                                   }
                            }
                     } catch (IOException e) {
                            throw new RuntimeException(e);
                     }
              }
              // Release all locks after commit/abort
              lockManager.releaseAllLocks(tid);
       }

       /**
        * Add a tuple to the specified table on behalf of transaction tid. Will
        * acquire a write lock on the page the tuple is added to and any other
        * pages that are updated (Lock acquisition is not needed for lab2).
        * May block if the lock(s) cannot be acquired.
        * 
        * Marks any pages that were dirtied by the operation as dirty by calling
        * their markDirty bit, and adds versions of any pages that have
        * been dirtied to the cache (replacing any existing versions of those pages) so
        * that future requests see up-to-date pages.
        *
        * @param tid     the transaction adding the tuple
        * @param tableId the table to add the tuple to
        * @param t       the tuple to add
        */
       public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
    List<Page> pages = dbFile.insertTuple(tid, t);
    for (Page page : pages) {
        // First check if we need to evict
        if (pageCache.size() >= maxPages) {
            evictPage();
        }
        page.markDirty(true, tid);
        pageCache.put(page.getId(), page);
    }
}

       /**
        * Remove the specified tuple from the buffer pool.
        * Will acquire a write lock on the page the tuple is removed from and any
        * other pages that are updated. May block if the lock(s) cannot be acquired.
        *
        * Marks any pages that were dirtied by the operation as dirty by calling
        * their markDirty bit, and adds versions of any pages that have
        * been dirtied to the cache (replacing any existing versions of those pages) so
        * that future requests see up-to-date pages.
        *
        * @param tid the transaction deleting the tuple.
        * @param t   the tuple to delete
        */
       public void deleteTuple(TransactionId tid, Tuple t)
                     throws DbException, IOException, TransactionAbortedException {
              int tableId = t.getRecordId().getPageId().getTableId();
              DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
              List<Page> pages = dbFile.deleteTuple(tid, t);
              for (Page page : pages) {
                     page.markDirty(true, tid);
                     pageCache.put(page.getId(), page);
              }
       }

       /**
        * Flush all dirty pages to disk.
        * NB: Be careful using this routine -- it writes dirty data to disk so will
        * break simpledb if running in NO STEAL mode.
        */
       public synchronized void flushAllPages() throws IOException {

              PageId[] pageIds = pageCache.keySet().toArray(new PageId[pageCache.size()]);
              for (PageId pid : pageIds) {
                     flushPage(pid);
              }
       }

       /**
        * Remove the specific page id from the buffer pool.
        * Needed by the recovery manager to ensure that the
        * buffer pool doesn't keep a rolled back page in its
        * cache.
        * 
        * Also used by B+ tree files to ensure that deleted pages
        * are removed from the cache so they can be reused safely
        */
       public synchronized void discardPage(PageId pid) {
              // some code goes here
              // not necessary for lab1

              pageCache.remove(pid);
       }

       /**
        * Flushes a certain page to disk
        * 
        * @param pid an ID indicating the page to flush
        */
       private synchronized void flushPage(PageId pid) throws IOException {
              // some code goes here
              // not necessary for lab1

              Page page = pageCache.get(pid);
              if (page != null && page.isDirty() != null) {
                     DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                     file.writePage(page);
                     page.markDirty(false, null);
              }
       }

       /**
        * Write all pages of the specified transaction to disk.
        */
       public synchronized void flushPages(TransactionId tid) throws IOException {
              PageId[] pageIds = pageCache.keySet().toArray(new PageId[pageCache.size()]);
              for (PageId pid : pageIds) {
                     Page page = pageCache.get(pid);
                     if (page != null && tid.equals(page.isDirty())) {
                            flushPage(pid);
                     }
              }
       }

       /**
        * Discards a page from the buffer pool.
        * Flushes the page to disk to ensure dirty pages are updated on disk.
        */
       private synchronized void evictPage() throws DbException {
              // some code goes here
              // not necessary for lab1

              if (pageCache.isEmpty()) {
                     throw new DbException("empty");
              }
              
              // no steal
              PageId pageToEvict = null;
              PageId[] pageIds = pageCache.keySet().toArray(new PageId[pageCache.size()]);
              for (PageId pid:pageIds){
                     Page page = pageCache.get(pid);
                     if ( page.isDirty()==null) {
                            pageToEvict = pid;
                            break;
                     }
              }
              if (pageToEvict == null) {
                     throw new DbException("All pages in buffer pool are dirty,cannot evict any page ");
              }
              
              pageCache.remove(pageToEvict);
       }

}