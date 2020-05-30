package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private final long TIMEOUT = 150;
    private final boolean abortSelf = true;
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    ConcurrentHashMap<PageId, Page> totPage;
    int numPage;
    ConcurrentHashMap<PageId, Integer> usedTime;
    ConcurrentHashMap<TransactionId, HashSet<PageId>> shared_t_p;
    ConcurrentHashMap<TransactionId, HashSet<PageId>> exclusive_t_p;

    ConcurrentHashMap<PageId, HashSet<TransactionId>> shared_p_t;
    ConcurrentHashMap<PageId, TransactionId> exclusive_p_t;
    HashSet<TransactionId> deadlock;
    int nowTime = 0;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        numPage = numPages;
        totPage = new ConcurrentHashMap<>();
        usedTime = new ConcurrentHashMap<>();
        shared_t_p = new ConcurrentHashMap<>();
        exclusive_t_p = new ConcurrentHashMap<>();
        shared_p_t = new ConcurrentHashMap<>();
        exclusive_p_t = new ConcurrentHashMap<>();
        deadlock = new HashSet<>();

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
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        addLock(tid, pid, perm);
        if (totPage.containsKey(pid)) {
            usedTime.put(pid, ++nowTime);
            return totPage.get(pid);
        }
        try {
            while (totPage.size() >= numPage) {
                evictPage();
            }
            Page needPut = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            usedTime.put(pid, ++nowTime);
            totPage.put(needPut.getId(), needPut);
            return needPut;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

        if (shared_p_t.containsKey(pid)) {
            HashSet<TransactionId> af = new HashSet<>();
            if (shared_p_t.get(pid) != null)
                for (TransactionId i : shared_p_t.get(pid)) {
                    if (!i.equals(tid))
                        af.add(i);
                }
            shared_p_t.put(pid, af);
        }
        exclusive_p_t.remove(pid);
        if (shared_t_p.containsKey(tid)) {
            HashSet<PageId> af = new HashSet<>();
            if (shared_t_p.get(tid) != null)
                for (PageId i : shared_t_p.get(tid)) {
                    if (!i.equals(pid))
                        af.add(i);
                }
            shared_t_p.put(tid, af);
        }
        if (exclusive_t_p.containsKey(tid)) {
            HashSet<PageId> af = new HashSet<>();
            if (exclusive_t_p.get(tid) != null)
                for (PageId i : exclusive_t_p.get(tid)) {
                    if (!i.equals(pid))
                        af.add(i);
                }
            exclusive_t_p.put(tid, af);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    private boolean canWrite(TransactionId tid, PageId pid) {
        if (shared_p_t.get(pid) == null)
            return true;
        for (TransactionId i : shared_p_t.get(pid)) {
            if (!i.equals(tid))
                return false;
        }
        return true;
    }


    private synchronized void addLock(TransactionId tid, PageId pid, Permissions pess) throws TransactionAbortedException {
//        System.out.println(deadlock.size());
//        if (deadlock.containsKey(tid) && System.currentTimeMillis() - deadlock.get(tid) <= TIMEOUT)
//            throw new TransactionAbortedException();
//        deadlock.remove(tid);


        synchronized (this) {
            if (exclusive_p_t.containsKey(pid)) {
                long begintime = System.currentTimeMillis();
                while (!exclusive_p_t.get(pid).equals(tid))
                    if (System.currentTimeMillis() - begintime >= TIMEOUT) {

                        if (!abortSelf && !deadlock.contains(tid)) {
                            throw new TransactionAbortedException();
                        }


                        if (!abortSelf)
                            deadlock.add(tid);
                        else throw new TransactionAbortedException();
                    }
            }
        }
        if (pess == Permissions.READ_ONLY) {
            HashSet<TransactionId> tmp = shared_p_t.get(pid);
            if (tmp == null)
                tmp = new HashSet<>();
            tmp.add(tid);
            shared_p_t.put(pid, tmp);
            HashSet<PageId> tmp2 = shared_t_p.get(tid);
            if (tmp2 == null)
                tmp2 = new HashSet<>();
            tmp2.add(pid);
            shared_t_p.put(tid, tmp2);

        } else {
            synchronized (this) {
                long begintime = System.currentTimeMillis();
                while (!canWrite(tid, pid))
                    if (System.currentTimeMillis() - begintime >= TIMEOUT) {


                        if (!abortSelf && !deadlock.contains(tid)) {
                            throw new TransactionAbortedException();
                        }


                        if (!abortSelf)
                            deadlock.add(tid);
                        else throw new TransactionAbortedException();
                    }
            }
            HashSet<TransactionId> tmp = shared_p_t.get(pid);
            if (tmp == null)
                tmp = new HashSet<>();
            tmp.remove(tid);
            shared_p_t.put(pid, tmp);
            HashSet<PageId> tmp2 = shared_t_p.get(tid);
            if (tmp2 == null)
                tmp2 = new HashSet<>();
            tmp2.remove(pid);
            shared_t_p.put(tid, tmp2);
            exclusive_p_t.put(pid, tid);
            tmp2 = exclusive_t_p.get(tid);
            if (tmp2 == null)
                tmp2 = new HashSet<>();
            tmp2.add(pid);
            exclusive_t_p.put(tid, tmp2);
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        if (exclusive_p_t.containsKey(p) && exclusive_p_t.get(p) == tid)
            return true;
        else if (shared_p_t.containsKey(p)) {
            for (TransactionId tmp : shared_p_t.get(p)) {
                if (tmp.equals(tid))
                    return true;
            }
            return false;
        }
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        deadlock.remove(tid);

        if (commit) {
            flushPages(tid);

        } else {
            if (shared_t_p.containsKey(tid) && shared_t_p.get(tid) != null) {
                for (PageId i : shared_t_p.get(tid))
                    discardPage(i);
            }
            if (exclusive_t_p.containsKey(tid) && exclusive_t_p.get(tid) != null) {
                for (PageId i : exclusive_t_p.get(tid))
                    discardPage(i);
            }
        }
        exclusive_t_p.remove(tid);
        shared_t_p.remove(tid);

        for (HashSet<TransactionId> i : shared_p_t.values()) {
            if (i != null)
                i.remove(tid);
        }
        ArrayList<PageId> tmp = new ArrayList<>();
        for (PageId i : exclusive_p_t.keySet())
            if (exclusive_p_t.get(i) != null && exclusive_p_t.get(i).equals(tid))
                tmp.add(i);
        for (PageId i : tmp)
            exclusive_p_t.remove(i);


//        if (exclusive_t_p.containsKey(tid) && exclusive_t_p.get(tid) != null)
//            for (PageId i : exclusive_t_p.get(tid))
//                releasePage(tid, i);
//                exclusive_p_t.remove(i);
//        exclusive_t_p.remove(tid);

//        if (shared_t_p.containsKey(tid) && shared_t_p.get(tid) != null)
//            for (PageId i : shared_t_p.get(tid))
//                releasePage(tid, i);
//                if (shared_p_t.containsKey(i) && shared_p_t.get(i) != null) {
//                    HashSet<TransactionId> tmp = shared_p_t.get(i);
//                    tmp.remove(tid);
//                    shared_p_t.put(i, tmp);
//
//                }
//        shared_t_p.remove(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> u = file.insertTuple(tid, t);
        for (Page page : u) {
            if (page != null)
                usedTime.put(page.getId(), ++nowTime);
            totPage.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> u = file.deleteTuple(tid, t);
        for (Page page : u) {
            if (page.isDirty() != null) {
                totPage.put(page.getId(), page);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId i : totPage.keySet()) {
            flushPage(i);
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        totPage.remove(pid);
        usedTime.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (pid == null || totPage.get(pid) == null)
            return;
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());

        Page tmp = totPage.get(pid);
        file.writePage(tmp);
        discardPage(pid);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (shared_t_p.containsKey(tid) && shared_t_p.get(tid) != null) {
            for (PageId i : shared_t_p.get(tid))
                flushPage(i);
        }
        if (exclusive_t_p.containsKey(tid) && exclusive_t_p.get(tid) != null) {
            for (PageId i : exclusive_t_p.get(tid))
                flushPage(i);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId now = null;
        for (PageId i : usedTime.keySet()) {
            if ((now == null || usedTime.get(i) < usedTime.get(now)) && totPage.get(i).isDirty() == null)
                now = i;
        }
        try {
            if (now == null)
                throw new DbException("all page is dirty");
            flushPage(now);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
