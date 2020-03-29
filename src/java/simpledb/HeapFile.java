package simpledb;

import com.sun.security.auth.UnixNumericGroupPrincipal;
import junit.framework.Assert;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */
    File fileOndisk;
    TupleDesc tableTd;
    int nowPage;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        fileOndisk = f;
        tableTd = td;
        nowPage = -1;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return fileOndisk;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return fileOndisk.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return tableTd;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid == null || pid.getPageNumber() < 0 || pid.getPageNumber() >= numPages() || pid.getTableId() != getId()) {
            throw new IllegalArgumentException();
        }
        try {
            byte[] data;
            FileInputStream ss = new FileInputStream(fileOndisk);
            ss.skipNBytes(BufferPool.getPageSize() * pid.getPageNumber());
            data = ss.readNBytes(BufferPool.getPageSize());
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException ignored) {

        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (fileOndisk.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (t == null) {
            throw new IOException();
        }

        for (int i = 0; i < numPages(); i++) {
            HeapPage tmp = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), null));
            if (tmp.getNumEmptySlots() != 0) {
                tmp.insertTuple(t);
                tmp.markDirty(true, tid);
                ArrayList<Page> output = new ArrayList<>();
                output.add(tmp);
                return output;
            }
        }
        HeapPage p = new HeapPage(new HeapPageId(getId(), numPages()),
                HeapPage.createEmptyPageData());
        p.insertTuple(t);
        p.markDirty(false, tid);
        byte[] bt = p.getPageData();
        FileOutputStream bw = new FileOutputStream(fileOndisk, true);
        bw.write(bt);
        bw.close();
        ArrayList<Page> output = new ArrayList<>();
        output.add(p);
        return output;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (t == null
                || t.getRecordId().getPageId().getTableId() != getId()
                || t.getRecordId().getPageId().getPageNumber() < 0
                || t.getRecordId().getPageId().getPageNumber() >= numPages())
            throw new DbException("he tuple cannot be deleted or is not a member of the file");
        HeapPage tmp = ((HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null));
        tmp.deleteTuple(t);
        tmp.markDirty(true, tid);
        ArrayList<Page> output = new ArrayList<>();
        output.add(tmp);
        return output;
    }

    class TupleListIterator implements DbFileIterator {
        private List<Tuple> tuples;
        private boolean isOpen;
        Iterator<Tuple> now;

        public TupleListIterator(PageId pid) {
            nowPage = 0;
            tuples = getNextVailPage();
            isOpen = false;
            now = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            now = tuples.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen)
                return false;
            if (now.hasNext())
                return true;
            if(nowPage>=numPages()-1)
                return false;
            nowPage++;
            return getNextVailPage().size() != 0;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen || now == null ) {
                throw new NoSuchElementException();
            }
            if (!now.hasNext()) {
                tuples = getNextVailPage();
                now = tuples.iterator();
            }
            return now.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("not support");
            }
            nowPage = 0;
            tuples = getNextVailPage();
            now = tuples.iterator();
        }

        @Override
        public void close() {
            isOpen = false;
        }

        private ArrayList<Tuple> getNextVailPage() {
            for (int i = nowPage; i < numPages(); i++) {
                ArrayList<Tuple> tmp = loadPageToList(new HeapPageId(getId(), i));
                if (tmp.size() != 0)
                    return tmp;
                nowPage++;
            }
            return new ArrayList<>();
        }
    }

//    private Page getNumPage(PageId pid) {
//        try {
//            byte[] data;
//            FileInputStream ss = new FileInputStream(fileOndisk);
//            ss.skipNBytes(BufferPool.getPageSize() * pid.getPageNumber());
//            data = ss.readNBytes(BufferPool.getPageSize());
//            return new HeapPage((HeapPageId) pid, data);
//        } catch (IOException o) {
//
//        } finally {
//
//        }
//        return null;
//    }

    private ArrayList<Tuple> loadPageToList(PageId pid) {
        try {
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
            Iterator<Tuple> it = p.iterator();
            ArrayList<Tuple> t = new ArrayList<>();
            while (it.hasNext()) {
                t.add(it.next());
            }
            return t;
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        nowPage = 0;
        return new TupleListIterator(new HeapPageId(getId(), nowPage));
    }

}

