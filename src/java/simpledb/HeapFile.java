package simpledb;

import com.sun.security.auth.UnixNumericGroupPrincipal;
import org.junit.Assert;

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

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        fileOndisk = f;
        tableTd = td;
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


        try {
            byte[] data = new byte[BufferPool.getPageSize()];
            FileInputStream ss = new FileInputStream(fileOndisk);
            ss.read(data);
            HeapPage l = new HeapPage((HeapPageId) pid, data);
            return l;
        } catch (IOException o) {

        } finally {

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
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    static class TupleListIterator implements DbFileIterator {
        private List<Tuple> tuples = new ArrayList<>();
        private boolean isOpen = false;
        Iterator<Tuple> now = null;

        void append(Tuple tmp) {
            tuples.add(tmp);
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            now = tuples.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return isOpen && now.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen || now == null || !now.hasNext()) {
                throw new NoSuchElementException();
            }
            return now.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (tuples.isEmpty() || !isOpen) {
                throw new DbException("not support");
            }
            now = tuples.iterator();
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        HeapPage p = (HeapPage) readPage(new HeapPageId(getId(), numPages()));

        Iterator<Tuple> it = p.iterator();
        TupleListIterator t = new TupleListIterator();
        while (it.hasNext()) {
            t.append(it.next());
        }
        return t;
    }

}

