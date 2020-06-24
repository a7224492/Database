package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes he
        int pageSize = BufferPool.getPageSize();
        if (pid.pageNumber() + 1 > numPages()) {
            return null;
        }

        try {
            FileInputStream fis = new FileInputStream(f);
            fis.skip(pageSize * pid.pageNumber());

            byte[] datas = new byte[pageSize];
            fis.read(datas);

            return new HeapPage(pid, datas);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
        return f.length() / BufferPool.getPageSize() + f.length() % BufferPool.getPageSize() == 0 ? 0 : 1;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapDbFileIterator(this);
    }

    public static class HeapDbFileIterator implements DbFileIterator {
        private HeapFile dbFile;
        private PageId pageId;
        private HeapPage page;
        private Iterator<Tuple> tupleListIter;

        public HeapDbFileIterator(HeapFile dbFile) {
            this.dbFile = dbFile;
            try {
                rewind();
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
        }

        /**
         * open的时候不要把表中所有的数据读出，以免表太大导致内存溢出
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            int numPages = dbFile.numPages();
            if (numPages <= 0) {
                return;
            }

            pageId = new HeapPageId(dbFile.getId(), 0);
            BufferPool bufferPool = Database.getBufferPool();
            page = (HeapPage) bufferPool.getPage(null, pageId, null);
            tupleListIter = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (page == null) {
                return false;
            }

            if (tupleListIter.hasNext()) {
                return true;
            }

            if (pageId.pageNumber() + 1 >= dbFile.numPages()) {
                return false;
            }

            pageId = new HeapPageId(dbFile.getId(), pageId.pageNumber() + 1);
            BufferPool bufferPool = Database.getBufferPool();
            page = (HeapPage) bufferPool.getPage(null, pageId, null);
            tupleListIter = page.iterator();
            return tupleListIter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return tupleListIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (page == null) {
                return;
            }

            BufferPool bufferPool = Database.getBufferPool();
            page = (HeapPage) bufferPool.getPage(null, new HeapPageId(dbFile.getId(), 0), null);
            tupleListIter = page.iterator();
        }

        @Override
        public void close() {
            page = null;
        }
    }

}

