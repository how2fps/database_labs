package simpledb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
       private final File f;
       private final TupleDesc td;

       /**
        * Constructs a heap file backed by the specified file.
        * 
        * @param f
        *          the file that stores the on-disk backing store for this heap
        *          file.
        */
       public HeapFile(File f, TupleDesc td) {
              this.f = f;
              this.td = td;
       }

       /**
        * Returns the File backing this HeapFile on disk.
        * 
        * @return the File backing this HeapFile on disk.
        */
       public File getFile() {
              return f;
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
              return f.getAbsoluteFile().hashCode();
       }

       /**
        * Returns the TupleDesc of the table stored in this DbFile.
        * 
        * @return TupleDesc of this DbFile.
        */
       public TupleDesc getTupleDesc() {
              return td;
       }

       // see DbFile.java for javadocs
       public Page readPage(PageId pid) {
              int pageSize = BufferPool.getPageSize();
              int pageNumber = pid.getPageNumber();
              int offset = pageNumber * pageSize;
              byte[] data = new byte[pageSize];
              try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
                     raf.seek(offset);
                     raf.readFully(data);
              } catch (IOException e) {
                     throw new RuntimeException("Failed to read page from disk", e);
              }
              try {
                     return new HeapPage((HeapPageId) pid, data);
              } catch (IOException e) {
                     throw new RuntimeException("Failed to create heap page from data", e);
              }
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
              long fileSize = f.length();
              if (fileSize == 0) {
                     return 0;
              }
              int pageSize = BufferPool.getPageSize();
              int numberOfPages = (int) Math.ceil((double) fileSize / pageSize);
              return numberOfPages;
       }

       // see DbFile.java for javadocs
       public List<Page> insertTuple(TransactionId tid, Tuple t)
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

       // see DbFile.java for javadocs
       public DbFileIterator iterator(TransactionId tid) {
              return new DbFileIterator() {
                     private int pageNo = 0;
                     private Iterator<Tuple> tupleIterator = null;

                     @Override
                     public void open() {
                            tupleIterator = getTupleIterator(0);
                            advanceToNextNonEmptyPage();
                     }

                     @Override
                     public boolean hasNext() {
                            return tupleIterator != null && tupleIterator.hasNext();
                     }

                     @Override
                     public Tuple next() {
                            if (!hasNext()) {
                                   throw new NoSuchElementException();
                            }
                            Tuple t = tupleIterator.next();
                            if (!tupleIterator.hasNext() || tupleIterator == null) {
                                   advanceToNextNonEmptyPage();
                            }
                            return t;
                     }

                     @Override
                     public void rewind() {
                            open();
                     }

                     @Override
                     public void close() {
                            tupleIterator = null;
                     }

                     private Iterator<Tuple> getTupleIterator(int pageNo) {
                            if (pageNo >= numPages()) {
                                   return null;
                            }
                            HeapPageId pid = new HeapPageId(getId(), pageNo);
                            try {
                                   HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                                                 tid, pid, Permissions.READ_ONLY);
                                   return page.iterator();
                            } catch (TransactionAbortedException e) {
                                   return null;
                            } catch (DbException e) {
                                   return null;
                            }
                     }

                     private void advanceToNextNonEmptyPage() {
                            while (tupleIterator != null && !tupleIterator.hasNext() && pageNo < numPages()) {
                                   pageNo = pageNo + 1;
                                   tupleIterator = getTupleIterator(pageNo);
                            }
                     }
              };

       }

}
