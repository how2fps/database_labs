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
       public Page readPage(PageId pid) throws IllegalArgumentException {
              int pageSize = BufferPool.getPageSize();
              int pageNumber = pid.getPageNumber();
              int offset = pageNumber * pageSize;
              byte[] data = new byte[pageSize];
              try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
                     raf.seek(offset);
                     raf.readFully(data);
              } catch (IOException e) {
                     throw new IllegalArgumentException("Failed", e);
              }
              try {
                     return new HeapPage((HeapPageId) pid, data);
              } catch (IOException e) {
                     throw new IllegalArgumentException("Failed", e);
              }
       }

       // see DbFile.java for javadocs
       public void writePage(Page page) throws IOException {
              int pageSize = BufferPool.getPageSize();
              int pageNumber = page.getId().getPageNumber();
              int offset = pageNumber * pageSize;
              byte[] data = page.getPageData();
              try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                     raf.seek(offset);
                     raf.write(data);
              }
       }

       /**
        * Returns the number of pages in this HeapFile.
        */
       public int numPages() {
              long fileSize = f.length();
              int pageSize = BufferPool.getPageSize();
              int numberOfPages = (int) Math.ceil((double) fileSize / pageSize);
              return numberOfPages;
       }

       // see DbFile.java for javadocs
       public List<Page> insertTuple(TransactionId tid, Tuple t)
                     throws DbException, IOException, TransactionAbortedException {
              List<Page> modifiedPages = new ArrayList<>();
              for (int i = 0; i < numPages(); i++) {
                     HeapPageId pid = new HeapPageId(getId(), i);
                     HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                                   tid, pid, Permissions.READ_WRITE);
                     if (page.getNumEmptySlots() > 0) {
                            page.insertTuple(t);
                            modifiedPages.add(page);
                            return modifiedPages;
                     }
              }
              HeapPageId newPid = new HeapPageId(getId(), numPages());
              HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
              newPage.insertTuple(t);
              writePage(newPage);
              modifiedPages.add(newPage);
              return modifiedPages;
       }

       // see DbFile.java for javadocs
       public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
                     TransactionAbortedException {
              ArrayList<Page> modifiedPages = new ArrayList<>();
              RecordId rid = t.getRecordId();
              if (rid == null) {
                     throw new DbException("No RecordId for tuple");
              }
              PageId pageId = rid.getPageId();
              if (pageId.getTableId() != getId()) {
                     throw new DbException("Tuple does not exist in this file");
              }
              HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                            tid, pageId, Permissions.READ_WRITE);
              page.deleteTuple(t);
              modifiedPages.add(page);
              return modifiedPages;
       }

       // see DbFile.java for javadocs
       public DbFileIterator iterator(TransactionId tid) {
              return new DbFileIterator() {
                     private int pageNo = 0;
                     private Iterator<Tuple> tupleIterator = null;

                     @Override
                     public void open() throws DbException, TransactionAbortedException {
                            pageNo = 0;
                            loadNextPage();
                     }

                     @Override
                     public boolean hasNext() throws DbException, TransactionAbortedException {
                            if (tupleIterator == null) {
                                   return false;
                            }
                            if (tupleIterator.hasNext()) {
                                   return true;
                            }
                            loadNextPage();
                            return tupleIterator != null && tupleIterator.hasNext();
                     }

                     @Override
                     public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                            if (!hasNext()) {
                                   throw new NoSuchElementException();
                            }
                            return tupleIterator.next();
                     }

                     @Override
                     public void rewind() throws DbException, TransactionAbortedException {
                            close();
                            open();
                     }

                     @Override
                     public void close() {
                            tupleIterator = null;
                            pageNo = 0;
                     }

                     private void loadNextPage() throws DbException, TransactionAbortedException {
                            tupleIterator = null;
                            while (pageNo < numPages()) {
                                   HeapPageId pid = new HeapPageId(getId(), pageNo);
                                   HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                                                 tid, pid, Permissions.READ_ONLY);
                                   Iterator<Tuple> pageIterator = page.iterator();
                                   pageNo++;
                                   if (pageIterator.hasNext()) {
                                          tupleIterator = pageIterator;
                                          return;
                                   }
                            }
                            tupleIterator = null;
                     }
              };
       }
}
