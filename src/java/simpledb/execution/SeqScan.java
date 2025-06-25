package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.transaction.TransactionId;
import simpledb.common.DbException;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator iterator;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.iterator = null;
    }

    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    public String getAlias() {
        return tableAlias;
    }

    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        iterator.open();
    }

    public TupleDesc getTupleDesc() {
        TupleDesc originalDesc = Database.getCatalog().getTupleDesc(tableid);
        Type[] types = new Type[originalDesc.numFields()];
        String[] fieldNames = new String[originalDesc.numFields()];

        for (int i = 0; i < originalDesc.numFields(); i++) {
            types[i] = originalDesc.getFieldType(i);
            String fieldName = originalDesc.getFieldName(i);
            fieldNames[i] = tableAlias + "." + fieldName;
        }

        return new TupleDesc(types, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (iterator == null) return false;
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (iterator == null) throw new NoSuchElementException();
        return iterator.next();
    }

    public void close() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        close();
        open();
    }
}
