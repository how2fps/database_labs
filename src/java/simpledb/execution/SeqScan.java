package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
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
    private int tableId;
    private String tableAlias;
    private DbFileIterator iterator;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    public String getAlias() {
        return tableAlias;
    }

    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
    }

    public TupleDesc getTupleDesc() {
        TupleDesc original = Database.getCatalog().getTupleDesc(tableId);
        int numFields = original.numFields();
        Type[] types = new Type[numFields];
        String[] names = new String[numFields];

        for (int i = 0; i < numFields; i++) {
            types[i] = original.getFieldType(i);
            names[i] = tableAlias + "." + original.getFieldName(i);
        }

        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        return iterator.next();
    }

    public void close() {
        iterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
    }
}

