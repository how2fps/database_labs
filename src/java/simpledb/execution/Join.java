package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private final JoinPredicate predicate;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple outerTuple;

    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return predicate;
    }

    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(predicate.getField1());
    }

    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(predicate.getField2());
    }

    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
        outerTuple = null;
    }

    public void close() {
        super.close();
        child1.close();
        child2.close();
        outerTuple = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        outerTuple = null;
    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (outerTuple != null || child1.hasNext()) {
            if (outerTuple == null) {
                outerTuple = child1.next();
            }

            while (child2.hasNext()) {
                Tuple innerTuple = child2.next();
                if (predicate.filter(outerTuple, innerTuple)) {
                    TupleDesc mergedDesc = getTupleDesc();
                    Tuple joinedTuple = new Tuple(mergedDesc);

                    int outerFields = outerTuple.getTupleDesc().numFields();
                    int innerFields = innerTuple.getTupleDesc().numFields();

                    for (int i = 0; i < outerFields; i++) {
                        joinedTuple.setField(i, outerTuple.getField(i));
                    }
                    for (int i = 0; i < innerFields; i++) {
                        joinedTuple.setField(outerFields + i, innerTuple.getField(i));
                    }

                    return joinedTuple;
                }
            }
            child2.rewind();
            outerTuple = null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child1, child2 };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children != null && children.length >= 2) {
            child1 = children[0];
            child2 = children[1];
        }
    }
}
