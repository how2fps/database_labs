package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Aggregation operator that computes an aggregate over a set of tuples.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;

    private Aggregator aggregator;
    private OpIterator aggIter;

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type afieldType = child.getTupleDesc().getFieldType(afield);
        Type gfieldType = (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldType(gfield);

        if (afieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, gfieldType, afield, aop);
        } else if (afieldType == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(gfield, gfieldType, afield, aop);
        } else {
            throw new IllegalArgumentException("Unsupported aggregate field type.");
        }
    }

    public int groupField() {
        return gfield;
    }

    public String groupFieldName() {
        return (gfield == Aggregator.NO_GROUPING)
            ? null
            : child.getTupleDesc().getFieldName(gfield);
    }

    public int aggregateField() {
        return afield;
    }

    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();

        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }

        aggIter = aggregator.iterator();
        aggIter.open();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (aggIter != null && aggIter.hasNext()) {
            return aggIter.next();
        }
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        aggIter.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        TupleDesc childTD = child.getTupleDesc();
        String aggName = String.format("%s(%s)", aop.toString(), childTD.getFieldName(afield));

        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggName});
        } else {
            Type[] types = new Type[]{childTD.getFieldType(gfield), Type.INT_TYPE};
            String[] names = new String[]{childTD.getFieldName(gfield), aggName};
            return new TupleDesc(types, names);
        }
    }

    @Override
    public void close() {
        super.close();
        child.close();
        if (aggIter != null) aggIter.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length > 0) {
            this.child = children[0];
        }
    }
}
