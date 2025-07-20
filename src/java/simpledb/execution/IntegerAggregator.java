package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import java.util.*;

/**
 * Computes aggregate functions over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final Map<Field, Integer> aggregates;
    private final Map<Field, Integer> counts;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.aggregates = new HashMap<>();
        this.counts = new HashMap<>();
    }

    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        int value = ((IntField) tup.getField(afield)).getValue();

        int currentVal = aggregates.getOrDefault(groupKey, getInitialValue());
        int currentCount = counts.getOrDefault(groupKey, 0);

        switch (what) {
            case SUM:
            case AVG:
                aggregates.put(groupKey, currentVal + value);
                break;
            case MIN:
                aggregates.put(groupKey, (currentCount == 0) ? value : Math.min(currentVal, value));
                break;
            case MAX:
                aggregates.put(groupKey, (currentCount == 0) ? value : Math.max(currentVal, value));
                break;
            case COUNT:
                aggregates.put(groupKey, currentVal + 1);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + what);
        }

        counts.put(groupKey, currentCount + 1);
    }

    private int getInitialValue() {
        switch (what) {
            case MIN: return Integer.MAX_VALUE;
            case MAX: return Integer.MIN_VALUE;
            case COUNT: return 0;
            default: return 0;
        }
    }

    public OpIterator iterator() {
        List<Tuple> results = new ArrayList<>();
        TupleDesc td;

        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        for (Field key : aggregates.keySet()) {
            int value = aggregates.get(key);
            if (what == Op.AVG) {
                value = value / counts.get(key);
            }

            Tuple t = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(value));
            } else {
                t.setField(0, key);
                t.setField(1, new IntField(value));
            }
            results.add(t);
        }

        return new TupleIterator(td, results);
    }
}
