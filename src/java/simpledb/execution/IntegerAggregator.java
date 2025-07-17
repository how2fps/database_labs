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

    // Maps group key to aggregate value
    private final Map<Field, Integer> aggregateValues = new HashMap<>();
    // Maps group key to count (for AVG)
    private final Map<Field, Integer> groupCounts = new HashMap<>();

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        int value = ((IntField) tup.getField(afield)).getValue();

        int currentVal = aggregateValues.getOrDefault(groupKey, initialize());
        int currentCount = groupCounts.getOrDefault(groupKey, 0);

        switch (what) {
            case SUM:
            case AVG:
                aggregateValues.put(groupKey, currentVal + value);
                break;
            case MIN:
                aggregateValues.put(groupKey, (currentCount == 0) ? value : Math.min(currentVal, value));
                break;
            case MAX:
                aggregateValues.put(groupKey, (currentCount == 0) ? value : Math.max(currentVal, value));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + what);
        }

        groupCounts.put(groupKey, currentCount + 1);
    }

    private int initialize() {
        switch (what) {
            case MIN: return Integer.MAX_VALUE;
            case MAX: return Integer.MIN_VALUE;
            default: return 0;
        }
    }

    public OpIterator iterator() {
        List<Tuple> resultTuples = new ArrayList<>();

        TupleDesc td = (gbfield == NO_GROUPING)
            ? new TupleDesc(new Type[]{Type.INT_TYPE})
            : new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

        for (Field key : aggregateValues.keySet()) {
            int aggregateVal = aggregateValues.get(key);
            if (what == Op.AVG) {
                int count = groupCounts.get(key);
                aggregateVal = aggregateVal / count;
            }

            Tuple t = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(aggregateVal));
            } else {
                t.setField(0, key);
                t.setField(1, new IntField(aggregateVal));
            }
            resultTuples.add(t);
        }

        return new TupleIterator(td, resultTuples);
    }
}
