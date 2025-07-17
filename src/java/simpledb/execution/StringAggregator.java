package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    // Map to track count per group
    private final Map<Field, Integer> groupCounts;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT)
            throw new IllegalArgumentException("StringAggregator only supports COUNT");

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupCounts = new HashMap<>();
    }

    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        groupCounts.put(groupKey, groupCounts.getOrDefault(groupKey, 0) + 1);
    }

    public OpIterator iterator() {
        List<Tuple> resultTuples = new ArrayList<>();
        TupleDesc td;

        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        for (Map.Entry<Field, Integer> entry : groupCounts.entrySet()) {
            Tuple tup = new Tuple(td);
            IntField countField = new IntField(entry.getValue());

            if (gbfield == NO_GROUPING) {
                tup.setField(0, countField);
            } else {
                tup.setField(0, entry.getKey());
                tup.setField(1, countField);
            }

            resultTuples.add(tup);
        }

        return new TupleIterator(td, resultTuples);
    }
}
