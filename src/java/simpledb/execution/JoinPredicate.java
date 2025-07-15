package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int field1;
    private final int field2;
    private final Predicate.Op op;

    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.field1 = field1;
        this.op = op;
        this.field2 = field2;
    }

    public boolean filter(Tuple t1, Tuple t2) {
        Field f1 = t1.getField(field1);
        Field f2 = t2.getField(field2);
        return f1.compare(op, f2);
    }

    public int getField1() {
        return field1;
    }

    public int getField2() {
        return field2;
    }

    public Predicate.Op getOperator() {
        return op;
    }
}
