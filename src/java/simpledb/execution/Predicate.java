package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;
import java.io.Serializable;

public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            switch (this) {
                case EQUALS: return "=";
                case GREATER_THAN: return ">";
                case LESS_THAN: return "<";
                case LESS_THAN_OR_EQ: return "<=";
                case GREATER_THAN_OR_EQ: return ">=";
                case LIKE: return "LIKE";
                case NOT_EQUALS: return "<>";
                default: throw new IllegalStateException("Unknown operator");
            }
        }
    }

    private final int field;
    private final Op op;
    private final Field operand;

    public Predicate(int field, Op op, Field operand) {
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    public int getField() {
        return field;
    }

    public Op getOp() {
        return op;
    }

    public Field getOperand() {
        return operand;
    }

    public boolean filter(Tuple t) {
        return t.getField(field).compare(op, operand);
    }

    public String toString() {
        return "f = " + field + " op = " + op.toString() + " operand = " + operand.toString();
    }
}
