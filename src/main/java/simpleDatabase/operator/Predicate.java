package simpleDatabase.operator;

import simpleDatabase.field.Field;
import simpleDatabase.cache.Tuple;

import java.io.Serializable;

/**
 * finished
 *
 * 通过调用 field内部的比较方法，来实现代码分离
 *
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private Field operand;

    private Op op;

    private int idx;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
    
    /**
     * Constructor.
     * 
     * @param index
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Predicate(int index, Op op, Field operand) {
        // some code goes here
        this.idx = index;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getIdx() {
        return idx;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand() {
        return operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // compare 方法第一额参数接受比较操作符，第二个参数接受数字
        // 例如如果大于，那么就判断这个field的值是否大于 operand
        return t.getField(idx).compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(tuple x).fields[").append(idx).append("] ")
                .append(op.toString()).append(" ").append(operand).append(" ?");
        return builder.toString();
    }
}
