package simpleDatabase.operator;

import simpleDatabase.iterator.OpIterator;


/**
 * Not totally finished on 6 Aug.
 *
 * A LogicalSubplanJoinNode represens the state needed of a join of a
 * table to a subplan in a LogicalQueryPlan -- inherits state from
 * {@link LogicalJoinNode}; t2 and f2 should always be null
 */
public class LogicalSubplanJoinNode extends LogicalJoinNode {
    
    /** The subplan (used on the inner) of the join */
    /** inner join select their common elements */
    OpIterator subPlan;
    
    public LogicalSubplanJoinNode(String table1, String joinField1, OpIterator sp, Predicate.Op pred) {
        t1Alias = table1;
        String[] tmps = joinField1.split("[.]");
        if (tmps.length>1) f1PureName = tmps[tmps.length-1];
        else f1PureName = joinField1;
        f1QuantifiedName = t1Alias + "." + f1PureName;
        subPlan = sp; //
        op = pred;
    }
    
    @Override public int hashCode() {
        return t1Alias.hashCode() + f1PureName.hashCode() + subPlan.hashCode();
    }
    
    @Override public boolean equals(Object o) {
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        if (!(o instanceof LogicalSubplanJoinNode)) return false;
        return (j2.t1Alias.equals(t1Alias)  && j2.f1PureName.equals(f1PureName) && ((LogicalSubplanJoinNode)o).subPlan.equals(subPlan));
    }

    // TODO 我猜测 晚点再看
    public LogicalSubplanJoinNode swapInnerOuter() {
        LogicalSubplanJoinNode j2 = new LogicalSubplanJoinNode(t1Alias, f1PureName, subPlan, op);
        return j2;
    }

}
