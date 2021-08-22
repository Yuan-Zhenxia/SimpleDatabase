package simpleDatabase.operator.join;

import java.util.Vector;

/** Class returned by  specifying the
    cost and cardinality of the optimal plan represented by plan.
*/
public class CostCard {
    /** The cost of the optimal subplan */
    public double cost;
    /** The cardinality of the optimal subplan */
    public int card;
    /** The optimal subplan */
    public Vector<LogicalJoinNode> plan;
}
