package simpleDatabase.operator.join;

import java.util.Vector;

/**
 * 记录了优化的子查询cost和优化的子查询的集合数量，和优化的子查询
 *
 * finished
 * Class returned by  specifying the
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
