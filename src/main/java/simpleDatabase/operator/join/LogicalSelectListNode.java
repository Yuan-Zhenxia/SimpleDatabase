package simpleDatabase.operator.join;

/**
 * finished on 5 Aug.
 *
 * 来表示一个 选择列表中的子句
 * select 操作所需要的field名字，包含group by可能需要的field
 * A LogicalSelectListNode represents a clause in the select list in
 * a LogicalQueryPlan
 */
public class LogicalSelectListNode {
    /** The field name being selected; the name may be (optionally) be
     * qualified with a table name or alias.
     */
    public String fname;

    /** The aggregation operation over the field (if any) */
    public String aggOp;

    public LogicalSelectListNode(String aggOp, String fname) {
        this.aggOp = aggOp;
        this.fname = fname;
    }
}
