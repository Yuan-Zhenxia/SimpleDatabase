package simpledb;

/**
 * Finished on 5 Aug.
 *
 * 描述join操作需要的成员变量，操作符 op，jion涉及到的两个column
 *
 * A LogicalJoinNode represens the state needed of a join of two
 * tables in a LogicalQueryPlan */
public class LogicalJoinNode {

    /** The first table to join (may be null). It's the alias of the table (if no alias, the true table name) */
    public String t1Alias;

    /** The second table to join (may be null).  It's the alias of the table, (if no alias, the true table name).*/
    public String t2Alias;

    /** The name of the field in t1 to join with. It's the pure name of a field, rather that alias.field. */
    public String f1PureName; // 不能是别名

    public String f1QuantifiedName;

    /** The name of the field in t2 to join with. It's the pure name of a field.*/
    public String f2PureName;

    public String f2QuantifiedName; // 类似于 Person.salary

    /** The join predicate */
    public Predicate.Op op;

    public LogicalJoinNode() {
    }

    // joinField 类似格式 xxx.xxx.tableName
    public LogicalJoinNode(String table1, String table2, String joinField1, String joinField2, Predicate.Op pred) {
        t1Alias = table1;
        t2Alias = table2;
        String[] tmps = joinField1.split("[.]");
        if (tmps.length > 1) f1PureName = tmps[tmps.length - 1];
        else f1PureName = joinField1;
        tmps = joinField2.split("[.]");
        if (tmps.length > 1) f2PureName = tmps[tmps.length-1];
        else f2PureName = joinField2;
        op = pred;
        this.f1QuantifiedName = t1Alias + "." + this.f1PureName;
        this.f2QuantifiedName = t2Alias + "." + this.f2PureName;
    }

    /**
     * 返回一个和原来相反的 LogicalJoinNode，例如 table1.a > table2.b 反过来就是 table2.b < table1.a
     */
    /** Return a new LogicalJoinNode with the inner and outer (t1.f1
     * and t2.f2) tables swapped. */
    public LogicalJoinNode swapInnerOuter() {
        Predicate.Op newp;
        if (op == Predicate.Op.GREATER_THAN)
            newp = Predicate.Op.LESS_THAN;
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ)
            newp = Predicate.Op.LESS_THAN_OR_EQ;
        else if (op == Predicate.Op.LESS_THAN)
            newp = Predicate.Op.GREATER_THAN;
        else if (op == Predicate.Op.LESS_THAN_OR_EQ)
            newp = Predicate.Op.GREATER_THAN_OR_EQ;
        else
            newp = op;

        LogicalJoinNode j2 = new LogicalJoinNode(t2Alias, t1Alias, f2PureName, f1PureName, newp);
        return j2;
    }

    @Override
    public boolean equals(Object o) {
        LogicalJoinNode j2 = (LogicalJoinNode) o;
        return (j2.t1Alias.equals(t1Alias)  || j2.t1Alias.equals(t2Alias)) && (j2.t2Alias.equals(t1Alias)  || j2.t2Alias.equals(t2Alias));
    }

    @Override
    public String toString() {
        return t1Alias + ":" + t2Alias ;//+ ";" + f1 + " " + p + " " + f2;
    }

    @Override
    public int hashCode() {
        return t1Alias.hashCode() + t2Alias.hashCode() + f1PureName.hashCode() + f2PureName.hashCode();
    }
}

