package simpleDatabase.operator.join;

import simpleDatabase.basic.Catalog;

/**
 * finised on 5 Aug.
 * 通过表索引和表别名构建逻辑扫描节点
 * A LogicalScanNode represents table in the FROM list in a
 * LogicalQueryPlan */
public class LogicalScanNode {

    /** The name (alias) of the table as it is used in the query */
    public String alias;

    /** The table identifier (can be passed to {@link Catalog#getDbFile})
     *   to retrieve a DbFile */
    public int t;

    public LogicalScanNode(int table, String tableAlias) {
        this.alias = tableAlias;
        this.t = table;
    }
}
