package simpleDatabase.others;

import simpleDatabase.basic.Database;
import simpleDatabase.basic.Type;
import simpleDatabase.cache.HeapFile;
import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;
import simpleDatabase.field.IntField;
import simpleDatabase.field.StringField;
import simpleDatabase.iterator.DbFileIterator;
import simpleDatabase.operator.IntHistogram;
import simpleDatabase.operator.Predicate;
import simpleDatabase.field.Field;
import simpleDatabase.operator.StringHistogram;
import simpleDatabase.tx.Transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * finished
 * 表示一个查询中，这张表的数据，例如 histogram
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    /* io 每一页的开销 */
    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String, TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * bin：容器 仓库 垃圾箱
     * histogram的容器数量
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /* Field名称 和 保存最大和最小值的数组 之间的映射 */
    private HashMap<String, int[]> minMaxMap;

    /* 名字和histogram的映射 */
    private HashMap<String, Object> name2Histogram;

    private HeapFile table;

    /* tuples 的数量 */
    private int tuplesNum;

    private int ioCostPerPage;

    private TupleDesc td;

    /* 获取it的数据量 和 最大最小值，和 histogram */
    private void calTupleNumAndHistogram(DbFileIterator it) {
        try {
            it.open();
            while (it.hasNext()) {
                /* 统计行数 */
                ++tuplesNum;
                Tuple cur = it.next();
                /* 统计每一列中的最大最小值 */
                for (int i = 0; i < td.numFields(); ++i) {
                    Type type = td.getFieldType(i);
                    /* String类型不需要统计最大最小值 */
                    if (type == Type.STRING_TYPE) continue;

                    String fieldName = td.getFieldName(i);
                    int val = ((IntField) cur.getField(i)).getValue();
                    /* 统计过一次了，继续统计 */
                    if (minMaxMap.containsKey(fieldName)) {
                        int[] tem = minMaxMap.get(fieldName);
                        /* 跟新最大最小值 */
                        tem[0] = tem[0] > val ? val : tem[0];
                        tem[1] = tem[1] < val ? val : tem[1];
                    } else {
                        int[] tem = new int[]{val, val};
                        minMaxMap.put(fieldName, tem);
                    }
                }
            }

            /* 计算每一列的数据分布情况 */
            for (Map.Entry<String, int[]> entry : minMaxMap.entrySet()) {
                int[] tem = entry.getValue();
                IntHistogram his = new IntHistogram(NUM_HIST_BINS, tem[0], tem[1]);
                name2Histogram.put(entry.getKey(), his);
            }

            /* 重置迭代器 */
            it.rewind();

            /* 更新分布情况 */
            while (it.hasNext()) {
                Tuple tuple = it.next();
                for (int i = 0; i < td.numFields(); ++i) {
                    String fieldName = td.getFieldName(i);
                    Type type = td.getFieldType(i);
                    if (type == Type.INT_TYPE) {
                        int val = ((IntField) tuple.getField(i)).getValue();
                        IntHistogram ih = (IntHistogram) name2Histogram.get(fieldName);
                        /* 根据val，在val对应的桶位置，bucket + 1 */
                        ih.addValue(val);
                    } else {
                        String val = ((StringField) tuple.getField(i)).getValue();
                        StringHistogram sh = null;
                        if (name2Histogram.containsKey(fieldName)) {
                            sh = (StringHistogram) name2Histogram.get(fieldName);
                            sh.addValue(val);
                        } else {
                            /* 还没有添加过，需要手动添加放入map中 */
                            sh = new StringHistogram(NUM_HIST_BINS);
                            sh.addValue(val);
                            name2Histogram.put(fieldName, sh);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个记录一张表所有列的数据的 tableStats 对象
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        table = (HeapFile) Database.getCatalog().getDbFile(tableid);
        td = table.getTupleDesc();
        minMaxMap = new HashMap<>();
        name2Histogram = new HashMap<>();
        /* create a new transaction for the query */
        Transaction t = new Transaction();
        DbFileIterator iterator = table.iterator(t.getId());
        calTupleNumAndHistogram(iterator);
    }

    /**
     * 根据页数来计算总共io开销
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return table.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        /* 行数 * 比重 */
        return (int) Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param idx
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int idx, Predicate.Op op, Field constant) {
        String fieldName = td.getFieldName(idx);
        if (constant.getType() == Type.INT_TYPE) {
            int val = ((IntField) constant).getValue();
            return ((IntHistogram) name2Histogram.get(fieldName)).estimateSelectivity(op, val);
        } else {
            String val = ((StringField) constant).getValue();
            return ((StringHistogram) name2Histogram.get(fieldName)).estimateSelectivity(op, val);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return tuplesNum;
    }

}
