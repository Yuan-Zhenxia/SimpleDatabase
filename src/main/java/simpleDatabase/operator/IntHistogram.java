package simpleDatabase.operator;

import simpleDatabase.operator.Predicate;

/**
 * finished
 * int矩形图
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min, max;

    private int buckets;

    private int width;

    private int[] histogram;

    public int tupleNum;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
    	this.max = max;
    	this.buckets = buckets;
    	/* 例如 max = min，需要 + 1， buckets是 2，range就是 2 */
    	double range = (double) (1 + max - min) / buckets;
    	/* 向上取整，例如1.3, 就是 2 */
    	width = (int) Math.ceil(range);
    	tupleNum = 0;
    	histogram = new int[buckets];
    	for (int i = 0; i < buckets; ++ i)
    	    histogram[i] = 0;
    }

    private int val2Idx(int v) {
        if (v == max) return buckets - 1;
        else return (v - min) / width;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	histogram[val2Idx(v)]++;
    	tupleNum++;
    }

    /**
     * 预测估计所需要的返回数
     * selectivity大概指的是 选择的比例 或者数据数量
     *
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int bucketIdx = val2Idx(v);
        /* left指的是当前这个bucket的左边界，right指的是当前这个bucket的右边界 */
        int num, left = bucketIdx * width + min, right = bucketIdx * width + min + width - 1;

        switch (op) {
            case EQUALS:
                if (v < min || v > max) return 0.0;
                else {
                    num = histogram[bucketIdx];
                    /* 该矩阵的高度表示了数量，表示这个值放入的bucket中，与此相同的值数量 */
                    return (num * 1.0 / width) / tupleNum;
                }
            case GREATER_THAN:
                if (v < min) return 1.0;
                if (v > max) return 0.0;
                num = histogram[bucketIdx];
                /* 计算当前bucket中必v大的数据量 */
                double partInBucket = ((right - v) / width * 1.0) * (num * 1.0 / tupleNum);
                int numsOfRightPart = 0;
                /* 累加右边的bucket，右边的都大于等于v */
                for (int i = bucketIdx + 1; i < buckets; ++i) numsOfRightPart += histogram[i];
                double partInRightBuckets = numsOfRightPart * 1.0 / tupleNum;
                return partInBucket + partInRightBuckets;
            case LESS_THAN:
                if (v < min) return 0.0;
                if (v > max) return 1.0;
                num = histogram[bucketIdx];
                double partInBucket2 = ((v - left) / width * 1.0) * (num * 1.0 / tupleNum);
                int numsOfLeftPart = 0;
                for (int i = bucketIdx - 1; i >= 0; i--) numsOfLeftPart += histogram[i];
                double partInLeftBuckets = numsOfLeftPart * 1.0 / tupleNum;
                return partInBucket2 + partInLeftBuckets;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v)
                        + estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v)
                        + estimateSelectivity(Predicate.Op.EQUALS, v);
            case LIKE:
                return avgSelectivity();
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                throw new RuntimeException();
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        /* int 貌似没有like */
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("width: " + width);
        sb.append(", buckets: " + this.buckets);
        sb.append(", max: " + this.max);
        sb.append(", min: " + this.max);
        return sb.toString();
    }
}
