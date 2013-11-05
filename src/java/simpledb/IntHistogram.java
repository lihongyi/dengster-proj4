package simpledb;
import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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
    	// some code goes here

        /** Customary shit. */
        this.numBuckets = buckets;
        this.theHistogram = new int[this.numBuckets];
        Arrays.fill(this.theHistogram, 0);
        this.min = min;
        this.max = max;


        /** Calculate bucket width. */
        this.bucketWidth = (int) Math.ceil((double) (this.max - this.min) / this.numBuckets);

        this.numberTuples = 0;
    }

    private int numBuckets;
    private int min;
    private int max;
    private int[] theHistogram;
    private int bucketWidth;
    private int numberTuples;


    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here

        /** Go through each bucket, see if value falls within range.
        /*  If so, then we increase value of bucket by 1 and increment numTuples. */

        for (int i = 0; i < this.theHistogram.length; i++) {
            int left = min + this.bucketWidth*i;
            int right = min + this.bucketWidth*(i+1);
            if (left <= v && v < right) {
                this.theHistogram[i] += 1;
                this.numberTuples++; 
                break;
            }
        }

    }

    /**
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

    	// some code goes here
        double retVal = 0.0;
        switch (op) {
            case EQUALS: 
                //do something
                retVal = eqSelectivity(v);
                break;
            case GREATER_THAN:
                //do something
                retVal = geSelectivity(v);
                break;
            case GREATER_THAN_OR_EQ:
                retVal = geequalSelectivity(v);
                break;
            case LESS_THAN:
                retVal = leSelectivity(v);
                break;
            case LESS_THAN_OR_EQ:
                retVal = leequalSelectivity(v);
                break;
            case NOT_EQUALS:
                retVal = 1 - eqSelectivity(v);
            default: 
                break;
        }

        return retVal;
    }

    private double eqSelectivity(int v) {
        double retVal = 0;
        for (int i = 0; i < this.theHistogram.length; i++) {
            int left = min + this.bucketWidth*i;
            int right = min + this.bucketWidth*(i+1);
            if (left <= v && v < right) {
                int height = this.theHistogram[i];
                retVal = (double) (height/this.bucketWidth) / this.numberTuples;
            }
        }
        return retVal;   
    }

    private double leSelectivity(int v) {
        if (v <= min) { return 0;}
        if (v > max) { return 1;}
        double retVal = 0;
        int foundBucket = Integer.MAX_VALUE;
        for (int i = 0; i < this.theHistogram.length; i++) {
            int left = min + this.bucketWidth*i;
            int right = min + this.bucketWidth*(i+1);
            if (left <= v && v < right) {
                foundBucket = i;
                double bLeft = (double) left;
                double bF = (double) this.theHistogram[i]/this.numberTuples;
                double bPart = (v-bLeft)/this.bucketWidth;
                retVal += (bF * bPart);
            }
            if (i < foundBucket) {
                retVal += (double) this.theHistogram[i]/ this.numberTuples;
            }
        }
        return retVal;   

    }

    private double leequalSelectivity(int v) {
        if (v <= min) { return 0;}
        if (v > max) { return 1;}
        double retVal = 0;
        int foundBucket = Integer.MAX_VALUE;
        for (int i = 0; i < this.theHistogram.length; i++) {
            int left = min + this.bucketWidth*i;
            int right = min + this.bucketWidth*(i+1);
            if (left <= v && v < right) {
                foundBucket = i;
            }
            if (i <= foundBucket) {
                retVal += (double) this.theHistogram[i]/ this.numberTuples;
            }
        }
        return retVal;           
    }

    private double geequalSelectivity(int v) {
        if (v < min) { return 1;}
        if (v >= max) { return 0;}
        double retVal = 0;
        int foundBucket = Integer.MAX_VALUE;
        for (int i = 0; i < this.theHistogram.length; i++) {
            int left = min + this.bucketWidth*i;
            int right = min + this.bucketWidth*(i+1);
            if (left <= v && v < right) {
                foundBucket = i;
            }
            if (i >= foundBucket) {
                retVal += (double) this.theHistogram[i]/ this.numberTuples;
            }
        }
        return retVal;   

    }

    private double geSelectivity(int v) {
        if (v < min) { return 1;}
        if (v >= max) { return 0;}
        double retVal = 0;
        int foundBucket = Integer.MAX_VALUE;
        for (int i = 0; i < this.theHistogram.length; i++) {
            int left = min + this.bucketWidth*i;
            int right = min + this.bucketWidth*(i+1);
            if (i > foundBucket) {
                retVal += (double) this.theHistogram[i]/ this.numberTuples;
            }
            if (left <= v && v < right) {
                foundBucket = i;
                double bRight = (double) right;
                double bF = (double) this.theHistogram[i]/this.numberTuples;
                double bPart = (bRight-v)/this.bucketWidth;
                retVal += (bF * bPart);
            }
        }
        return retVal;   

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
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
