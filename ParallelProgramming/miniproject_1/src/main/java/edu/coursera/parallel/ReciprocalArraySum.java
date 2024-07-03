package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ForkJoinPool;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            double sum = 0.0;
            for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                sum += 1.0 / input[i];
            }
            value = sum;  // Set the value field instead of returning the sum
        }        
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        // Assert the length of the input is even (divisible by 2)
        assert input.length % 2 == 0;
        double sum = 0.0;

        // Calculate the midpoint to separate the array in two chunks
        int midpoint = input.length / 2;
        ReciprocalArraySumTask r1 = new ReciprocalArraySumTask(0, midpoint, input);
        ReciprocalArraySumTask r2 = new ReciprocalArraySumTask(midpoint, input.length, input);
        // Compute the sum of each array
        r1.fork();
        r2.compute();
        r1.join();
        sum = r1.getValue() + r2.getValue();
        return sum;
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(4));
        double sum = 0;

        // Create a task for each chunk of the array
        HashMap<Integer, ReciprocalArraySumTask> tasks = new HashMap<>();
        //ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
        for (int i = 0; i < numTasks; i++) {
            int startIndex = getChunkStartInclusive(i, numTasks, input.length);
            int stopIndex = getChunkEndExclusive(i, numTasks, input.length);
            tasks.put(i, new ReciprocalArraySumTask(startIndex, stopIndex, input));
        }

        for (int i=1; i<numTasks; i++){
            tasks.get(i).fork();
        }
        tasks.get(0).compute();
        sum += tasks.get(0).getValue();
        for (int i=1; i<numTasks; i++){
            tasks.get(i).join();
            sum += tasks.get(i).getValue();
        }

        return sum;
    }


public static void main(String[] args) {
    double[] array = {1.0, 2.0, 2.0, 3.0, 2.5, 1.2};
    System.out.println(seqArraySum(array));
    System.out.println(parArraySum(array));
    System.out.println(parManyTaskArraySum(array,6));
}
}
