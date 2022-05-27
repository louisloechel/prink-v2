package pringtest;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pringtest.datatypes.Cluster;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;

public class CastleFunction extends KeyedProcessFunction implements CheckpointedFunction {

    private int k = 5;
    private int delta = 10;
    private int beta = 5;
    /* Set of non-k_s anonymized clusters */
    private ArrayList<Cluster> bigGamma = new ArrayList<>();
    /* Set of k_s anonymized clusters */
    private ArrayList<Cluster> bigOmega = new ArrayList<>();
    /* All tuple objects currently at hold */
    private ArrayList<Object> globalTuples = new ArrayList<>();
    // TODO commend variable use
    private int tau = 0;

    public CastleFunction(int k, int delta, int beta){
        this.k = k;
        this.delta = delta;
        this.beta = beta;
    }

    @Override
    public void processElement(Object input, Context context, Collector output) throws Exception {

        Cluster bestCluster = bestSelection(input);
        if(bestCluster == null){
            // Create a new cluster on 'input' and insert it into bigGamma
            bestCluster = new Cluster();
            bigGamma.add(bestCluster);
        }
        // Push 'input' into bestCluster
        bestCluster.addEntry(input);
        globalTuples.add(input);

        // Let 'inputOld' be the tuple with position equal to [input.position - delta]
//        Object inputOld = null; // TODO getTupleAtPosition(input.position - delta);
//        if(!inputOld.isReleased()) delayConstraint(inputOld); // TODO create isReleased() for clusters
        // different approach from the CASTLEGUARD code (see: https://github.com/hallnath1/CASTLEGUARD)
        if(globalTuples.size() > delta) delayConstraint(globalTuples.get(0));
    }

    private void delayConstraint(Object rename) {
    }

    /**
     * Finds the best cluster to add 'input' to.
     * For more information see: CASTLE paper TODO add ref to paper
     * @param input the streaming tuple to add to a cluster
     * @return the best selection of all possible clusters.
     * Returns null if no fitting cluster is present.
     */
    private Cluster bestSelection(Object input) {
        ArrayList<Float> enlargementResults = new ArrayList<>();

        for (Cluster cluster: bigGamma){
            enlargementResults.add(cluster.enlargementValue());
        }
        // return null if no clusters are present
        if(enlargementResults.isEmpty()) return null;

        float minValue = Collections.min(enlargementResults);

        ArrayList<Cluster> minClusters = new ArrayList<>();
        ArrayList<Cluster> okClusters = new ArrayList<>();
        for (Cluster cluster: bigGamma){
            if(cluster.enlargementValue() == minValue){
                minClusters.add(cluster);

                float informationLoss = cluster.informationLossWith(input);
                if(informationLoss <= tau){
                    okClusters.add(cluster);
                }
            }
        }

        if(okClusters.isEmpty()){
            if(bigGamma.size() >= beta){
                // Return any cluster in minValue with minValue
                return minClusters.get(0);
            } else {
                return null;
            }
        } else {
            // Return any cluster in okCluster with minValue
            return okClusters.get(0);
        }
    }

    // State section
    // TODO create the needed states

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
