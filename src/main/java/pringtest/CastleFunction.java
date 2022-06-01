package pringtest;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pringtest.datatypes.Cluster;
import pringtest.datatypes.TaxiFare;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

public class CastleFunction extends KeyedProcessFunction
        implements CheckpointedFunction {

    public enum Generalization {
        REDUCTION,
        AGGREGATION,
        NONE
    }

    private Generalization[] config;

    private int k = 5;
    private int delta = 10;
    private int beta = 5;
    /* Set of non-k_s anonymized clusters */
    private ArrayList<Cluster> bigGamma = new ArrayList<>();
    /* Set of k_s anonymized clusters */
    private ArrayList<Cluster> bigOmega = new ArrayList<>();
    /* All tuple objects currently at hold */
    private ArrayList<Tuple> globalTuples = new ArrayList<>();
    /* The average information loss per cluster */
    private float tau = 0;

//    public CastleFunction(int k, int delta, int beta){
//        this.k = k;
//        this.delta = delta;
//        this.beta = beta;
//    }

    public CastleFunction(Generalization[] config){
        this.config = config;
    }

    @Override
    public void processElement(Object input, Context context, Collector output) throws Exception {
        // TODO move outside of CastelFunction
        Tuple tuple = convertToTuple(input);

        Cluster bestCluster = bestSelection(tuple);
        if(bestCluster == null){
            // Create a new cluster on 'input' and insert it into bigGamma
            bestCluster = new Cluster(config);
            bigGamma.add(bestCluster);
        }
        // Push 'input' into bestCluster
        bestCluster.addEntry(tuple);
        globalTuples.add(tuple);

        // Different approach from the CASTLEGUARD code (see: https://github.com/hallnath1/CASTLEGUARD)
        if(globalTuples.size() > delta) delayConstraint(globalTuples.get(0), output);
    }

    private void delayConstraint(Tuple input, Collector output) {
        Cluster clusterWithInput = getClusterContaining(input);

        if(clusterWithInput.size() >= k){
            outputCluster(clusterWithInput, output);
        }else{
            // TODO check if it is possible to have multiple clusters
            Cluster[] ksClustersWithInput = getClustersContaining(input);
            if(ksClustersWithInput.length > 0){
                // TODO check if Random needs to be outside of function to not create a new random object every time (to not be predictable)
                int random = new Random().nextInt(ksClustersWithInput.length);
                Cluster selected = ksClustersWithInput[random];
                // Output 'input' with the generalization of the selected cluster
                output.collect(selected.generalize(input));
                removeTuple(input);
                return;
            }

            int m = 0;
            for(Cluster cluster: bigGamma){
                if(clusterWithInput.size() < cluster.size()) m++;
            }

            if(m > (bigGamma.size()/2)){
                // suppress t with the most generalized QI value
                // TODO find out what 'most generalized QI' exactly means
//                output.collect(selected.generalize(input));
                removeTuple(input);
            }

            // TODO check which implementation is better
            int totalGammaSize = bigGamma.stream().mapToInt(cluster -> cluster.size()).sum();
//            int count = bigGamma.stream().collect(summingInt(cluster -> cluster.size()) );
            if(totalGammaSize < k){
                // suppress t
                // TODO find out what 'most generalized QI' exactly means
//                output.collect(selected.generalize(input));
                removeTuple(input);
            }

            Cluster mergedCluster = mergeClusters(clusterWithInput);
            outputCluster(mergedCluster, output);
        }

    }

    private Cluster mergeClusters(Cluster input) {
        while(input.size() < k){
            float minEnlargement = Float.MAX_VALUE;
            Cluster clusterWithMinEnlargement = null;
            for(Cluster cluster: bigGamma){
                // skip 'input' to not merge with itself
                if(cluster == input) continue;
                
                if(input.enlargementValue(cluster) < minEnlargement){
                    minEnlargement = input.enlargementValue(cluster);
                    clusterWithMinEnlargement = cluster;
                }
            }
            input.addAllEntries(clusterWithMinEnlargement.getAllEntries());
            bigGamma.remove(clusterWithMinEnlargement);
        }
        return input;
    }

    /**
     * Removes the 'input' tuple from the castle algorithm
     * @param input Tuple to remove
     */
    private void removeTuple(Object input) {
        globalTuples.remove(input);
        Cluster cluster = getClusterContaining(input);
        cluster.removeEntry(input);
        // TODO maybe move inside cluster (inside removeEntry())
        if(cluster.size() <= 0) bigGamma.remove(cluster);
    }

    private void outputCluster(Cluster input, Collector output) {
        ArrayList<Cluster> clusters = new ArrayList<>();
        if(input.size() >= (2*k)){
            clusters.addAll(split(input));
        }else{
            clusters.add(input);
        }

        for(Cluster cluster: clusters){
            for(Tuple tuple: cluster.getAllEntries()){

                output.collect(cluster.generalize(tuple));
//                TODO check if entry should be deleted here or with the deletion of the cluster below (check for calculation of tau)
//                cluster.removeEntry(tuple);
                globalTuples.remove(tuple);
            }
            updateTau(cluster);
            if(cluster.infoLoss() < tau) bigOmega.add(cluster);

            bigGamma.remove(cluster);
        }
    }

    private void updateTau(Cluster cluster) {
        // TODO replace with real calculation
        tau = (tau > 0) ? ((tau + cluster.infoLoss()) / 2) : cluster.infoLoss();
    }

    private Collection<Cluster> split(Cluster input) {
        // TODO remove temp code and implement real split function
        ArrayList<Cluster> output = new ArrayList<>();
        output.add(input);
        return output;
    }

    /**
     * Returns the non-k_s anonymized cluster that includes 'input'
     * @param input
     * @return Cluster including 'input'
     */
    private Cluster getClusterContaining(Object input) {
        for(Cluster cluster: bigGamma){
            if(cluster.contains(input)) return cluster;
        }
        return null;
    }

    /**
     * Returns all k_s anonymized clusters that includes 'input'
     * @param input
     * @return Clusters including 'input'
     */
    private Cluster[] getClustersContaining(Object input) {
        ArrayList<Cluster> output = new ArrayList<>();
        for(Cluster cluster: bigOmega){
            if(cluster.contains(input)) output.add(cluster);
        }
        return output.toArray(new Cluster[0]);
    }


    /**
     * Finds the best cluster to add 'input' to.
     * For more information see: CASTLE paper TODO add ref to paper
     * @param input the streaming tuple to add to a cluster
     * @return the best selection of all possible clusters.
     * Returns null if no fitting cluster is present.
     */
    private Cluster bestSelection(Tuple input) {
        ArrayList<Float> enlargementResults = new ArrayList<>();

        for (Cluster cluster: bigGamma){
            enlargementResults.add(cluster.enlargementValue(input));
        }
        // return null if no clusters are present
        if(enlargementResults.isEmpty()) return null;

        float minValue = Collections.min(enlargementResults);

        ArrayList<Cluster> minClusters = new ArrayList<>();
        ArrayList<Cluster> okClusters = new ArrayList<>();
        for (Cluster cluster: bigGamma){
            if(cluster.enlargementValue(input) == minValue){
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

    private Tuple convertToTuple(Object input){
//        Type t = data.GetType();
//        if(t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Tuple<,>))
//        {
//            var types = t.GetGenericArguments();
//            Console.WriteLine("Datatype = Tuple<{0}, {1}>", types[0].Name, types[1].Name)
//        }
        // TODO make independent of TaxiFare
        TaxiFare temp = (TaxiFare) input;
        return new Tuple4<>(temp.rideId, temp.taxiId, temp.totalFare, temp.totalFare);
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
