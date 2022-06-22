package pringtest;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pringtest.datatypes.CastleRule;
import pringtest.datatypes.Cluster;

import java.time.Instant;
import java.util.*;

public class CastleFunction extends KeyedProcessFunction
        implements CheckpointedFunction {

    public enum Generalization {
        REDUCTION,
        AGGREGATION,
        NONNUMERICAL,
        NONE
    }

    private final Generalization[] config;

    private final MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeInformation.of(new TypeHint<CastleRule>(){}));

    private transient ListState<Tuple2<ArrayList<Cluster>, ArrayList<Tuple>>> checkpointedState;

    private final int k = 5;
    private final int l = 2;
    private final int delta = 10;
    private final int beta = 5;
    /* Set of non-k_s anonymized clusters */
    private ArrayList<Cluster> bigGamma = new ArrayList<>();
    /* Set of k_s anonymized clusters */
    private final ArrayList<Cluster> bigOmega = new ArrayList<>();
    /* All tuple objects currently at hold */
    private ArrayList<Tuple> globalTuples = new ArrayList<>();
    /* The average information loss per cluster */
    private float tau = 0;
    /* Position of the id value inside the tuples */
    private final int posTupleId = 1;
    /* Position of the l diversity sensible attribute */
    private final int[] posSensibleAttributes = new int[]{4,5};

//    public CastleFunction(int k, int delta, int beta){
//        this.k = k;
//        this.delta = delta;
//        this.beta = beta;
//    }

    public CastleFunction(Generalization[] config){
        this.config = config;
    }

    @Override
    public void processElement(Object input, Context context, Collector output) {

        Tuple tuple = (Tuple) input;
        tuple.setField(Instant.now(), 2);

        Cluster bestCluster = bestSelection(tuple);
        if(bestCluster == null){
            // Create a new cluster on 'input' and insert it into bigGamma
            bestCluster = new Cluster(config);
            bigGamma.add(bestCluster);
        }
        // Push 'input' into bestCluster
        bestCluster.addEntry(tuple);
        globalTuples.add(tuple);

        // Different approach than CASTLE. Approach from the CASTLEGUARD code (see: https://github.com/hallnath1/CASTLEGUARD)
        if(globalTuples.size() > delta) delayConstraint(globalTuples.get(0), output);
    }

    private void delayConstraint(Tuple input, Collector output) {
        Cluster clusterWithInput = getClusterContaining(input);
        if(clusterWithInput == null){
            System.out.println("ERROR: delayConstraint -> clusterWithInput is NULL");
            return;
        }

        if(clusterWithInput.size() >= k && clusterWithInput.diversity(posSensibleAttributes) >= l){
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
                return;
            }

            // Check that total big gamma size is bigger than k before merging
            // TODO-Later check which implementation is better
            int totalGammaSize = bigGamma.stream().mapToInt(Cluster::size).sum();
//            int count = bigGamma.stream().collect(summingInt(cluster -> cluster.size()) );
            // it must also be checked that there exist at least 'l' distinct values of a_s among all clusters in bigGamma
            Set<Object> distinctValues = new HashSet<>();
            for(Cluster cluster: bigGamma){
                distinctValues.addAll(cluster.getSensibleValues(posSensibleAttributes));
            }
            if(totalGammaSize < k || distinctValues.size() < l){
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
            if(clusterWithMinEnlargement != null) input.addAllEntries(clusterWithMinEnlargement.getAllEntries());
            bigGamma.remove(clusterWithMinEnlargement);
        }
        return input;
    }

    /**
     * Removes the 'input' tuple from the castle algorithm
     * @param input Tuple to remove
     */
    private void removeTuple(Tuple input) {
        globalTuples.remove(input);
        Cluster cluster = getClusterContaining(input);
        cluster.removeEntry(input);
        if(cluster.size() <= 0) bigGamma.remove(cluster);
    }

    private void outputCluster(Cluster input, Collector output) {
        ArrayList<Cluster> clusters = new ArrayList<>();
        if(input.size() >= (2*k) && input.diversity(posSensibleAttributes) >= l){
            if(l > 0){
                clusters.addAll(splitL(input, posSensibleAttributes));
            }else{
                clusters.addAll(split(input));
            }
            bigGamma.remove(input); // TODO verify if removal at this position is correct
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

    /**
     * Splits the cluster in smaller clusters still conforming k
     * @param input Cluster to split
     * @return Collection on newly generated sub-clusters
     */
    private Collection<Cluster> split(Cluster input) {
        // Output cluster. See 'SC' in CASTLE definition
        ArrayList<Cluster> output = new ArrayList<>();
        HashMap<Long, ArrayList<Tuple>> buckets = generateBuckets(input);

        while(buckets.size() >= k){
            // Randomly select a bucket and select one tuple
            // TODO see if Random() needs to be defined outside the function
            int random = new Random().nextInt(buckets.size());
            List<Long> ids = new ArrayList<>(buckets.keySet());
            long selectedBucketId = ids.get(random);
            ArrayList<Tuple> selectedBucket = buckets.get(selectedBucketId);
            Tuple selectedTuple = selectedBucket.get(0);

            // Create new sub-cluster with selectedTuple and remove it from original entry
            Cluster newCluster = new Cluster(config);
            newCluster.addEntry(selectedTuple);
            selectedBucket.remove(0);

            if(selectedBucket.size() <= 0) buckets.remove(selectedBucketId);

            // Find buckets with smallest distance to 'selectedTuple'
            ArrayList<Tuple2<Long, Float>> distances = new ArrayList<>();
            for(Map.Entry<Long, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
                if(bucketEntry.getValue().equals(selectedBucket)) continue;
                // Pick one of the tuples and calculate the distance to 'selectedTuple'
                // TODO check correctness. See CASTLEGUARD and implementation of enlargement
                distances.add(new Tuple2<>(bucketEntry.getKey(), newCluster.enlargementValue(bucketEntry.getValue().get(0))));
            }
            distances.sort(Comparator.comparing(o -> (o.f1)));

            for(Tuple2<Long, Float> entry: distances.subList(0,k-1)){
                ArrayList<Tuple> bucket = buckets.get(entry.f0);
                Tuple firstTuple = bucket.get(0);
                // Shift tuple from bucket into new cluster
                newCluster.addEntry(firstTuple);
                bucket.remove(0);
                if(bucket.size() <= 0) buckets.remove(entry.f0);
            }

            output.add(newCluster);
        }

        for(Map.Entry<Long, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
            // Find nearest cluster for remaining buckets
            float nearestDistance = Float.MAX_VALUE;
            Cluster nearestCluster = null;
            for(Cluster out: output){
                if(out.enlargementValue(bucketEntry.getValue().get(0)) < nearestDistance){
                    nearestDistance = out.enlargementValue(bucketEntry.getValue().get(0));
                    nearestCluster = out;
                }
            }
            // Add all tuples to cluster and delete the cluster
            if(nearestCluster != null) nearestCluster.addAllEntries(bucketEntry.getValue());
//            buckets.remove(bucketEntry.getKey()); // TODO maybe delete
        }
        return output;
    }

    private Collection<Cluster> splitL(Cluster input, int[] posSensAtts) {
        ArrayList<Cluster> output = new ArrayList<>();
        HashMap<Object, ArrayList<Tuple>> buckets = generateBucketsSensAtt(input, posSensAtts);

        if(buckets.size() < l){
            output.add(input);
            return output;
        }

        int sum = 0;
        for(ArrayList<Tuple> bucket: buckets.values()){
            sum = sum + bucket.size();
        }
        while(buckets.size() >= l && sum >= k){
            // Randomly select a bucket and select one tuple
            // TODO see if Random() needs to be defined outside the function
            Random random = new Random();
            List<Object> ids = new ArrayList<>(buckets.keySet());
            Object selectedBucketId = ids.get(random.nextInt(buckets.size()));
            ArrayList<Tuple> selectedBucket = buckets.get(selectedBucketId);
            int randomNum = random.nextInt(selectedBucket.size());
            Tuple selectedTuple = selectedBucket.get(randomNum);

            // Create new sub-cluster with selectedTuple and remove it from original entry
            Cluster newCluster = new Cluster(config);
            newCluster.addEntry(selectedTuple);
            selectedBucket.remove(randomNum);

            ArrayList<Object> bucketKeysToDelete = new ArrayList<>();

            for(Map.Entry<Object, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
                ArrayList<Tuple> bucket = bucketEntry.getValue();

                // Find tuples with smallest enlargement
                ArrayList<Tuple2<Tuple, Float>> enlargement = new ArrayList<>();
                for(Tuple tuple: bucket){
                    enlargement.add(new Tuple2<>(tuple, newCluster.enlargementValue(tuple)));
                }
                // Sort enlargement values to select the tuples with the smallest enlargements
                enlargement.sort(Comparator.comparing(o -> (o.f1)));

                // Select first (k*(bucket.size()/sum)) tuples and move them to the new cluster
                for(int i = 0; i < (k*(bucket.size()/sum)); i++) {
                    newCluster.addEntry(enlargement.get(i).f0);
                    bucket.remove(enlargement.get(i).f0);
                }

                // Remove buckets that have no values in them
//                if(bucket.size() <= 0) buckets.remove(bucketEntry.getKey());
                if(bucket.size() <= 0) bucketKeysToDelete.add(bucketEntry.getKey());
            }
            for(Object key: bucketKeysToDelete) buckets.remove(key);

            output.add(newCluster);
        }

        ArrayList<Object> bucketKeysToDelete = new ArrayList<>();

        // Find nearest cluster for remaining bucket values
        for(Map.Entry<Object, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
            ArrayList<Tuple> bucket = bucketEntry.getValue();

            for(Tuple tuple: bucket){
                float nearestDistance = Float.MAX_VALUE;
                Cluster nearestCluster = null;
                for(Cluster out: output){
                    if(out.enlargementValue(tuple) < nearestDistance){
                        nearestDistance = out.enlargementValue(tuple);
                        nearestCluster = out;
                    }
                }
                // Add all tuples to cluster and delete the cluster
                if(nearestCluster != null) nearestCluster.addEntry(tuple);
            }
            bucketKeysToDelete.add(bucketEntry.getKey());
        }
        for(Object key: bucketKeysToDelete) buckets.remove(key);

        for(Cluster cluster: output){
            ArrayList<Tuple> idTuples = new ArrayList<>();
            for(Tuple tuple: cluster.getAllEntries()) {
                // Select all tuples inside input with the same id value as tuple
                // TODO check if generateBucketsSensAtt needs to remove tuples from the input cluster to prevent duplicates
                long tupleId = tuple.getField(posTupleId);
                for (Tuple inputTuple : input.getAllEntries()) {
                    long tupleIdInput = inputTuple.getField(posTupleId);
                    if (tupleId == tupleIdInput) idTuples.add(inputTuple);
                }
            }
            // Add all tuples with the same ids to the cluster
            cluster.addAllEntries(idTuples);
            // Delete them from the input cluster
            input.removeAllEntries(idTuples);
            // Delete cluster from bigGamma if empty
            if(input.size() <= 0) bigGamma.remove(input);
        }

        return output;
    }

    /**
     * Generates a HashMap (Buckets inside CASTLE) with one tuples per 'pid' inside input, while using the sensible attribute as key
     * @param input Cluster with tuples to create HashMap
     * @return HashMap of one tuple per 'pid' sorted in 'buckets' based on 'sensible attribute'
     */
    private HashMap<Object, ArrayList<Tuple>> generateBucketsSensAtt(Cluster input, int[] posSensAtts) {
        // TODO replace object with string if attribute combining is kept
        HashMap<Object, ArrayList<Tuple>> output = new HashMap<>();
        HashSet<Long> usedIds = new HashSet<>();

        for(Tuple tuple: input.getAllEntries()){
            long tupleId = tuple.getField(posTupleId);
            if(usedIds.add(tupleId)){
                StringBuilder builder = new StringBuilder();
                for(int pos: posSensAtts){
                    builder.append(tuple.getField(pos).toString());
                    // Add seperator to prevent attribute mismatching
                    builder.append(";");
                }
                String sensAtt = builder.toString();
                output.putIfAbsent(sensAtt, new ArrayList<>());
                output.get(sensAtt).add(tuple);
            }
        }
        // TODO check if generateBucketsSensAtt needs to remove tuples from the input cluster
        return output;
    }

    /**
     * Generates a HashMap (Buckets inside CASTLE) with all the tuples inside input, while using the tupleId as key
     * @param input Cluster with tuples to create HashMap
     * @return HashMap of all tuples sorted in 'buckets' based on tuple id
     */
    private HashMap<Long, ArrayList<Tuple>> generateBuckets(Cluster input) {
        HashMap<Long, ArrayList<Tuple>> output = new HashMap<>();

        for(Tuple tuple: input.getAllEntries()){
            long tupleId = tuple.getField(posTupleId);
            output.putIfAbsent(tupleId, new ArrayList<>());
            output.get(tupleId).add(tuple);
        }
        return output;
    }

    /**
     * Returns the non-k_s anonymized cluster that includes 'input'
     * @param input The tuple that needs to be included
     * @return Cluster including 'input'
     */
    private Cluster getClusterContaining(Tuple input) {
        for(Cluster cluster: bigGamma){
            if(cluster.contains(input)) return cluster;
        }
        return null;
    }

    /**
     * Returns all k_s anonymized clusters that includes 'input'
     * @param input The tuple that needs to be included
     * @return Clusters including 'input'
     */
    private Cluster[] getClustersContaining(Tuple input) {
        ArrayList<Cluster> output = new ArrayList<>();
        for(Cluster cluster: bigOmega){
            if(cluster.contains(input)) output.add(cluster);
        }
        return output.toArray(new Cluster[0]);
    }


    /**
     * Finds the best cluster to add 'input' to.
     * For more information see: CASTLE paper
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
//        StringBuilder sb = new StringBuilder();
//        sb.append("EnlargementResults1:" + enlargementResults.toString() + "\n");
//        sb.append("EnlargementResults2:");
        for (Cluster cluster: bigGamma){
//            sb.append(cluster.enlargementValue(input) + ";");
            if(cluster.enlargementValue(input) == minValue){
                minClusters.add(cluster);
//                sb.append("<- ADDED;");

                float informationLoss = cluster.informationLossWith(input);
                if(informationLoss <= tau){
                    okClusters.add(cluster);
                }
            }
        }
//        System.out.println(sb.toString());

        if(okClusters.isEmpty()){
            if(bigGamma.size() >= beta){
                // Return any cluster in minValue with minValue
                System.out.println("MinCluster:" + minClusters.toString());
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
        checkpointedState.clear();
        // TODO check if bigOmega needs to be saved as well
        checkpointedState.add(new Tuple2<>(bigGamma, globalTuples));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<ArrayList<Cluster>, ArrayList<Tuple>>> descriptor =
                new ListStateDescriptor<>("bigGamma-globalTuples",
                        TypeInformation.of(new TypeHint<Tuple2<ArrayList<Cluster>, ArrayList<Tuple>>>(){}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            Tuple2<ArrayList<Cluster>, ArrayList<Tuple>> entry = (Tuple2<ArrayList<Cluster>, ArrayList<Tuple>>) checkpointedState.get();
            bigGamma = entry.f0;
            globalTuples = entry.f1;
        }
    }
}
