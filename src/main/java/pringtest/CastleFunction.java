package pringtest;

import org.apache.flink.api.common.state.BroadcastState;
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
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import pringtest.datatypes.CastleRule;
import pringtest.datatypes.Cluster;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class CastleFunction extends KeyedBroadcastProcessFunction
        implements CheckpointedFunction {

    public enum Generalization {
        REDUCTION,
        AGGREGATION,
        NONNUMERICAL,
        REDUCTION_WITHOUT_GENERALIZATION,
        AGGREGATION_WITHOUT_GENERALIZATION,
        NONNUMERICAL_WITHOUT_GENERALIZATION,
        NONE
    }

    private CastleRule[] rules;

    private final MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeInformation.of(new TypeHint<CastleRule>(){}));

    private transient ListState<Tuple2<ArrayList<Cluster>, ArrayList<Tuple>>> checkpointedState;

    private final int k = 5;
    private final int l = 2;
    /** Number of 'delayed' tuples */
    private final int delta = 10;
    /** Max number of clusters in bigGamma */
    private final int beta = 10;
    /** Max number of clusters in bigOmega */
    private final int zeta = 10;
    /** Set of non-k_s anonymized clusters */
    private ArrayList<Cluster> bigGamma = new ArrayList<>();
    /** Set of k_s anonymized clusters */
    private final LinkedList<Cluster> bigOmega = new LinkedList<>();
    /** All tuple objects currently at hold */
    private ArrayList<Tuple> globalTuples = new ArrayList<>();
    /** The average information loss per cluster */
    private float tau = 0; // TODO check if start at 0 or Float.MAX_VALUE
    /** Number of last infoLoss values considered for tau */
    private float mu = 5;
    /** Last infoLoss values (max size = mu) */
    private LinkedList<Float> recentInfoLoss = new LinkedList<>();
    /** Position of the id value inside the tuples */
    private final int posTupleId = 1;
    /** Position of the l diversity sensible attribute */
    private int[] posSensibleAttributes = new int[]{1};

//    public CastleFunction(int k, int l, int delta, int beta){
//        this.k = k;
//        this.delta = delta;
//        this.beta = beta;
//    }

    public CastleFunction(){
    }

    @Override
    public void processBroadcastElement(Object input, Context context, Collector collector) throws Exception {
        CastleRule rule = (CastleRule) input;
        System.out.println("RULE TRANSMITTED: Position:" + rule.getPosition() + " rule:" + rule.toString());

        BroadcastState<Integer, CastleRule> currentRuleState = context.getBroadcastState(ruleStateDescriptor);

        // Convert rule map into sorted array
        ArrayList<CastleRule> newRuleArray = new ArrayList<>();
        for(int i = 0; i < rule.getPosition(); i++) {
            if(currentRuleState.contains(i)){
                newRuleArray.add(i, currentRuleState.get(i));
            }else{
                CastleRule missingRule = new CastleRule(i, Generalization.NONE, false);
                context.getBroadcastState(ruleStateDescriptor).put(i, missingRule);
                newRuleArray.add(i, missingRule);
            }
        }
        context.getBroadcastState(ruleStateDescriptor).put(rule.getPosition(), rule);
        newRuleArray.add(rule.getPosition(), rule);

        rules = newRuleArray.toArray(new CastleRule[]{});

        // Redefine sensible attribute positions
        ArrayList<Integer> newPos = new ArrayList<>();
        for(int i = 0; i < rules.length; i++) {
            if(rules[i].getIsSensibleAttribute()) newPos.add(i);
        }
        posSensibleAttributes = newPos.stream().mapToInt(i -> i).toArray();

        // TODO-Later remove after testing
        System.out.println("RULE TRANSMISSION: Updated rules:");
        for(CastleRule r: rules){
            System.out.println("Rule " + r.getPosition() + ": " + r.toString());
        }
    }

    @Override
    public void processElement(Object input, ReadOnlyContext context, Collector output) {

        Tuple tuple = (Tuple) input;
        tuple.setField(Instant.now(), 2); // TODO remove after testing

        // Do not process any tuples without a rule set to follow
        if(rules == null) return;

        Cluster bestCluster = bestSelection(tuple);
        if(bestCluster == null){
            // Create a new cluster on 'input' and insert it into bigGamma
            bestCluster = new Cluster(rules);
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
            Cluster[] ksClustersWithInput = getClustersContaining(input);
            if(ksClustersWithInput.length > 0){
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
//                System.out.println("Outlier detected (m > (bigGamma.size()/2)): m:" + m + " bigGamma.size:" + bigGamma.size() + " input:" + input);
//                output.collect(selected.generalize(input));
                removeTuple(input);
                return;
            }

            // Check that total big gamma size is bigger than k before merging
            // TODO-Later check which implementation is better
            int totalGammaSize = bigGamma.stream().mapToInt(Cluster::size).sum();
//            int count = bigGamma.stream().collect(summingInt(cluster -> cluster.size()) );
            // it must also be checked that there exist at least 'l' distinct values of a_s among all clusters in bigGamma
            if(totalGammaSize < k || !checkDiversityBigGamma()){
                // suppress t
                // TODO find out what 'most generalized QI' exactly means
//                System.out.println("Outlier detected (totalGammaSize(" + totalGammaSize + ") < k || !checkDiversityBigGamma()(" + !checkDiversityBigGamma() + ") < l):" + input);
//                output.collect(selected.generalize(input));
                removeTuple(input);
                return;
            }

            Cluster mergedCluster = mergeClusters(clusterWithInput);
            outputCluster(mergedCluster, output);
        }
    }

    /**
     * Checks if bigGamma has at least l diverse values for the sensible attributes
     * @return true if diversity is equal or bigger than l, false if diversity is smaller than l
     */
    private boolean checkDiversityBigGamma() {
        if(posSensibleAttributes.length <= 0) return true;

        ArrayList<Tuple> allBigGammaEntries = new ArrayList<>();
        for(Cluster cluster: bigGamma){
            allBigGammaEntries.addAll(cluster.getAllEntries());
        }

        if(posSensibleAttributes.length == 1){
            // Calculate the amount of different values inside the sensible attribute and return true if bigger than l
            Set<String> output = new HashSet<>();
            for(Tuple tuple: allBigGammaEntries){
                output.add(tuple.getField(posSensibleAttributes[0]));
                if(output.size() >= l) return true;
            }
        }else {
            // See for concept: https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            int counter = 0;

            while (allBigGammaEntries.size() > 0) {
                counter++;
                if(counter >= l) return true;
                for (int pos : posSensibleAttributes) {
                    // TODO check if two strings are added to the same grouping if they have the same value but are different objects
                    Map.Entry<Object, Long> temp = allBigGammaEntries.stream().collect(Collectors.groupingBy(s -> s.getField(pos), Collectors.counting()))
                            .entrySet().stream().max((attEntry1, attEntry2) -> attEntry1.getValue() > attEntry2.getValue() ? 1 : -1).get();
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> attEntry1.f1.getValue() > attEntry2.f1.getValue() ? 1 : -1).get();
//                System.out.println("BigGamma: Least diverse attribute value:" + mapEntryToDelete.toString() + " Counter:" + counter + " CopySize:" + allBigGammaEntries.size() + " OriginalSize:" + allBigGammaEntries.size());
                // Remove all entries that have the least diverse attribute
                allBigGammaEntries.removeIf(i -> i.getField(mapEntryToDelete.f0).equals(mapEntryToDelete.f1.getKey()));
                numOfAppearances.clear();
            }
        }
        return false;
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
                clusters.addAll(splitL(input));
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
                globalTuples.remove(tuple);
            }
            float clusterInfoLoss = cluster.infoLoss();
            updateTau(clusterInfoLoss);
            if(clusterInfoLoss < tau) bigOmega.addLast(cluster);
            updateBigOmega();

            bigGamma.remove(cluster);
        }
    }

    /**
     * Update tau be calculating the average of the 'mu' last infoLoss values
     * @param clusterInfoLoss new infoLoss from the last generalized cluster (will be added to recentInfoLoss)
     */
    private void updateTau(float clusterInfoLoss) {
        recentInfoLoss.addLast(clusterInfoLoss);
        if(recentInfoLoss.size() > mu) recentInfoLoss.removeFirst();

        float sum = 0;
        for(float recentIL: recentInfoLoss) sum = sum + recentIL;
        tau = sum / recentInfoLoss.size();
    }

    /**
     * Remove the first value inside bigOmega if the size exceeds zeta
     */
    private void updateBigOmega() {
        // TODO-Maybe (remove all entries with infoLoss bigger than tau)
        if(bigOmega.size() > zeta){
            bigOmega.removeFirst();
        }
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
            Cluster newCluster = new Cluster(rules);
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

    private Collection<Cluster> splitL(Cluster input) {

        // TODO delete after testing
        StringBuilder sb = new StringBuilder();
        sb.append("------ splitL input values ------\n");
        for(Tuple t: input.getAllEntries()){
            sb.append("| - ").append(t.toString()).append("\n");
        }
        sb.append("-----------------------------------");
//        System.out.println(sb.toString());

        ArrayList<Cluster> output = new ArrayList<>();
        HashMap<Object, ArrayList<Tuple>> buckets = generateBucketsSensAtt(input);
        // TODO see if Random() needs to be defined outside the function
        Random random = new Random();


        StringBuilder sb1 = new StringBuilder();
        sb1.append("------ splitL input values after generateBuckets (tuples with more than one entry for pid) ------\n");
        for(Tuple t: input.getAllEntries()){
            sb1.append("| - ").append(t.toString()).append("\n");
        }
//        System.out.println(sb1.toString());

        if(buckets.size() < l){
            // Re-add bucket values to input to return input back to the system
            for(ArrayList<Tuple> bucket: buckets.values()){
                input.addAllEntries(bucket);
            }
            output.add(input);
            // TODO delete after testing
            checkGeneratedClusters(output, "bucket.size to small. No bucket generation");
            return output;
        }

        int sum = 0;
        for(ArrayList<Tuple> bucket: buckets.values()){
            sum = sum + bucket.size();
        }
        while(buckets.size() >= l && sum >= k){
//            System.out.println("DEBUG: Start while (buckets.size() >= l && sum >= k) -> " + buckets.size() + " >= " + l + " && " + sum + " >= " + k + ")");
            // Randomly select a bucket and select one tuple
            List<Object> ids = new ArrayList<>(buckets.keySet());
            Object selectedBucketId = ids.get(random.nextInt(buckets.size()));
            ArrayList<Tuple> selectedBucket = buckets.get(selectedBucketId);
            int randomNum = random.nextInt(selectedBucket.size());
            Tuple selectedTuple = selectedBucket.get(randomNum);

            // Create new sub-cluster with selectedTuple and remove it from original entry
            Cluster newCluster = new Cluster(rules);
            newCluster.addEntry(selectedTuple);
            selectedBucket.remove(randomNum);
            // Remove bucket if it has no values in them
            if(selectedBucket.size() <= 0) buckets.remove(selectedBucketId);

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

                // Select first (k*(bucket.size()/sum)) tuples or at least 1 and move them to the new cluster
                if(bucket.size() > 0) {
                    double amountToAdd = Math.max((k * (bucket.size() / (float) sum)), 1);
//                    System.out.print("DEBUG: amountToAdd:" + (k * (bucket.size() / (float) sum)) + " details[key:" + bucketEntry.getKey() + " size:" + bucket.size() + "] Added:");
                    for (int i = 0; i < amountToAdd; i++) {
                        if(newCluster.size() >= k) break;
//                        System.out.print("+");
                        newCluster.addEntry(enlargement.get(i).f0);
                        bucket.remove(enlargement.get(i).f0);
                    }
//                    System.out.println(" End size:" + newCluster.size());
                }else{
//                    System.out.println("DEBUG: amountToAdd:" + "Bucket empty" + " details[key:" + bucketEntry.getKey() + " size:" + bucket.size() + "]");
                }

                // Remove buckets that have no values in them
                if(bucket.size() <= 0) bucketKeysToDelete.add(bucketEntry.getKey());
                if(newCluster.size() >= k) break;
            }
            // Remove buckets that have no values in them
            for(Object key: bucketKeysToDelete) buckets.remove(key);

            output.add(newCluster);

            // Recalculate sum
            sum = 0;
            for(ArrayList<Tuple> bucket: buckets.values()){
                sum = sum + bucket.size();
            }
//            System.out.println("DEBUG: End while (buckets.size() >= l && sum >= k) -> " + buckets.size() + " >= " + l + " && " + sum + " >= " + k + ")");
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
        }

        // TODO delete after testing
//        checkGeneratedClusters(output, "Bucket generation");

        return output;
    }

    // Pur testing function delete after testing
    private void checkGeneratedClusters(ArrayList<Cluster> output, String text) {
        StringBuilder sb = new StringBuilder();
        sb.append("----------| Cluster Check (").append(text).append(") |---------- \n");
        for (int i = 0; i < output.size(); i++) {
            sb.append("| Output Cluster Nr:").append(i).append("\n");
            sb.append("| Diversity:").append(output.get(i).diversity(posSensibleAttributes)).append(" l:").append(l).append("\n");
            sb.append("| Has entries:\n");
            for(Tuple tuple: output.get(i).getAllEntries()){
                sb.append("| - ").append(tuple).append("\n");
            }
        }
        sb.append("---------------------------------");
        System.out.println(sb.toString());
    }

    /**
     * Generates a HashMap (Buckets inside CASTLE) with one tuples per 'pid' inside input, while using the sensible attribute as key
     * @param input Cluster with tuples to create HashMap
     * @return HashMap of one tuple per 'pid' sorted in 'buckets' based on 'sensible attribute'
     */
    private HashMap<Object, ArrayList<Tuple>> generateBucketsSensAtt(Cluster input) {
        HashMap<Object, ArrayList<Tuple>> output = new HashMap<>();
        HashSet<Long> usedIds = new HashSet<>();
        ArrayList<Tuple> inputTuplesToDelete = new ArrayList<>(); // TODO-Later maybe delete through iterator if more performant

        if(posSensibleAttributes.length <= 0) return output;
        if(posSensibleAttributes.length == 1){
            for(Tuple tuple: input.getAllEntries()){
                long tupleId = tuple.getField(posTupleId);
                if(usedIds.add(tupleId)){
                    output.putIfAbsent(tuple.getField(posSensibleAttributes[0]), new ArrayList<>());
                    output.get(tuple.getField(posSensibleAttributes[0])).add(tuple);
                    inputTuplesToDelete.add(tuple);
                }
            }
        }else{
            ArrayList<Tuple> oneIdTuples = new ArrayList<>();
            // Select one tuple per pid/tupleId
            for(Tuple tuple: input.getAllEntries()){
                long tupleId = tuple.getField(posTupleId);
                if(usedIds.add(tupleId)){
                    oneIdTuples.add(tuple);
                    inputTuplesToDelete.add(tuple);
                }
            }

            // Generate buckets based on concept: https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            int counter = 0;
            while (oneIdTuples.size() > 0) {
                counter++;
                for (int pos: posSensibleAttributes) {
                    // TODO check if two strings are added to the same grouping if they have the same value but are different objects
                    Map.Entry<Object, Long> temp = oneIdTuples.stream().collect(Collectors.groupingBy(s -> (s.getField(pos).getClass().isArray() ? ((Object[]) s.getField(pos))[((Object[]) s.getField(pos)).length-1] : s.getField(pos)), Collectors.counting()))
                            .entrySet().stream().max((attEntry1, attEntry2) -> attEntry1.getValue() > attEntry2.getValue() ? 1 : -1).get();
                    numOfAppearances.add(Tuple2.of(pos, temp));
                    // TODO delete after testing
//                    Map<Object, Long> temp2 = oneIdTuples.stream().collect(Collectors.groupingBy(s -> (s.getField(pos).getClass().isArray() ? ((Object[]) s.getField(pos))[((Object[]) s.getField(pos)).length-1] : s.getField(pos)), Collectors.counting()));
//                    System.out.println("DEBUG: Collector results[" + pos + "]:" + temp2.toString());
                }
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> attEntry1.f1.getValue() > attEntry2.f1.getValue() ? 1 : -1).get();
//                System.out.println("Generate buckets:Least diverse attribute value:" + mapEntryToDelete.toString() + " Counter:" + counter + " CopySize:" + oneIdTuples.size() + " OriginalSize:" + oneIdTuples.size());

                // Remove all entries that have the least diverse attribute from input and add them to a bucket
                // TODO check if there is a better key generation or just use the counter value as key
                String generatedKey = mapEntryToDelete.f0 + "-" + mapEntryToDelete.f1.getKey().toString();
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                output.putIfAbsent(generatedKey, new ArrayList<>());
                for(Tuple tuple: oneIdTuples){
                    // adjust to find also values from arrays TODO re-write
                    Object toCompare = (tuple.getField(mapEntryToDelete.f0).getClass().isArray() ? ((Object[]) tuple.getField(mapEntryToDelete.f0))[((Object[]) tuple.getField(mapEntryToDelete.f0)).length-1] : tuple.getField(mapEntryToDelete.f0));
                    if(toCompare.equals(mapEntryToDelete.f1.getKey())){
                        output.get(generatedKey).add(tuple);
                        tuplesToDelete.add(tuple);
                    }
                }
                oneIdTuples.removeAll(tuplesToDelete);

                numOfAppearances.clear();
            }
        }
//        System.out.println("Generated buckets -> " + output.toString());
        // Remove all used tuples to prevent duplicates inside splitL function
        input.removeAllEntries(inputTuplesToDelete);
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
     * Returns all k_s anonymized clusters that includes 'input' (enlargement value is 0)
     * @param input The tuple that needs to be included
     * @return Clusters including 'input'
     */
    private Cluster[] getClustersContaining(Tuple input) {
        ArrayList<Cluster> output = new ArrayList<>();
        for(Cluster cluster: bigOmega){
            if(cluster.enlargementValue(input) <= 0) output.add(cluster);
        }
        System.out.println("getClusterContaining (found inputs in bigOmega) BigOmega Size:" + bigOmega.size() + " found size:" + output.size()
                + " bigOmega:" + bigOmega.toString());
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

            // TODO recreate rules array (see broadcast input)
        }
    }
}
