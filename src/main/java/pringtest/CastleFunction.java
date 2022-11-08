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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pringtest.datatypes.CastleRule;
import pringtest.datatypes.Cluster;

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

    // Counter for monitoring
    private transient Counter numSuppressedTuples;
    private transient Counter numMergedCluster;
    private transient Counter numCollectedClusters;
    private transient Counter numCollectedClustersKs;
    private transient Counter numCreatedClusters;
    private transient Counter numCreatedClustersSplit;

    private transient DescriptiveStatisticsHistogram eventTimeLag;
    private transient DescriptiveStatisticsHistogram tauHistogram;
    private transient DescriptiveStatisticsHistogram infoLossHistogram;

    private static final Logger LOG = LoggerFactory.getLogger(CastleFunction.class);

    private CastleRule[] rules;

    private final MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeInformation.of(new TypeHint<CastleRule>(){}));

    private transient ListState<Tuple2<ArrayList<Cluster>, ArrayList<Tuple>>> checkpointedState;

    /** Value k for k-anonymity */
    private int k = 5;
    /** Value l for l-diversity (if 0 l-diversity is not applied) */
    private int l = 2;
    /** Number of 'delayed' tuples */
    private int delta = 20;
    /** Max number of clusters in bigGamma */
    private int beta = 50;
    /** Max number of clusters in bigOmega */
    private int zeta = 10;
    /** Set of non-k_s anonymized clusters */
    private ArrayList<Cluster> bigGamma = new ArrayList<>();
    /** Set of k_s anonymized clusters */
    private final LinkedList<Cluster> bigOmega = new LinkedList<>();
    /** All tuple objects currently at hold */
    private ArrayList<Tuple> globalTuples = new ArrayList<>();
    /** The average information loss per cluster */
    private float tau = Float.MAX_VALUE;
    /** Number of last infoLoss values considered for tau */
    private int mu = 10;
    /** Last infoLoss values (max size = mu) */
    private final LinkedList<Float> recentInfoLoss = new LinkedList<>();
    /** Position of the id value inside the tuples */
    private int posTupleId = 1;
    /** Position of the l diversity sensible attribute */
    private int[] posSensibleAttributes = new int[]{1};
    /** If true Prink adds the info loss of a tuple at the end of the tuple (tuple size increases by 1)*/
    private boolean showInfoLoss = false;
    /** Defines how to handle tuples that need to be suppressed.
     * 0 = no tuple is collected;
     * 1 = every quasi-identifier gets nulled;
     * 2 = quasi-identifiers are generalized with max generalization values*/
    private int suppressStrategy = 2;

    public CastleFunction(int posTupleId, int k, int l, int delta, int beta, int zeta, int mu, boolean showInfoLoss, int suppressStrategy){
        this.posTupleId = posTupleId;
        this.k = k;
        this.l = l;
        this.delta = delta;
        this.beta = beta;
        this.zeta = zeta;
        this.mu = mu;
        this.showInfoLoss = showInfoLoss;
        this.suppressStrategy = suppressStrategy;
    }

    public CastleFunction(){
    }

    @Override
    public void processBroadcastElement(Object input, Context context, Collector collector) throws Exception {
        CastleRule rule = (CastleRule) input;
        LOG.info("Rule received: {}", rule.toString());

        BroadcastState<Integer, CastleRule> currentRuleState = context.getBroadcastState(ruleStateDescriptor);

        // Convert rule map into sorted array
        int numOfRules = Math.max((rules != null ? rules.length : 1), (rule.getPosition() + 1));
        CastleRule[] newRuleArray = new CastleRule[numOfRules];
        for(int i = 0; i < numOfRules; i++) {
            if(i == rule.getPosition()){
                context.getBroadcastState(ruleStateDescriptor).put(i, rule);
                newRuleArray[i] = rule;
            }else{
                if (currentRuleState.contains(i)) {
                    newRuleArray[i] = currentRuleState.get(i);
                }else{
                    CastleRule missingRule = new CastleRule(i, Generalization.NONE, false);
                    context.getBroadcastState(ruleStateDescriptor).put(i, missingRule);
                    newRuleArray[i] = missingRule;
                }
            }
        }
        rules = newRuleArray;

        // Redefine sensible attribute positions
        ArrayList<Integer> newPos = new ArrayList<>();
        for(int i = 0; i < rules.length; i++) {
            if(rules[i].getIsSensibleAttribute()) newPos.add(i);
        }
        posSensibleAttributes = newPos.stream().mapToInt(i -> i).toArray();

        // TODO-Later convert into Apache Flink Gauge
//        for(CastleRule r: rules){
//            System.out.println("Rule " + r.getPosition() + ": " + r.toString());
//        }
    }

    @Override
    public void processElement(Object input, ReadOnlyContext context, Collector output) {
        TimerService ts = context.timerService();

        Tuple tuple = (Tuple) input;
        tuple.setField(ts.currentProcessingTime(), 2); // TODO remove after testing

        eventTimeLag.update(ts.currentProcessingTime() - ts.currentWatermark());

        // Do not process any tuples without a rule set to follow
        if(rules == null) return;

        Cluster bestCluster = bestSelection(tuple);
        if(bestCluster == null){
            // Create a new cluster on 'input' and insert it into bigGamma
            bestCluster = new Cluster(rules, posTupleId, showInfoLoss);
            numCreatedClusters.inc();
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
            LOG.error("delayConstraint -> clusterWithInput is NULL");
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
                numCollectedClustersKs.inc();
                removeTuple(input);
                return;
            }

            int m = 0;
            for(Cluster cluster: bigGamma){
                if(clusterWithInput.size() < cluster.size()) m++;
            }

            if(m > (bigGamma.size()/2)){
                // suppress t based on suppressStrategy
                if(suppressStrategy != 0) output.collect(suppressTuple(input));
                removeTuple(input);
                numSuppressedTuples.inc();
                return;
            }

            // Check that total big gamma size is bigger than k before merging
            // TODO check if the overall size sum is enough to determine the size or if all individual pid needs to be merged
            int totalGammaSize = bigGamma.stream().mapToInt(Cluster::size).sum();
            // it must also be checked that there exist at least 'l' distinct values of a_s among all clusters in bigGamma
            if(totalGammaSize < k || !checkDiversityBigGamma()){
                // suppress t based on suppressStrategy
                if(suppressStrategy != 0) output.collect(suppressTuple(input));
                removeTuple(input);
                numSuppressedTuples.inc();
                return;
            }

            Cluster mergedCluster = mergeClusters(clusterWithInput);
            outputCluster(mergedCluster, output);
        }
    }

    /**
     * Generalizes the given tuple with the maximum generalization values
     * @param input Tuple to generalize
     * @return Generalized tuple
     */
    private Object suppressTuple(Tuple input) {
        // null every QI value
        if(suppressStrategy == 1) {
            int inputArity = input.getArity();
            if(showInfoLoss) inputArity++;
            Tuple output = Tuple.newInstance(inputArity);

            for (int i = 0; i < Math.min(inputArity, rules.length); i++) {
                switch (rules[i].getGeneralizationType()) {
                    case REDUCTION:
                    case AGGREGATION:
                    case NONNUMERICAL:
                        output.setField(null, i);
                        break;
                    case NONE:
                    case REDUCTION_WITHOUT_GENERALIZATION:
                    case AGGREGATION_WITHOUT_GENERALIZATION:
                    case NONNUMERICAL_WITHOUT_GENERALIZATION:
                        output.setField(input.getField(i), i);
                        break;
                    default:
                        LOG.error("suppressTuple -> undefined transformation type: {}", rules[i]);
                }
            }
            if(showInfoLoss) output.setField(1f,inputArity-1);
            return output;
        }else if(suppressStrategy == 2){
            // generalize with the most generalized QI value
            Cluster tempCluster = new Cluster(rules, posTupleId, showInfoLoss);
            return tempCluster.generalizeMax(input);
        }else{
            LOG.error("suppressTuple -> undefined suppress strategy! Strategy value: {}", suppressStrategy);
            int inputArity = input.getArity();
            if(showInfoLoss) inputArity++;
            return Tuple.newInstance(inputArity);
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
            // TODO test if correct (if object can be compared with object)
            Set<Object> output = new HashSet<>();
            for(Tuple tuple: allBigGammaEntries){
                int pos = posSensibleAttributes[0];
                // Check for arrays
                Object sensAttributeValue = tuple.getField(pos).getClass().isArray() ? ((Object[]) tuple.getField(pos))[((Object[]) tuple.getField(pos)).length-1] : tuple.getField(pos);
                output.add(sensAttributeValue);
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
                    Map.Entry<Object, Long> temp = allBigGammaEntries.stream().collect(Collectors.groupingBy(s -> (s.getField(pos).getClass().isArray() ? ((Object[]) s.getField(pos))[((Object[]) s.getField(pos)).length-1] : s.getField(pos)), Collectors.counting()))
                            .entrySet().stream().max((attEntry1, attEntry2) -> attEntry1.getValue() > attEntry2.getValue() ? 1 : -1).get();
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> attEntry1.f1.getValue() > attEntry2.f1.getValue() ? 1 : -1).get();
                // Remove all entries that have the least diverse attribute
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                for(Tuple tuple: allBigGammaEntries){
                    // adjust to find also values from arrays TODO re-write
                    Object toCompare = (tuple.getField(mapEntryToDelete.f0).getClass().isArray() ? ((Object[]) tuple.getField(mapEntryToDelete.f0))[((Object[]) tuple.getField(mapEntryToDelete.f0)).length-1] : tuple.getField(mapEntryToDelete.f0));
                    if(toCompare.equals(mapEntryToDelete.f1.getKey())){
                        tuplesToDelete.add(tuple);
                    }
                }
                allBigGammaEntries.removeAll(tuplesToDelete);
                numOfAppearances.clear();
            }
        }
        return false;
    }

    private Cluster mergeClusters(Cluster input) {
        while(input.size() < k){
            numMergedCluster.inc();
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
            bigGamma.remove(input);
        }else{
            clusters.add(input);
        }

        for(Cluster cluster: clusters){
            numCollectedClusters.inc();
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
        infoLossHistogram.update(recentInfoLoss.getLast().longValue());

        float sum = 0;
        for(float recentIL: recentInfoLoss) sum = sum + recentIL;
        tau = sum / recentInfoLoss.size();

        tauHistogram.update((long) tau);
    }

    /**
     * Remove the first value inside bigOmega if the size exceeds zeta
     */
    private void updateBigOmega() {
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
        // Output cluster. See 'SC' in CASTLE Paper definition
        ArrayList<Cluster> output = new ArrayList<>();
        HashMap<Long, ArrayList<Tuple>> buckets = generateBuckets(input);

        while(buckets.size() >= k){
            // Randomly select a bucket and select one tuple
            int random = new Random().nextInt(buckets.size());
            List<Long> ids = new ArrayList<>(buckets.keySet());
            long selectedBucketId = ids.get(random);
            ArrayList<Tuple> selectedBucket = buckets.get(selectedBucketId);
            Tuple selectedTuple = selectedBucket.get(0);

            // Create new sub-cluster with selectedTuple and remove it from original entry
            Cluster newCluster = new Cluster(rules, posTupleId, showInfoLoss);
            newCluster.addEntry(selectedTuple);
            selectedBucket.remove(0);

            if(selectedBucket.size() <= 0) buckets.remove(selectedBucketId);

            // Find buckets with smallest distance to 'selectedTuple'
            ArrayList<Tuple2<Long, Float>> distances = new ArrayList<>();
            for(Map.Entry<Long, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
                if(bucketEntry.getValue().equals(selectedBucket)) continue;
                // Pick one of the tuples and calculate the distance to 'selectedTuple'
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
        }
        numCreatedClustersSplit.inc(output.size());
        return output;
    }

    private Collection<Cluster> splitL(Cluster input) {

        ArrayList<Cluster> output = new ArrayList<>();
        HashMap<Object, ArrayList<Tuple>> buckets = generateBucketsSensAtt(input);
        Random random = new Random();

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
            // Randomly select a bucket and select one tuple
            List<Object> ids = new ArrayList<>(buckets.keySet());
            Object selectedBucketId = ids.get(random.nextInt(buckets.size()));
            ArrayList<Tuple> selectedBucket = buckets.get(selectedBucketId);
            int randomNum = random.nextInt(selectedBucket.size());
            Tuple selectedTuple = selectedBucket.get(randomNum);

            // Create new sub-cluster with selectedTuple and remove it from original entry
            Cluster newCluster = new Cluster(rules, posTupleId, showInfoLoss);
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
                // TODO maybe later if the selection of tuples to add is to large high enlargement values can flow into the cluster. Check if other approach may have less info loss
                if(bucket.size() > 0) {
                    double amountToAdd = Math.max((k * (bucket.size() / (float) sum)), 1);
                    for (int i = 0; i < amountToAdd; i++) {
                        if(newCluster.size() >= k) break;
                        newCluster.addEntry(enlargement.get(i).f0);
                        bucket.remove(enlargement.get(i).f0);
                    }
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
        numCreatedClustersSplit.inc(output.size());
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
                    // TODO check if it works with objects
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
            while (oneIdTuples.size() > 0) {
                for (int pos: posSensibleAttributes) {
                    // TODO check if two strings are added to the same grouping if they have the same value but are different objects
                    Map.Entry<Object, Long> temp = oneIdTuples.stream().collect(Collectors.groupingBy(s -> (s.getField(pos).getClass().isArray() ? ((Object[]) s.getField(pos))[((Object[]) s.getField(pos)).length-1] : s.getField(pos)), Collectors.counting()))
                            .entrySet().stream().max((attEntry1, attEntry2) -> attEntry1.getValue() > attEntry2.getValue() ? 1 : -1).get();
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> attEntry1.f1.getValue() > attEntry2.f1.getValue() ? 1 : -1).get();
                // Remove all entries that have the least diverse attribute from input and add them to a bucket
                String generatedKey = mapEntryToDelete.f0 + "-" + mapEntryToDelete.f1.getKey().toString();
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                output.putIfAbsent(generatedKey, new ArrayList<>());
                for(Tuple tuple: oneIdTuples){
                    // Adjust to find also values from arrays TODO re-write
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
            if(bigGamma.size() >= beta && !minClusters.isEmpty()){
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

    // Metric section

    @Override
    public void open(Configuration config) throws Exception {
        // Gauge section
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Tau (*100 as Int)", (Gauge<Integer>) () -> Math.round(tau*100));
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Information Loss (*100 as Int)", (Gauge<Integer>) () -> Math.round(recentInfoLoss.getLast()*100));
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("k-Value", (Gauge<Integer>) () -> k);
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("l-Value", (Gauge<Integer>) () -> l);
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Sensible Attributes", (Gauge<Integer>) () -> posSensibleAttributes.length);
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Global Tuple Size", (Gauge<Integer>) () -> globalTuples.size());
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Prink", (Gauge<CastleRule[]>) () -> rules);
        // Counter section
        this.numSuppressedTuples = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Suppressed Tuples");
        this.numMergedCluster = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Merged Clusters");
        this.numCollectedClusters = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Collected Cluster (normal)");
        this.numCollectedClustersKs = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Collected Cluster (ks)");
        this.numCreatedClusters = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Created Clusters (normal)");
        this.numCreatedClustersSplit = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Created Clusters (split)");
        // Meter section
//        this.numSuppressedTuples = getRuntimeContext()
//                .getMetricGroup()
//                .addGroup("Prink","Meter")
//                .meter("myMeter", new MyMeter());
        // Histogramm section
        this.eventTimeLag = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink")
                .histogram("eventTimeLag", new DescriptiveStatisticsHistogram(10000));
        this.tauHistogram = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink")
                .histogram("Tau", new DescriptiveStatisticsHistogram(10000));
        this.infoLossHistogram = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink")
                .histogram("Information Loss", new DescriptiveStatisticsHistogram(10000));
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
