package prink.datatypes;

import org.apache.flink.api.java.tuple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prink.CastleFunction;
import prink.generalizations.AggregationGeneralizer;
import prink.generalizations.NonNumericalGeneralizer;
import prink.generalizations.ReductionGeneralizer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Cluster class used by the CastleFunction to group data tuples together and generalize them.
 * Also handles information loss and enlargement calculation.
 */
public class Cluster {

    private final CastleRule[] config;
    private final int posTupleId;

    private final ArrayList<Tuple> entries = new ArrayList<>();

    private final AggregationGeneralizer aggreGeneralizer;
    private final ReductionGeneralizer reductGeneralizer;
    private final NonNumericalGeneralizer nonNumGeneralizer;

    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private float cachedInfoLoss = -1;

    /** If true caches enlargement value results per generalized attribute (gets cleared when new entity is added to the cluster) */
    private final boolean CACHE_INFO_LOSS_PER_ATT = true;
    final int MAX_CACHED_ENTRIES = 100;
    // DEBUG params
    boolean showRemoveEntry = false;
    boolean showAddedEntry = false;
    boolean showInfoLoss;
    boolean showEnlargement = false;

    List<Map<Object, Double>> infoLossValuesRequests = new ArrayList<>();

    /**
     * Constructor of the Cluster class
     * @param rules Array of CastleRules to follow
     * @param posTupleId Position of the tuple id inside the used tuples
     * @param showInfoLoss If true, the information loss will be added to the output tuple
     */
    public Cluster(CastleRule[] rules, int posTupleId, boolean showInfoLoss) {
        this.config = rules;
        this.posTupleId = posTupleId;
        this.showInfoLoss = showInfoLoss;
        aggreGeneralizer = new AggregationGeneralizer(config);
        reductGeneralizer = new ReductionGeneralizer(this);
        nonNumGeneralizer = new NonNumericalGeneralizer(config);
    }

    /**
     * Calculates the enlargement value of this cluster when adding the given input
     * @param input Cluster to consider for the enlargement calculation
     * @return Enlargement value with input
     */
    public float enlargementValue(Cluster input) {
        if(entries.size() <= 0) LOG.error("enlargementValue(Cluster) called on cluster with size: 0 ");
        if(showEnlargement) System.out.println("Enlargement Value Cluster:" + (informationLossWith(input) - infoLoss()));
        return informationLossWith(input) - infoLoss();
    }

    /**
     * Calculates the enlargement value of this cluster when adding the given input
     * @param input Tuple to consider for the enlargement calculation
     * @return Enlargement value with input
     */
    public float enlargementValue(Tuple input) {
        if(entries.size() <= 0) LOG.error("enlargementValue(tuple) called on cluster with size: 0");
        if(showEnlargement) System.out.println("Enlargement Value Tuple:" + (informationLossWith(input) - infoLoss()));
        return informationLossWith(input) - infoLoss();
    }

    /**
     * Calculates the information loss of this cluster when adding the given input
     * @param input Cluster to consider for the information loss calculation
     * @return Information loss value with input
     */
    public float informationLossWith(Cluster input) {
        return informationLossWith(input.getAllEntries());
    }

    /**
     * Calculates the information loss of this cluster when adding the given input
     * @param input Tuple to consider for the information loss calculation
     * @return Information loss value with input
     */
    public float informationLossWith(Tuple input) {
        return informationLossWith(Collections.singletonList(input));
    }

    /**
     * Calculates the information loss of this cluster when adding the given input
     * @param input List of tuples to consider for the information loss calculation
     * @return Information loss value with input
     */
    private float informationLossWith(List<Tuple> input) {
        boolean singularInput = (input.size() == 1);

        if(entries.size() <= 0){
            LOG.error("informationLossWith() called on cluster with size: 0");
            return 0.0f;
        }
        double[] infoLossWith = new double[config.length];

        for (int i = 0; i < config.length; i++) {
            // Check if the value needs to be generalized and if there is an existing cache for it. If so return it
            if(CACHE_INFO_LOSS_PER_ATT && singularInput && !config[i].getGeneralizationType().equals(CastleFunction.Generalization.NONE)) {
                if(infoLossValuesRequests.size() <= i) {
                    while (infoLossValuesRequests.size() <= i){
                        // Use a LinkedHashMap to limit size of cache. Set MAX_CACHED_ENTRIES for size of cache per generalized attribute
                        infoLossValuesRequests.add(new LinkedHashMap<Object, Double>(MAX_CACHED_ENTRIES*10/7, 0.7f, true) {
                            @Override
                            protected boolean removeEldestEntry(Map.Entry<Object, Double> eldest) {
                                return size() > MAX_CACHED_ENTRIES;
                            }
                        });
                    }
                }
                Map<Object, Double> requestCounter = infoLossValuesRequests.get(i);
                Object valueToCheck = input.get(0).getField(i);

                if(requestCounter.containsKey(valueToCheck)){
                    infoLossWith[i] = requestCounter.get(valueToCheck);
                    continue;
                }
            }

            // If no cache is hit calculate information loss with input
            switch (config[i].getGeneralizationType()) {
                case NONE:
                    infoLossWith[i] = 0;
                    break;
                case REDUCTION:
                case REDUCTION_WITHOUT_GENERALIZATION:
                    infoLossWith[i] = reductGeneralizer.generalize(input, i).f1;
                    break;
                case AGGREGATION:
                case AGGREGATION_WITHOUT_GENERALIZATION:
                    infoLossWith[i] = aggreGeneralizer.generalize(input, i).f1;
                    break;
                case NONNUMERICAL:
                case NONNUMERICAL_WITHOUT_GENERALIZATION:
                    infoLossWith[i] = nonNumGeneralizer.generalize(input, i).f1;
                    break;
                default:
                    LOG.error("informationLossWith() -> undefined transformation type: {}", config[i]);
            }
            // Add info loss to cache if input is singular and not of type NONE
            if(CACHE_INFO_LOSS_PER_ATT && singularInput && !config[i].getGeneralizationType().equals(CastleFunction.Generalization.NONE)) infoLossValuesRequests.get(i).put(input.get(0).getField(i), infoLossWith[i]);
        }
        return calcCombinedInfoLoss(infoLossWith);
    }

    /**
     * Calculates the information loss of this cluster
     * @return Information loss value of the cluster
     */
    public float infoLoss() {
        // check if cached value exist
        if(cachedInfoLoss != -1) return cachedInfoLoss;
        if(entries.size() <= 0){
            LOG.error("infoLoss() called on cluster with size: 0");
            return 0.0f;
        }
        if(config.length <= 0){
            LOG.error("infoLoss() called without any config/rules");
            return 0.0f;
        }
        double[] infoLoss = new double[config.length];

        for (int i = 0; i < config.length; i++) {
            switch (config[i].getGeneralizationType()) {
                case NONE:
                    infoLoss[i] = 0;
                    break;
                case REDUCTION:
                case REDUCTION_WITHOUT_GENERALIZATION:
                    infoLoss[i] = reductGeneralizer.generalize(i).f1;
                    break;
                case AGGREGATION:
                case AGGREGATION_WITHOUT_GENERALIZATION:
                    infoLoss[i] = aggreGeneralizer.generalize(i).f1;
                    break;
                case NONNUMERICAL:
                case NONNUMERICAL_WITHOUT_GENERALIZATION:
                    infoLoss[i] = nonNumGeneralizer.generalize(i).f1;
                    break;
                default:
                    LOG.error("infoLoss() -> undefined transformation type: {}", config[i]);
            }
        }
        float output = calcCombinedInfoLoss(infoLoss);
        // cache infoLoss until new tuple is added
        cachedInfoLoss = output;
        return output;
    }

    /**
     * Calculates the combined information loss for the cluster using the provided info loss values.
     * If all InfoLoss rule multiplier combined are equal to 1 the Normalized Certainty Penalty is used.
     * If not a normal average calculation is used.
     * @param infoLoss Information Loss values to use to calculate combined information loss result
     * @return General information loss for provided values using the clusters config
     */
    private float calcCombinedInfoLoss(double[] infoLoss) {
        // Check what methode should be used for the general information loss (see Doc.)
        double multiplierSum = Arrays.stream(config).mapToDouble(CastleRule::getInfoLossMultiplier).sum();
        int numAppliedGeneralizers = 0;
        float output;
        if(Math.round(multiplierSum) == 1){
            // Multiply info loss values with provided info loss multiplier and sum together
            for (int i = 0; i < config.length; i++) {
                if(config[i].getGeneralizationType() != CastleFunction.Generalization.NONE){
                    infoLoss[i] = config[i].getInfoLossMultiplier() * infoLoss[i];
                    numAppliedGeneralizers++;
                }
            }
            output = (float) Arrays.stream(infoLoss).sum();
        }else{
            // Count number of generalizers to calculate avg info loss
            for (CastleRule castleRule : config) {
                if (castleRule.getGeneralizationType() != CastleFunction.Generalization.NONE) {
                    numAppliedGeneralizers++;
                }
            }
            // Calculate the avg off all info loss values
            output = (float) (Arrays.stream(infoLoss).sum() / numAppliedGeneralizers);
        }
        // Return 0.0 if no generalization rules are present else output
        if(numAppliedGeneralizers == 0) return 0.0f;
        return output;
    }

    /**
     * Creates a generalized tuple based on the currently included data tuples in 'entries' and
     * the provided rules. Data fields that are not generalized use the provided value from the input.
     * @param input Tuple to use for field values that are not generalized
     * @return Generalized tuple
     */
    public Tuple generalize(Tuple input) {
        // Return new tuple with generalized field values
        int inputArity = input.getArity();
        if(showInfoLoss) inputArity++;
        Tuple output = Tuple.newInstance(inputArity);

        if(entries.size() <= 0){
            LOG.error("generalize(Tuple) called on cluster with size: 0");
            return output;
        }

        for (int i = 0; i < Math.min(inputArity, config.length); i++) {
            switch (config[i].getGeneralizationType()) {
                case REDUCTION:
                    output.setField(reductGeneralizer.generalize(i).f0, i);
                    break;
                case AGGREGATION:
                    output.setField(aggreGeneralizer.generalize(i).f0, i);
                    break;
                case NONNUMERICAL:
                    output.setField(nonNumGeneralizer.generalize(i).f0, i);
                    break;
                case NONE:
                case REDUCTION_WITHOUT_GENERALIZATION:
                case AGGREGATION_WITHOUT_GENERALIZATION:
                case NONNUMERICAL_WITHOUT_GENERALIZATION:
                    output.setField(input.getField(i), i);
                    break;
                default:
                    LOG.error("generalize -> undefined transformation type: {}", config[i]);
            }
        }
        if(showInfoLoss) output.setField(infoLoss(),inputArity-1);
        return output;
    }

    /**
     * Generalizes the input tuple with the maximum generalization of the generalizers
     * (Can be used on a cluster without entries)
     * @param input Tuple to generalize
     * @return Maximal generalized tuple
     */
    public Tuple generalizeMax(Tuple input) {
        // Return new tuple with generalized field values
        int inputArity = input.getArity();
        if(showInfoLoss) inputArity++;
        Tuple output = Tuple.newInstance(inputArity);

        for (int i = 0; i < Math.min(inputArity, config.length); i++) {
            switch (config[i].getGeneralizationType()) {
                case REDUCTION:
                    output.setField(reductGeneralizer.generalizeMax(i).f0, i);
                    break;
                case AGGREGATION:
                    output.setField(aggreGeneralizer.generalizeMax(i).f0, i);
                    break;
                case NONNUMERICAL:
                    output.setField(nonNumGeneralizer.generalizeMax(i).f0, i);
                    break;
                case NONE:
                case REDUCTION_WITHOUT_GENERALIZATION:
                case AGGREGATION_WITHOUT_GENERALIZATION:
                case NONNUMERICAL_WITHOUT_GENERALIZATION:
                    output.setField(input.getField(i), i);
                    break;
                default:
                    LOG.error("generalizeMax -> undefined transformation type: {}", config[i]);
            }
        }
        if(showInfoLoss) output.setField(1.0f,inputArity-1);
        return output;
    }

    public void addEntry(Tuple input) {
        if(CACHE_INFO_LOSS_PER_ATT){
            for (Map<Object, Double> cacheMap: infoLossValuesRequests) {
                cacheMap.clear();
            }
        }

        if (showAddedEntry) System.out.println("Added " + input.toString() + " to cluster: " + this + " size:" + this.entries.size());
        entries.add(input);
        // Invalidate cache of infoLoss
        cachedInfoLoss = -1;
        // Update the aggregation boundaries
        aggreGeneralizer.updateAggregationBounds(input);
        nonNumGeneralizer.updateTree(input);
    }

    public void addAllEntries(ArrayList<Tuple> input) {
        if(CACHE_INFO_LOSS_PER_ATT){
            for (Map<Object, Double> cacheMap: infoLossValuesRequests) {
                cacheMap.clear();
            }
        }

        if (showAddedEntry) System.out.println("Added multiple (" + input.size() + ") to cluster: " + this + " size:" + this.entries.size());
        entries.addAll(input);
        // Invalidate cache of infoLoss
        cachedInfoLoss = -1;
        // Update the aggregation boundaries for all inputs
        for (Tuple in : input) {
            aggreGeneralizer.updateAggregationBounds(in);
            nonNumGeneralizer.updateTree(in);
        }
    }

    public void removeEntry(Tuple input) {
        entries.remove(input);
        if(showRemoveEntry) System.out.println("Cluster: removeEntry -> new size:" + entries.size());
    }

    public void removeAllEntries(ArrayList<Tuple> idTuples) {
        entries.removeAll(idTuples);
        if(showRemoveEntry) System.out.println("Cluster: removeAllEntry -> new size:" + entries.size());
    }

    public ArrayList<Tuple> getAllEntries() {
        return entries;
    }

    public boolean contains(Tuple input) {
        return entries.contains(input);
    }

    /**
     * Returns the number of distinct individuals inside the cluster
     * @return Number of distinct values for the posTupleId inside entries
     */
    public int size() {
        // TODO-later maybe find better performing solution. See: https://stackoverflow.com/questions/17973098/is-there-a-faster-way-to-extract-unique-values-from-object-collection
        Set<Object> tupleIds = new HashSet<>();
        for(final Tuple entry: entries) {
            tupleIds.add(entry.getField(posTupleId));
        }
        return tupleIds.size();
    }

    /**
     * Returns the diversity of the cluster entries
     * @return cluster diversity
     */
    public int diversity(int[] posSensibleAttributes) {
        if(posSensibleAttributes.length <= 0) return 0;
        if(posSensibleAttributes.length == 1){
            // Return the amount of different values inside the sensible attribute
            Set<Object> output = new HashSet<>();
            for(Tuple tuple: entries){
                int pos = posSensibleAttributes[0];
                // Check for arrays
                Object sensAttributeValue = tuple.getField(pos).getClass().isArray() ? ((Object[]) tuple.getField(pos))[((Object[]) tuple.getField(pos)).length-1] : tuple.getField(pos);
                output.add(sensAttributeValue);
            }
            return output.size();
        }else{
            // See for concept: https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1
            @SuppressWarnings("unchecked")
            ArrayList<Tuple> entriesCopy = (ArrayList<Tuple>) entries.clone();
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            int counter = 0;

            while (entriesCopy.size() > 0) {
                counter++;
                for (int pos : posSensibleAttributes) {
                    //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::getValue)'
                    Map.Entry<Object, Long> temp = entriesCopy.stream().collect(Collectors.groupingBy(s -> (s.getField(pos).getClass().isArray() ? ((Object[]) s.getField(pos))[((Object[]) s.getField(pos)).length-1] : s.getField(pos)), Collectors.counting()))
                        .entrySet().stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.getValue(), attEntry2.getValue())).orElse(null);
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::f1.getValue)'
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.f1.getValue(), attEntry2.f1.getValue())).get();
                // Remove all entries that have the least diverse attribute
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                for(Tuple tuple: entriesCopy){
                    // adjust to find also values from arrays
                    Object toCompare = (tuple.getField(mapEntryToDelete.f0).getClass().isArray() ? ((Object[]) tuple.getField(mapEntryToDelete.f0))[((Object[]) tuple.getField(mapEntryToDelete.f0)).length-1] : tuple.getField(mapEntryToDelete.f0));
                    if(toCompare.equals(mapEntryToDelete.f1.getKey())){
                        tuplesToDelete.add(tuple);
                    }
                }
                entriesCopy.removeAll(tuplesToDelete);
                numOfAppearances.clear();
            }
            return counter;
        }
    }
}
