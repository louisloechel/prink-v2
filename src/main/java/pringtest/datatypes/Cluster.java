package pringtest.datatypes;

import org.apache.flink.api.java.tuple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pringtest.CastleFunction;
import pringtest.generalizations.AggregationGeneralizer;
import pringtest.generalizations.NonNumericalGeneralizer;
import pringtest.generalizations.ReductionGeneralizer;

import java.util.*;
import java.util.stream.Collectors;

public class Cluster {

    private final CastleRule[] config;
    private final int posTupleId;

    private final ArrayList<Tuple> entries = new ArrayList<>();

    private final AggregationGeneralizer aggreGeneralizer;
    private final ReductionGeneralizer reductGeneralizer;
    private final NonNumericalGeneralizer nonNumGeneralizer;

    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    // DEBUG params
    boolean showRemoveEntry = false;
    boolean showAddedEntry = false;
    boolean showInfoLoss = false;
    boolean showEnlargement = false;

    public Cluster(CastleRule[] rules, int posTupleId, boolean showInfoLoss) {
        this.config = rules;
        this.posTupleId = posTupleId;
        this.showInfoLoss = showInfoLoss;
        aggreGeneralizer = new AggregationGeneralizer(config);
        reductGeneralizer = new ReductionGeneralizer(this);
        nonNumGeneralizer = new NonNumericalGeneralizer(config);
    }

    public Float enlargementValue(Cluster input) {
        if(entries.size() <= 0) LOG.error("enlargementValue(Cluster) called on cluster with size: 0 ");
        if(showEnlargement) System.out.println("Enlargement Value Cluster:" + (informationLossWith(input) - infoLoss()));
        return informationLossWith(input) - infoLoss();
    }

    public Float enlargementValue(Tuple input) {
        if(entries.size() <= 0) LOG.error("enlargementValue(tuple) called on cluster with size: 0");
        if(showEnlargement) System.out.println("Enlargement Value Tuple:" + (informationLossWith(input) - infoLoss()));
        return informationLossWith(input) - infoLoss();
    }

    public float informationLossWith(Cluster input) {
        return informationLossWith(input.getAllEntries());
    }

    public float informationLossWith(Tuple input) {
        return informationLossWith(Collections.singletonList(input));
    }

    private float informationLossWith(List<Tuple> input) {
        if(entries.size() <= 0){
            LOG.error("informationLossWith() called on cluster with size: 0");
            return 0.0f;
        }
        double[] infoLossWith = new double[config.length];

        for (int i = 0; i < config.length; i++) {
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
        }
        return calcCombinedInfoLoss(infoLossWith);
    }

    public float infoLoss() {
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
        return calcCombinedInfoLoss(infoLoss);
    }

    /**
     * Calculates the combined information loss for the cluster using the provided info loss values
     * If all InfoLoss rule multiplier combined are equal to 1 the Normalized Certainty Penalty is used.
     * If not a normal average calculation is used.
     * @param infoLoss Information Loss values to use to calculate combined information loss result
     * @return General information loss for provided values using the clusters config
     */
    private float calcCombinedInfoLoss(double[] infoLoss) {
        // Check what methode should be used for the general information loss (see Doc.)
        double multiplierSum = Arrays.stream(config).mapToDouble(CastleRule::getInfoLossMultiplier).sum();
        if(Math.round(multiplierSum) == 1){
            // Multiply info loss values with provided info loss multiplier and sum together
            for (int i = 0; i < config.length; i++) {
                if(config[i].getGeneralizationType() != CastleFunction.Generalization.NONE){
                    infoLoss[i] = config[i].getInfoLossMultiplier() * infoLoss[i];
                }
            }
            return (float) Arrays.stream(infoLoss).sum();
        }else{
            // Count number of generalizers to calculate avg info loss
            int numAppliedGeneralizers = 0;
            for (CastleRule castleRule : config) {
                if (castleRule.getGeneralizationType() != CastleFunction.Generalization.NONE) {
                    numAppliedGeneralizers++;
                }
            }
            // Calculate the avg off all info loss values
            return (float) (Arrays.stream(infoLoss).sum() / numAppliedGeneralizers);
        }
    }

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
     * (Can be used on an cluster without entries)
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
        if (showAddedEntry) System.out.println("Added " + input.toString() + " to cluster: " + this.toString() + " size:" + this.entries.size());
        entries.add(input);
        // Update the aggregation boundaries
        aggreGeneralizer.updateAggregationBounds(input);
        nonNumGeneralizer.updateTree(input);
    }

    public void addAllEntries(ArrayList<Tuple> input) {
        if (showAddedEntry) System.out.println("Added multiple (" + input.size() + ") to cluster: " + this.toString() + " size:" + this.entries.size());
        entries.addAll(input);
        // Update the aggregation boundaries for all inputs
        for (Tuple in : input) {
            aggreGeneralizer.updateAggregationBounds(in);
            nonNumGeneralizer.updateTree(in);
        }
    }

    public void removeEntry(Tuple input) {
        // TODO check if boundaries need to be adjusted when removing tuples
        entries.remove(input);
        if(showRemoveEntry) System.out.println("Cluster: removeEntry -> new size:" + entries.size());
    }

    public void removeAllEntries(ArrayList<Tuple> idTuples) {
        // TODO check if boundaries need to be adjusted when removing tuples
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
        // TODO change to tupleIds and maybe find better performing solution. See: https://stackoverflow.com/questions/17973098/is-there-a-faster-way-to-extract-unique-values-from-object-collection
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
            // TODO test if correct (if object can be compared with object)
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
            ArrayList<Tuple> entriesCopy = (ArrayList<Tuple>) entries.clone();
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            int counter = 0;

            while (entriesCopy.size() > 0) {
                counter++;
                for (int pos : posSensibleAttributes) {
                    // TODO check if two strings are added to the same grouping if they have the same value but are different objects
                    Map.Entry<Object, Long> temp = entriesCopy.stream().collect(Collectors.groupingBy(s -> (s.getField(pos).getClass().isArray() ? ((Object[]) s.getField(pos))[((Object[]) s.getField(pos)).length-1] : s.getField(pos)), Collectors.counting()))
                            .entrySet().stream().max((attEntry1, attEntry2) -> attEntry1.getValue() > attEntry2.getValue() ? 1 : -1).get();
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> attEntry1.f1.getValue() > attEntry2.f1.getValue() ? 1 : -1).get();
                // Remove all entries that have the least diverse attribute
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                for(Tuple tuple: entriesCopy){
                    // adjust to find also values from arrays TODO re-write
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
