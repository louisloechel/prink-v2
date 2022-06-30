package pringtest.datatypes;

import org.apache.flink.api.java.tuple.*;
import pringtest.generalizations.AggregationGeneralizer;
import pringtest.generalizations.NonNumericalGeneralizer;
import pringtest.generalizations.ReductionGeneralizer;

import java.util.*;

public class Cluster {

    private final CastleRule[] config;

    private final ArrayList<Tuple> entries = new ArrayList<>();

    private final AggregationGeneralizer aggreGeneralizer;
    private final ReductionGeneralizer reductGeneralizer;
    private final NonNumericalGeneralizer nonNumGeneralizer;

    // DEBUG params
    boolean showRemoveEntry = false;
    boolean showAddedEntry = false;
    boolean showInfoLoss = false;
    boolean showEnlargement = false;

    public Cluster(CastleRule[] rules) {
        // TODO-Later maybe remove since values without rules are rejected
        this.config = (rules == null) ? new CastleRule[]{} : rules;
        aggreGeneralizer = new AggregationGeneralizer(config);
        reductGeneralizer = new ReductionGeneralizer(this);
        nonNumGeneralizer = new NonNumericalGeneralizer(config);
    }

    public Float enlargementValue(Cluster input) {
        if(showEnlargement) System.out.println("Enlargement Value Cluster:" + (informationLossWith(input) - infoLoss()));
        return informationLossWith(input) - infoLoss();
    }

    public Float enlargementValue(Tuple input) {
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
                    System.out.println("ERROR: inside Cluster: undefined transformation type:" + config[i]);
            }
        }
        double sumWith = Arrays.stream(infoLossWith).sum();
        if(showInfoLoss) System.out.println("InfoLossTuple with: " + Arrays.toString(infoLossWith) + " Result:" + ((float) sumWith) / config.length);
        return ((float) sumWith) / config.length;
    }

    public float infoLoss() {
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
                    System.out.println("ERROR: inside Cluster: undefined transformation type:" + config[i]);
            }
        }
        double sumWith = Arrays.stream(infoLoss).sum();
        if(showInfoLoss) System.out.println("InfoLoss with: " + Arrays.toString(infoLoss) + " Result:" + ((float) sumWith) / config.length);
        return ((float) sumWith) / config.length;
    }

    public Tuple generalize(Tuple input) {

        Object[] fieldValues = new Object[config.length];

        for (int i = 0; i < config.length; i++) {
            switch (config[i].getGeneralizationType()) {
                case REDUCTION:
                    fieldValues[i] = reductGeneralizer.generalize(i).f0;
                    break;
                case AGGREGATION:
                    fieldValues[i] = aggreGeneralizer.generalize(i).f0;
                    break;
                case NONNUMERICAL:
                    fieldValues[i] = nonNumGeneralizer.generalize(i).f0;
                    break;
                case NONE:
                case REDUCTION_WITHOUT_GENERALIZATION:
                case AGGREGATION_WITHOUT_GENERALIZATION:
                case NONNUMERICAL_WITHOUT_GENERALIZATION:
                    fieldValues[i] = input.getField(i);
                    break;
                default:
                    System.out.println("ERROR: inside Cluster: undefined transformation type:" + config[i]);
            }
        }
        // Return new tuple with generalized field values
        int inputArity = input.getArity();
        Tuple output = Tuple.newInstance(inputArity);
        for (int i = 0; i < Math.min(inputArity, config.length); i++) {
            output.setField(fieldValues[i], i);
        }
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

    public int size() {
        return entries.size();
    }

    /**
     * Returns the diversity of the cluster entries
     * @return cluster diversity
     */
    public int diversity(int[] posSensibleAttributes) {
        // TODO maybe calculate on insert to save performance
        return getSensibleValues(posSensibleAttributes).size();
    }

    public Collection<String> getSensibleValues(int[] posSensibleAttributes) {
        // TODO check if better sensible attribute combiner exists
        Set<String> output = new HashSet<>();
        for(Tuple tuple: entries){
            StringBuilder builder = new StringBuilder();
            for(int pos: posSensibleAttributes){
                builder.append(tuple.getField(pos).toString());
                // Add separator to prevent attribute mismatching
                builder.append(";");
            }
            output.add(builder.toString());
        }
        return output;
    }
}
