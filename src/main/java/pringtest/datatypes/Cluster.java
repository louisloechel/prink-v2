package pringtest.datatypes;

import org.apache.flink.api.java.tuple.*;
import pringtest.CastleFunction;
import pringtest.generalizations.AggregationGeneralizer;
import pringtest.generalizations.NonNumericalGeneralizer;
import pringtest.generalizations.ReductionGeneralizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Cluster {

    private final CastleFunction.Generalization[] config;

    private final ArrayList<Tuple> entries = new ArrayList<>();

    private final AggregationGeneralizer aggreGeneralizer;
    private final ReductionGeneralizer reductGeneralizer;
    private final NonNumericalGeneralizer nonNumGeneralizer;

    // DEBUG params
    boolean showRemoveEntry = false;
    boolean showAddedEntry = false;
    boolean showInfoLoss = false;

    public Cluster(CastleFunction.Generalization[] config) {
        this.config = config;
        aggreGeneralizer = new AggregationGeneralizer(config);
        reductGeneralizer = new ReductionGeneralizer(this);
        nonNumGeneralizer = new NonNumericalGeneralizer(config);
    }

    // TODO check if float is needed or can be replaced with int
    public Float enlargementValue(Cluster input) {
        return informationLossWith(input) - infoLoss();
    }

    // TODO check if float is needed or can be replaced with int
    public Float enlargementValue(Tuple input) {
        return informationLossWith(input) - infoLoss();
    }

    public float informationLossWith(Cluster input) {
        return informationLossWith(input.getAllEntries());
    }

    public float informationLossWith(Tuple input) {
        return informationLossWith(Collections.singletonList(input));
    }

    // TODO check if informationLossWith is the total loss or only in relation to input
    private float informationLossWith(List<Tuple> input) {
        double[] infoLossWith = new double[config.length];

        for (int i = 0; i < config.length; i++) {
            switch (config[i]) {
                case NONE:
                    infoLossWith[i] = 0;
                    break;
                case REDUCTION:
                    infoLossWith[i] = reductGeneralizer.generalize(input, i).f1;
                    break;
                case AGGREGATION:
                    infoLossWith[i] = aggreGeneralizer.generalize(input, i).f1;
                    break;
                case NONNUMERICAL:
                    infoLossWith[i] = nonNumGeneralizer.generalize(i).f1;
                    break;
                default:
                    System.out.println("ERROR: inside Cluster: undefined transformation type:" + config[i]);
            }
        }
        double sumWith = Arrays.stream(infoLossWith).sum();
        if(showInfoLoss) System.out.println("InfoLossTuple with: " + Arrays.toString(infoLossWith));
        return (float) sumWith;
    }

    public float infoLoss() {
        double[] infoLoss = new double[config.length];

        for (int i = 0; i < config.length; i++) {
            switch (config[i]) {
                case NONE:
                    infoLoss[i] = 0;
                    break;
                case REDUCTION:
                    infoLoss[i] = reductGeneralizer.generalize(i).f1;
                    break;
                case AGGREGATION:
                    infoLoss[i] = aggreGeneralizer.generalize(i).f1;
                    break;
                case NONNUMERICAL:
                    infoLoss[i] = nonNumGeneralizer.generalize(i).f1;
                    break;
                default:
                    System.out.println("ERROR: inside Cluster: undefined transformation type:" + config[i]);
            }
        }
        double sumWith = Arrays.stream(infoLoss).sum();
        if(showInfoLoss) System.out.println("InfoLoss with: " + Arrays.toString(infoLoss));
        return (float) sumWith;
    }

    public Tuple generalize(Tuple input) {
//        TODO find out how to create tuple from class
//        Class<? extends Tuple> output = Tuple.getTupleClass(config.length);

//        TypeInformation<Tuple> tupleInfo = getReturnValues();
//        Map<String, TypeInformation<?>> tupleTypes = tupleInfo.getGenericParameters();
//        tupleTypes.get("T0");
        Object[] fieldValues = new Object[config.length];

        for (int i = 0; i < config.length; i++) {
            switch (config[i]) {
                case NONE:
                    fieldValues[i] = input.getField(i);
                    break;
                case REDUCTION:
                    fieldValues[i] = reductGeneralizer.generalize(i).f0;
                    break;
                case AGGREGATION:
                    fieldValues[i] = aggreGeneralizer.generalize(i).f0;
                    break;
                case NONNUMERICAL:
                    fieldValues[i] = nonNumGeneralizer.generalize(i).f0;
                    break;
                default:
                    System.out.println("ERROR: inside Cluster: undefined transformation type:" + config[i]);
            }
        }
        // TODO extend to cover all 26 Tuple classes
        switch (config.length) {
            case 1:
                return Tuple1.of(fieldValues[0]);
            case 2:
                return Tuple2.of(fieldValues[0], fieldValues[1]);
            case 3:
                return Tuple3.of(fieldValues[0], fieldValues[1], fieldValues[2]);
            case 4:
                return Tuple4.of(fieldValues[0], fieldValues[1], fieldValues[2], fieldValues[3]);
            case 8:
                return Tuple8.of(fieldValues[0], fieldValues[1], fieldValues[2], fieldValues[3], fieldValues[4], fieldValues[5], fieldValues[6], fieldValues[7]);
            default:
                return new Tuple0();
        }
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
    public int diversity() {
        // TODO calculate diversity
        // TODO maybe calculate on insert to save performance
        return 10;
    }

}
