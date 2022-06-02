package pringtest.datatypes;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.*;
import pringtest.CastleFunction;

import java.util.*;
import java.util.stream.Stream;

public class Cluster {

    private final CastleFunction.Generalization[] config;

    private ArrayList<Tuple> entries = new ArrayList<>();
    private HashMap<Integer, Tuple2<Float, Float>> aggregationRanges = new HashMap<>();

    // DEBUG params
    boolean showAddedEntry = false;
    boolean showInfoLoss = false;

    public Cluster(CastleFunction.Generalization[] config) {
        this.config = config;
    }

    // TODO check if float is needed or can be replaced with int
    public Float enlargementValue(Cluster input) {
        return informationLossWith(input) - infoLoss();
    }

    // TODO check if float is needed or can be replaced with int
    public Float enlargementValue(Tuple input) {
        return informationLossWith(input) - infoLoss();
    }

    // TODO check if informationLossWith is the total loss or only in relation to input
    public float informationLossWith(Cluster input) {
        double[] infoLossWith = new double[config.length];

        for (int i = 0; i < config.length; i++) {
            switch (config[i]) {
                case NONE:
                    infoLossWith[i] = 0;
                    break;
                case REDUCTION:
                    infoLossWith[i] = generalizeReduction(input.getAllEntries(), i).f1;
                    break;
                case AGGREGATION:
//                    fieldValues[i] = generalizeAggregation(i); TODO
                    infoLossWith[i] = 1;
                    break;
                default:
                    System.out.println("ERROR: inside Cluster: undefined transformation type:" + config[i]);
            }
        }
        double sumWith = Arrays.stream(infoLossWith).sum();
        if(showInfoLoss) System.out.println("InfoLossTuple Cluster with: " + Arrays.toString(infoLossWith));
        return (float) sumWith;
    }

    public float informationLossWith(Tuple input) {

        double[] infoLossWith = new double[config.length];

        for (int i = 0; i < config.length; i++) {
            switch (config[i]) {
                case NONE:
                    infoLossWith[i] = 0;
                    break;
                case REDUCTION:
                    infoLossWith[i] = generalizeReduction(Collections.singletonList(input), i).f1;
                    break;
                case AGGREGATION:
//                    fieldValues[i] = generalizeAggregation(i); TODO
                    infoLossWith[i] = 1;
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
                    infoLoss[i] = generalizeReduction(i).f1;
                    break;
                case AGGREGATION:
//                    fieldValues[i] = generalizeAggregation(i); TODO
                    infoLoss[i] = 1;
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
                    fieldValues[i] = generalizeReduction(i).f0;
                    break;
                case AGGREGATION:
                    fieldValues[i] = generalizeAggregation(i);
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
        updateAggregationBounds(input);
    }

    public void addAllEntries(ArrayList<Tuple> input) {
        if (showAddedEntry) System.out.println("Added multiple (" + input.size() + ") to cluster: " + this.toString() + " size:" + this.entries.size());
        entries.addAll(input);
        // Update the aggregation boundaries for all inputs
        for (Tuple in : input) {
            updateAggregationBounds(in);
        }
    }

    public void removeEntry(Object input) {
        entries.remove(input);
    }

    public ArrayList<Tuple> getAllEntries() {
        return entries;
    }

    public boolean contains(Object input) {
        return entries.contains(input);
    }

    public int size() {
        return entries.size();
    }

    /**
     * Update the upper and lower bound of aggregated fields
     * @param input the new added tuple
     */
    private void updateAggregationBounds(Tuple input) {
        for (int i = 0; i < config.length; i++) {
            if (config[i] == CastleFunction.Generalization.AGGREGATION) {
                if (!aggregationRanges.containsKey(i)) {
                    aggregationRanges.put(i, new Tuple2<>(input.getField(i), input.getField(i)));
                } else {
                    aggregationRanges.get(i).f0 = Math.min(input.getField(i), aggregationRanges.get(i).f0);
                    aggregationRanges.get(i).f1 = Math.max(input.getField(i), aggregationRanges.get(i).f1);
                }
            }
        }
    }

    /**
     * Reduces information by replacing the last values with '*' until all values are the same
     * @param pos Position of the value inside the tuple
     * @return Tuple2<String, Float> where f1 is the reduced value as a String
     * and f2 is the information loss of the generalization
     */
    private Tuple2<String, Float> generalizeReduction(int pos) {
        return generalizeReduction(null, pos);
    }

    private Tuple2<String, Float> generalizeReduction(List<Tuple> withTuples, int pos) {

        int arraySize = (withTuples == null) ? entries.size() : (entries.size() + withTuples.size());
        if(arraySize <= 0){
            System.out.println("FIXME: generalizeReduction called with length of 0. Cluster size:" + this.entries.size());
            return new Tuple2<>("",0f);
        }
        String[] ids = new String[arraySize];

        for (int i = 0; i < entries.size(); i++) {
            Long temp = entries.get(i).getField(pos);
            ids[i] = String.valueOf(temp);
        }

        if(withTuples != null){
            for (int i = 0; i < withTuples.size(); i++) {
                Long temp = withTuples.get(i).getField(pos);
                ids[i + entries.size()] = String.valueOf(temp);
            }
        }

        int maxLength = Stream.of(ids).map(String::length).max(Integer::compareTo).get();

        // count number of reducted chars to calculate a information loss value
        float numReducted = 0;

        for (int i = 0; i < maxLength; i++) {
            for (int j = 0; j < ids.length; j++) {
                String overlay = StringUtils.repeat('*', i);
                ids[j] = StringUtils.overlay(ids[j], overlay, ids[j].length() - i, ids[j].length());
            }
            numReducted++;
            // Break loop when all values are the same
            // TODO check if HashSet methode to find count is faster
            if (Stream.of(ids).distinct().count() <= 1) break;

        }
        return new Tuple2<>(ids[0], numReducted);
    }

    /**
     * Returns the previous recorded Aggregation bounds for a given position
     * (see addEntry() and addAllEntries())
     * @param pos Position inside the entry tuple
     * @return aggregation bounds ad Tuple2<Float, Float>
     */
    private Tuple2<Float, Float> generalizeAggregation(int pos) {
        return Tuple2.of(aggregationRanges.get(pos).f0, aggregationRanges.get(pos).f1);
    }
}
