package pringtest.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import pringtest.CastleFunction;
import pringtest.datatypes.CastleRule;
import pringtest.datatypes.TreeNode;

import java.util.HashMap;
import java.util.List;

public class AggregationGeneralizer implements BaseGeneralizer{

    private final CastleRule[] config;
    private final HashMap<Integer, Tuple2<Float, Float>> aggregationRanges = new HashMap<>();
    private final HashMap<Integer, Tuple2<Float, Float>> domainRanges = new HashMap<>();

    public AggregationGeneralizer(CastleRule[] rules){
        this.config = rules;
        // Add rule defined domain ranges to the generalizer
        for(int i = 0; i < rules.length; i++){
            if(rules[i].getGeneralizationType() == CastleFunction.Generalization.AGGREGATION && rules[i].getDomain() != null){
                domainRanges.put(i, rules[i].getDomain());
            }
        }
    }

    /**
     * Returns the previous recorded Aggregation bounds for a given position
     * (see Cluster.addEntry() and Cluster.addAllEntries()) and the produced information loss
     * @param pos Position inside the entry tuple
     * @return A Tuple2 with [Aggregation bounds as Tuple2<Float, Float>, InfoLoss]
     */
    @Override
    public Tuple2<Tuple2<Float, Float>, Float> generalize(int pos) {
        Tuple2<Float, Float> borders = Tuple2.of(aggregationRanges.get(pos).f0, aggregationRanges.get(pos).f1);
        return Tuple2.of(borders, infoLoss(pos, aggregationRanges.get(pos).f0, aggregationRanges.get(pos).f1));
    }

    @Override
    public Tuple2<Tuple2<Float, Float>, Float> generalize(List<Tuple> withTuples, int pos) {
        float min = aggregationRanges.get(pos).f0;
        float max = aggregationRanges.get(pos).f1;

        for(Tuple input: withTuples){
            min = Math.min(input.getField(pos), min);
            max = Math.max(input.getField(pos), max);
        }
        Tuple2<Float, Float> borders = Tuple2.of(min, max);
        return Tuple2.of(borders, infoLoss(pos, min, max));
    }

    /**
     * Calculate the information loss using the domain range and the min, max values of the generalization
     * @param pos attribute position
     * @param min min value of generalization
     * @param max max value of generalization
     * @return information loss of the give position and values
     */
    private float infoLoss(int pos, float min, float max){
        return ((max - min) / (domainRanges.get(pos).f1 - domainRanges.get(pos).f0));
    }
    
    /**
     * Update the upper and lower bound of aggregated fields
     * @param input the new added tuple
     */
    public void updateAggregationBounds(Tuple input) {
        for (int i = 0; i < config.length; i++) {
            if (config[i].getGeneralizationType() == CastleFunction.Generalization.AGGREGATION) {
                if (!aggregationRanges.containsKey(i)) {
                    aggregationRanges.put(i, new Tuple2<>(input.getField(i), input.getField(i)));
                } else {
                    aggregationRanges.get(i).f0 = Math.min(input.getField(i), aggregationRanges.get(i).f0);
                    aggregationRanges.get(i).f1 = Math.max(input.getField(i), aggregationRanges.get(i).f1);
                }
                updateDomainBounds(i);
            }
        }
    }

    /**
     * Checks if new added values are bigger than the defined domain ranges.
     * If that is the case the domain range gets set to the new min/max values
     * @param pos parameter position
     */
    private void updateDomainBounds(int pos){
        if(domainRanges.containsKey(pos)){
            domainRanges.get(pos).f0 = Math.min(aggregationRanges.get(pos).f0, domainRanges.get(pos).f0);
            domainRanges.get(pos).f1 = Math.max(aggregationRanges.get(pos).f0, domainRanges.get(pos).f1);
        }else{
            domainRanges.put(pos, Tuple2.of(aggregationRanges.get(pos).f0, aggregationRanges.get(pos).f1));
        }
    }
}
