package pringtest.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import pringtest.CastleFunction;

import java.util.HashMap;
import java.util.List;

public class AggregationGeneralizer implements BaseGeneralizer{

    private final CastleFunction.Generalization[] config;
    private final HashMap<Integer, Tuple2<Float, Float>> aggregationRanges = new HashMap<>();

    public AggregationGeneralizer(CastleFunction.Generalization[] config){
        this.config = config;
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
        // TODO update with more accurate information loss function
        float infoLoss = aggregationRanges.get(pos).f1 - aggregationRanges.get(pos).f0;
        return Tuple2.of(borders, infoLoss);
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
        // TODO update with more accurate information loss function
        float infoLoss = max - min;
        return Tuple2.of(borders, infoLoss);
    }
    
    /**
     * Update the upper and lower bound of aggregated fields
     * @param input the new added tuple
     */
    public void updateAggregationBounds(Tuple input) {
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
}
