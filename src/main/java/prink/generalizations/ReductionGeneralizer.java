package prink.generalizations;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prink.datatypes.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ReductionGeneralizer implements BaseGeneralizer{

    private final Cluster cluster;

    private static final Logger LOG = LoggerFactory.getLogger(ReductionGeneralizer.class);


    public ReductionGeneralizer(Cluster cluster){
        this.cluster = cluster;
    }

    /**
     * Reduces information by replacing the last values with '*' until all values are the same
     * @param pos Position of the value inside the tuple
     * @return Tuple2<String, Float> where f1 is the reduced value as a String
     * and f2 is the information loss of the generalization
     */
    @Override
    public Tuple2<String, Float> generalize(int pos) {
        return generalize(null, pos);
    }

    /**
     * Reduces information by replacing the last values with '*' until all values are the same, while conserving the additional tuples 'withTuples'
     * @param withTuples Tuples to consider when calculating generalization
     * @param pos Position of the value inside the tuple
     * @return Tuple2<String, Float> where f1 is the reduced value as a String
     * and f2 is the information loss of the generalization
     */
    @Override
    public Tuple2<String, Float> generalize(List<Tuple> withTuples, int pos) {

        ArrayList<Tuple> entries = cluster.getAllEntries();

        int arraySize = (withTuples == null) ? entries.size() : (entries.size() + withTuples.size());
        if(arraySize <= 0){
            LOG.error("ERROR: generalizeReduction called with length of 0. Cluster size:{}", entries.size());
            return new Tuple2<>("",1.0f);
        }
        String[] ids = new String[arraySize];

        for (int i = 0; i < entries.size(); i++) {
            ids[i] = entries.get(i).getField(pos).toString();
        }

        if(withTuples != null){
            for (int i = 0; i < withTuples.size(); i++) {
                ids[i + entries.size()] = withTuples.get(i).getField(pos).toString();
            }
        }

        int maxLength = Stream.of(ids).map(String::length).max(Integer::compareTo).get();

        // count number of reducted chars to calculate a information loss value. Start at -1 to check if nothing needs to be reducted (i=0)
        float numReducted = -1;

        for (int i = 0; i < maxLength+1; i++) {
            for (int j = 0; j < ids.length; j++) {
                String overlay = StringUtils.repeat('*', i);
                ids[j] = StringUtils.overlay(ids[j], overlay, ids[j].length() - i, ids[j].length());
            }
            numReducted++;
            // Break loop when all values are the same
            // TODO-later check if HashSet methode to find count is faster
            if (Stream.of(ids).distinct().count() <= 1) break;
        }
        float infoLoss = (numReducted > 0) ? (numReducted/maxLength) : 0 ;
        return Tuple2.of(ids[0], infoLoss);
    }

    @Override
    public Tuple2<String, Float> generalizeMax(int pos) {
        return Tuple2.of("*", 1.0f);
    }
}
