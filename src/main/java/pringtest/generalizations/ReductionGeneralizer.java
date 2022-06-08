package pringtest.generalizations;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import pringtest.datatypes.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ReductionGeneralizer implements BaseGeneralizer{

    private Cluster cluster;

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

    @Override
    public Tuple2<String, Float> generalize(List<Tuple> withTuples, int pos) {

        ArrayList<Tuple> entries = cluster.getAllEntries();

        int arraySize = (withTuples == null) ? entries.size() : (entries.size() + withTuples.size());
        if(arraySize <= 0){
            System.out.println("FIXME: generalizeReduction called with length of 0. Cluster size:" + entries.size());
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
        return Tuple2.of(ids[0], numReducted);
    }
}
