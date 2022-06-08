package pringtest.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public interface BaseGeneralizer {
    public Tuple2<?, Float> generalize(int pos);
    public Tuple2<?, Float> generalize(List<Tuple> withTuples, int pos);
}
