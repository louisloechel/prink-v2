package pringtest.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public interface BaseGeneralizer {
    /**
     * Returns the generalized value and the resulting information loss
     * @param pos Position inside the tuple to generalize
     * @return Tuple2<Generalized Value, Information Loss>
     */
    public Tuple2<?, Float> generalize(int pos);

    /**
     * Returns the generalized value (while using the provided extra tuples) and the resulting information loss
     * @param pos Position inside the tuple to generalize
     * @return Tuple2<Generalized Value, Information Loss>
     */
    public Tuple2<?, Float> generalize(List<Tuple> withTuples, int pos);
}
