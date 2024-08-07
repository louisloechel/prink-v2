package ganges;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import prink.CastleFunction;
import prink.datatypes.CastleRule;
import prink.generalizations.*;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class GangesEvaluation {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        GangesEvaluation gangesEvaluation = new GangesEvaluation();
        JobExecutionResult r = gangesEvaluation.execute(params);
        System.out.println(r.toString());
    }

    public JobExecutionResult execute(ParameterTool parameters) throws Exception {

        // process provided job parameters
        int k = parameters.getInt("k");
        int l = parameters.getInt("l");
        int delta = parameters.getInt("delta");
        int beta = parameters.getInt("beta");
        int zeta = parameters.getInt("zeta");
        int mu = parameters.getInt("mu");
        boolean parallel = parameters.getBoolean("parallel", false); // false
        boolean local = parameters.getBoolean("local", true); // true

        String sutHost = parameters.get("sut_host", "localhost");
        int sutPortWrite = parameters.getInt("sut_port_write", 50051);
        int sutPortRead = parameters.getInt("sut_port_read", 50052);

        // Set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceData = env.socketTextStream(sutHost, sutPortWrite);

        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>() {
                        }));

        List<CastleRule> rules = Arrays.stream(DatasetFields.values()).map(f -> new CastleRule(f.getId(), f.getGeneralizer(), f.isSensitive())).collect(Collectors.toList());

        BroadcastStream<CastleRule> ruleBroadcastStream = env.fromCollection(rules)
                .broadcast(ruleStateDescriptor);

        String evalDescription = "Ganges Eval: " + new SimpleDateFormat("yyyy-MM-dd hh-mm-ss").format(new Date()) + " (k=" + k + " l=" + l + " delta=" + delta + " beta=" + beta + " zeta=" + zeta + " mu=" + mu + " parallel=" + parallel + ")";

        // Create a stream of custom elements and apply transformations
        DataStream<Tuple18<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> dataStream = sourceData
                .map(new StringToTuple<Tuple17<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>())
                .returns(TypeInformation.of(new TypeHint<Tuple17<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>() {
                }))
                .keyBy(tuple -> tuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction<Long, Tuple17<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>, Tuple18<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>(0, k, l, delta, beta, zeta, mu, true, 2))
                .returns(TypeInformation.of(new TypeHint<Tuple18<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>() {
                }))

                .name(evalDescription);

        dataStream.writeToSocket(sutHost, sutPortRead, new TupleToString<>());

        // Execute the transformation pipeline
        return env.execute(evalDescription);
    }


    public enum DatasetFields {
        ID,
        TS,
        BUILDING_ID(new AggregationIntegerGeneralizer(Tuple2.of(0, 1000)), false),

        TIMESTAMP,
        METER_READING(new AggregationFloatGeneralizer(Tuple2.of(0f, 100000f)), false),
        PRIMARY_USE(new NonNumericalGeneralizer(new String[][]{
                {"private", "Lodging/Residential"},
                {"public", "commercial", "Entertainment", "Technology/Science", "Office", "Parking"},
                {"public", "administrative", "Education"},
                {"public", "administrative", "Public Services"},
                {"public", "administrative", "Utility"}
        }), false),
        SQUARE_FEET(new AggregationFloatGeneralizer(Tuple2.of(0f, 50f)), false),
        YEAR_BUILD,
        FLOOR_COUNT,
        AIR_TEMPERATURE(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        CLOUD_COVERAGE(new AggregationFloatGeneralizer(Tuple2.of(0f, 9f)), false),
        DEW_TEMPERATURE(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        PERCIP_DEPTH_1_HR,
        SEA_LEVEL_PRESSURE(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        WIND_DIRECTION(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        WIND_SPEED(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),

        A(),
        ;

        private final BaseGeneralizer generalizer;
        private final boolean sensitive;

        DatasetFields(BaseGeneralizer generalizer, boolean sensitive) {
            this.generalizer = generalizer;
            this.sensitive = sensitive;
        }

        DatasetFields() {
            this(new NoneGeneralizer(), false);
        }

        public int getId() {
            return this.ordinal();
        }

        public boolean isSensitive() {
            return sensitive;
        }

        public BaseGeneralizer getGeneralizer() {
            return generalizer;
        }


        public Object parse(String input) {
            if (this.generalizer instanceof AggregationFloatGeneralizer) {
                return Float.parseFloat(input);
            }
            if (this.generalizer instanceof AggregationIntegerGeneralizer) {
                return Integer.parseInt(input);
            }

            return input;
        }
    }

    public static class TupleToString<T extends Tuple> implements SerializationSchema<T> {

        @Override
        public byte[] serialize(T element) {
            return element.toString().getBytes(StandardCharsets.UTF_8);
        }
    }


    public static class StringToTuple<T extends Tuple> implements MapFunction<String, T> {

        @Override
        public T map(String s) throws Exception {
            String[] split = s.split(",");
            DatasetFields[] fields = DatasetFields.values();
            T newTuple = (T) Tuple.newInstance(fields.length);

            for (DatasetFields field : DatasetFields.values()) {
                if (split.length <= field.getId()) {
                    continue;
                }
                String input = split[field.getId()];
                try {
                    Object value = field.parse(input);
                    newTuple.setField(value, field.getId());
                } catch (Exception e) {
                    System.err.printf("could not parse field %s: %s (%s): %s %n", field.name(), input, s, e);
                }
            }
            return newTuple;
        }
    }

}
