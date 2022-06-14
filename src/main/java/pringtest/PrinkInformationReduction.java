package pringtest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pringtest.datatypes.TaxiFare;
import pringtest.sources.TaxiFareGenerator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Class to test out flink functionality
 * TODO remove when testing is completed
 */
public class PrinkInformationReduction {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> sink;

    /** Creates a job using the source and sink provided. */
    public PrinkInformationReduction(SourceFunction<TaxiFare> source, SinkFunction<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        PrinkInformationReduction job = new PrinkInformationReduction(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the information reduction pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator and arrange for watermarking
        DataStream<Tuple8<Long, Long, Long, Instant, String, Float, Float, Float>> fares = env
                .addSource(source)
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order
                        WatermarkStrategy
                                .<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((fare, t) -> fare.getEventTimeMillis()))
                .map(new TaxiFareToTuple());

        CastleFunction.Generalization[] config = new CastleFunction.Generalization[]{
                CastleFunction.Generalization.NONE,
                CastleFunction.Generalization.REDUCTION,
                CastleFunction.Generalization.NONE,
                CastleFunction.Generalization.NONE,
                CastleFunction.Generalization.NONE, // CastleFunction.Generalization.NONNUMERICAL,
                CastleFunction.Generalization.AGGREGATION,
                CastleFunction.Generalization.AGGREGATION,
                CastleFunction.Generalization.AGGREGATION};

        CastleFunction castle = new CastleFunction(config);

/*        DataStream<Tuple4<Long, Long, String, Float>> privateFares = fares
//                .keyBy((TaxiFare fare) -> fare.driverId)
                .keyBy(fare -> fare.getField(0))
                .process(castle)
                .returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>(){}));
//                .returns(castle.getReturnValues());

        privateFares.addSink(sink);
*/
        DataStream<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> privateFares = fares
                .keyBy(fare -> fare.getField(0))
                .process(castle)
                .returns(TypeInformation.of(new TypeHint<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>>(){}));

        privateFares.addSink(new TimerSink());

        // execute the transformation pipeline
        return env.execute("Data Reduction Job");
    }

    private static class RangeAggregationProcessWindowFunction
            extends ProcessWindowFunction<TaxiFare, Tuple4<Long, Long, String, Float>, Long, TimeWindow> {

        public void process(Long key,
                            Context context,
                            Iterable<TaxiFare> input,
                            Collector<Tuple4<Long, Long, String, Float>> out) {

            long count = 0;
            float minValue = 999999999;
            float maxValue = 0;
            for (TaxiFare in: input) {
                minValue = Math.min(in.tip, minValue);
                maxValue = Math.max(in.tip, maxValue);
                count++;
            }

            for (TaxiFare in: input) {
                out.collect(new Tuple4<>(key, in.rideId, String.format("[%.2f..%.2f]", minValue, maxValue), in.tip));
            }
            System.out.println("Window: " + context.window() + " count: " + count);
        }
    }

    /**
     * Convert TaxiFares into a tuple8 representation
     */
    public class TaxiFareToTuple implements MapFunction<TaxiFare, Tuple8<Long, Long, Long, Instant, String, Float, Float, Float>> {

        @Override
        public Tuple8<Long, Long, Long, Instant, String, Float, Float, Float> map(TaxiFare input) {
            return new Tuple8<>(input.rideId, input.taxiId, input.driverId, input.startTime,
                    input.paymentType, input.tip, input.tolls, input.totalFare);
        }
    }

    public class TimerSink extends RichSinkFunction<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> {

        int reportInterval = 1000;
        int counter = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        TreeMap<String, Integer> rangeCounter = new TreeMap<>();
        ArrayList<Long> seenIds = new ArrayList<>();

        @Override
        public void invoke(Tuple8<Object, Object, Object, Object,Object, Object, Object, Object> input, Context context) throws Exception {

            long processingTime = Duration.between((Instant) input.f2, Instant.now()).toMillis();
            seenIds.add((Long) input.f0);
            counter++;

            minTime = Math.min(minTime, processingTime);
            maxTime = Math.max(maxTime, processingTime);

            if(processingTime >= 0 && processingTime < 50){
                rangeCounter.merge("A| 0ms - 49ms", 1, Integer::sum);
            }else if (processingTime >= 50 && processingTime < 100){
                rangeCounter.merge("B| 50ms - 99ms", 1, Integer::sum);
            }else if (processingTime >= 100 && processingTime < 200){
                rangeCounter.merge("C| 100ms - 199ms", 1, Integer::sum);
            }else if (processingTime >= 200 && processingTime < 300){
                rangeCounter.merge("D| 200ms - 299ms", 1, Integer::sum);
            }else if (processingTime >= 300 && processingTime < 400){
                rangeCounter.merge("E| 200ms - 399ms", 1, Integer::sum);
            }else if (processingTime >= 400 && processingTime < 500){
                rangeCounter.merge("F| 400ms - 499ms", 1, Integer::sum);
            }else if (processingTime >= 500 && processingTime < 600){
                rangeCounter.merge("G| 500ms - 599ms", 1, Integer::sum);
            }else if (processingTime >= 600 && processingTime < 700){
                rangeCounter.merge("H| 600ms - 699ms", 1, Integer::sum);
            }else if (processingTime >= 700 && processingTime < 800){
                rangeCounter.merge("I| 700ms - 799ms", 1, Integer::sum);
            }else if (processingTime >= 800 && processingTime < 900){
                rangeCounter.merge("J| 800ms - 899ms", 1, Integer::sum);
            }else if (processingTime >= 900 && processingTime < 1000){
                rangeCounter.merge("K| 900ms - 999ms", 1, Integer::sum);
            }else if (processingTime >= 1000 && processingTime < 1250){
                rangeCounter.merge("L| 1000ms - 1249ms", 1, Integer::sum);
            }else if (processingTime >= 1250 && processingTime < 1500){
                rangeCounter.merge("M| 1250ms - 1499ms", 1, Integer::sum);
            }else if (processingTime >= 1500 && processingTime < 2000){
                rangeCounter.merge("N| 1500ms - 1999ms", 1, Integer::sum);
            }else if (processingTime >= 2000 && processingTime < 2500){
                rangeCounter.merge("O| 2000ms - 2499ms", 1, Integer::sum);
            }else{
                rangeCounter.merge("P| 2500ms - >2500ms", 1, Integer::sum);
            }

            if((counter % reportInterval) == 0){
                System.out.println(input.toString());
                System.out.println("------------------ REPORT Sink:" + this + " --------------------");
                for (Map.Entry<String, Integer> entry : rangeCounter.entrySet()) {
//                    System.out.println("| " + entry.getKey() + ":       " + entry.getValue());
                    System.out.format("| %18s | %16d |", entry.getKey().substring(3), entry.getValue());
                    System.out.println();
                }
                System.out.println("------------------------------------------");
                System.out.format("| %18s | %16d |", "Min. Time (ms)", minTime);
                System.out.println();
                System.out.format("| %18s | %16d |", "Max. Time (ms)", maxTime);
                System.out.println();
                System.out.format("| %18s | %16d |", "Total entries", counter);
                System.out.println();
                System.out.println("---------------------------------------------------");

            }else{
//            System.out.println(input.toString() +
//                    ";" + ((Instant) input.f3).toEpochMilli() +
//                    ";" + Instant.now().toEpochMilli() +
//                    ";ProcessingTime;" + (Duration.between((Instant) input.f2, Instant.now()).toMillis()));

            }

        }
    }
}
