package pringtest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pringtest.datatypes.TaxiFare;
import pringtest.sources.TaxiFareGenerator;

import java.time.Instant;

/**
 * Class to test out flink functionality
 * TODO remove when testing is completed
 */
public class PrinkInformationReduction {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple4<Long, Long, String, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public PrinkInformationReduction(SourceFunction<TaxiFare> source, SinkFunction<Tuple4<Long, Long, String, Float>> sink) {

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
        DataStream<TaxiFare> fares = env
                .addSource(source)
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order
                        WatermarkStrategy
                                .<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((fare, t) -> fare.getEventTimeMillis()));

        // compute tips per hour for each driver
//        DataStream<Tuple4<Long, Long, String, Float>> privateFares = fares
//                .keyBy((TaxiFare fare) -> fare.driverId)
//                .window(TumblingEventTimeWindows.of(Time.hours(8)))
//                .process(new RangeAggregationProcessWindowFunction());

        DataStream<Tuple4<Long, Long, String, Float>> privateFares = fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .process(new CastleFunction());


        privateFares.addSink(sink);

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
     * Convert TaxiFares into a tuple representation
     * TODO include into stream
     */
    public class TaxiFareToTuple implements MapFunction<TaxiFare, Tuple8<Long, Long, Long, Instant, String, Float, Float, Float>> {

        @Override
        public Tuple8<Long, Long, Long, Instant, String, Float, Float, Float> map(TaxiFare input) {
            return new Tuple8<>(input.rideId, input.taxiId, input.driverId, input.startTime,
                    input.paymentType, input.tip, input.tolls, input.totalFare);
        }
    }
}
