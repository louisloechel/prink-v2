package pringtest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import pringtest.datatypes.CastleRule;
import pringtest.datatypes.TaxiFare;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CastleFunctionJobTest {

    int numStreamingTuples = 2000;

    ArrayList<Tuple> inputData = new ArrayList<>();

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    private static Stream<Arguments> provideTestParameters() {
//      Relevant Prink parameters:
//      - k
//      - l
//      - delta (Number of 'delayed' tuples)
//      - beta (Max Number of clusters in bigGamma)
//      - zeta (Max Number of clusters in bigOmega)
//      - mu (Number of last infoLoss values considered for tau)
//      Through rules
//      - Number of quasi-identifiers (QIs)
//      - Type of quasi-identifiers (QIs)
//      - Number of sensible attributes
//      Through flink
//      - input speed
//      - parallelism

        int[] kValues = {10}; // {2,3,4,5,6,7,8,9,10,20,50,100}; // {5};
        int[] lValues = {2}; // {0,1,2,3,4,5,6,7,8,9,10}; // {2};
        int[] deltaValues = {100}; // {2,5,10,20,50}; // {20} if k < delta
        int[] betaValues = {50}; //{1,2,5,10,20,30,40,50,60,70,80,90,100,150,200,300,500,1000};//{50};
        int[] zetaValues = {1000}; //{10};
        int[] muValues = {10}; //{1,2,3,4,5,6,7,8,9,10,20,30,40,50,75,100,200}; //{10};

        ArrayList<Arguments> arguments = new ArrayList<>();

        for(int k: kValues){
            for(int l: lValues) {
                for (int delta : deltaValues) {
                    for (int beta : betaValues) {
                        for (int zeta : zetaValues) {
                            for (int mu : muValues) {
                                arguments.add(Arguments.of(k, l, delta, beta, zeta, mu));
                            }
                        }
                    }
                }
            }
        }

        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource("provideTestParameters")
    public void castlePerformance(int k, int l, int delta, int beta, int zeta, int mu) throws Exception {
        // create TaxiFares to test stream
        for (int i = 0; i < numStreamingTuples; i++) {
            TaxiFare input = new TaxiFare(i);
            inputData.add(new Tuple8<>(input.rideId, input.taxiId, input.driverId, input.startTime,
                    input.paymentType, input.tip, input.tolls, input.totalFare));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        PerformanceSink.values.clear();
        PerformanceSink.durations.clear();
        PerformanceSink.infoLoss.clear();

        // Prink setup
        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));

        // Create treeEntries for non-numerical generalizer
        ArrayList<String[]> treeEntries = new ArrayList<>();
        treeEntries.add(new String[]{"CARD","APPLE PAY"});
        treeEntries.add(new String[]{"CASH","Banknotes"});
        treeEntries.add(new String[]{"CASH","Coins"});
        treeEntries.add(new String[]{"CARD","CREDIT CARD"});
        treeEntries.add(new String[]{"CARD","MAESTRO CARD"});
        treeEntries.add(new String[]{"CARD","PAYPAL","Ratenzahlung"});
        treeEntries.add(new String[]{"CARD","PAYPAL","Auf Rechnung"});
        treeEntries.add(new String[]{"CARD","PAYPAL","Direktzahlung"});
        treeEntries.add(new String[]{"9-Euro Ticket","No payment"});

        // Broadcast the rules and create the broadcast state
        ArrayList<CastleRule> rules = new ArrayList<>();
        rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));
//        rules.add(new CastleRule(1, CastleFunction.Generalization.NONE, false));
        rules.add(new CastleRule(1, CastleFunction.Generalization.REDUCTION, false));
        rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
        rules.add(new CastleRule(3, CastleFunction.Generalization.NONE, false));
        rules.add(new CastleRule(4, CastleFunction.Generalization.NONNUMERICAL, treeEntries, true));
        rules.add(new CastleRule(5, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,100f), false));
        rules.add(new CastleRule(6, CastleFunction.Generalization.NONE, Tuple2.of(0f,200f), true));
        rules.add(new CastleRule(7, CastleFunction.Generalization.NONE, Tuple2.of(10f,500f), true));

        DataStream<CastleRule> ruleStream = env.fromCollection(rules);
        BroadcastStream<CastleRule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);

        // create a stream of custom elements and apply transformations
        DataStream<Tuple> stream = env
                .fromCollection(inputData)
                .keyBy(value -> value.getField(1))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction(1, k, l, delta, beta, zeta, mu, true, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple9<Object, Object, Object, Object,Object, Object, Object, Object, Object>>(){}));

        stream.addSink(new PerformanceSink("Prink;k=" + k + ";l=" + l + ";delta=" + delta + ";beta=" + beta + ";zeta=" + zeta + ";mu=" + mu));

        // execute
        env.execute();

        // verify your results
        System.out.println(PerformanceSink.durations.toString());
        System.out.println("Expected amount:" + inputData.size() + " collected:" + PerformanceSink.values.size());
        StringBuilder sb = new StringBuilder();
        if(PerformanceSink.values.size() >= 10000){
            OptionalDouble averageInfoLoss = PerformanceSink.infoLoss
                    .stream()
                    .mapToDouble(a -> a)
                    .average();

            OptionalDouble averageDuration = PerformanceSink.durations
                    .stream()
                    .mapToDouble(a -> a)
                    .average();

            sb.append("Prink;").append(PerformanceSink.values.size()).append(";").append(k).append(";").append(l).append(";").append(delta).append(";").append(beta).append(";").append(zeta).append(";").append(mu).append(";");
            sb.append(averageInfoLoss.getAsDouble()).append(";").append(averageDuration.getAsDouble()).append(System.lineSeparator());

        }else{
            sb.append("Prink;").append(PerformanceSink.values.size()).append(";").append(k).append(";").append(l).append(";").append(delta).append(";").append(beta).append(";").append(zeta).append(";").append(mu).append(";");
            sb.append("WIP").append(";").append("WIP").append(System.lineSeparator());
        }
        try {
            Files.write(Paths.get("C:\\Users\\Groneberg\\Desktop\\Studium\\AAA_Master\\5_Masterarbeit\\Evaluation\\01.08.22\\test_eval.txt"), sb.toString().getBytes(), StandardOpenOption.APPEND);
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }

        assertTrue(PerformanceSink.values.size() >= (inputData.size()-100));
//        assertEquals(PerformanceSink.values.size(),inputData.size());
    }

    private static Stream<Arguments> provideAgeEvalParameters() {
        int[] kValues = {50};//{50, 100}; // {2,3,4,5,6,7,8,9,10,20,50,100}; // {5};
        int[] lValues = {2}; // {0,1,2,3,4,5,6,7,8,9,10}; // {2};
        int[] deltaValues = {20}; // {2,5,10,20,50}; // {20} if k < delta
        int[] betaValues = {50}; //{1,2,5,10,20,30,40,50,60,70,80,90,100,150,200,300,500,1000};//{50};
        int[] zetaValues = {50}; //{10};
        int[] muValues = {50}; //{1,2,3,4,5,6,7,8,9,10,20,30,40,50,75,100,200}; //{10};

        // ------------ DGH section ------------
        // [3, 10] workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked
        ArrayList<String[]> treeWorkclass = new ArrayList<>();
        treeWorkclass.add(new String[]{"Never-worked"});
        treeWorkclass.add(new String[]{"Self-emp","Self-emp-not-inc"});
        treeWorkclass.add(new String[]{"Self-emp","Self-emp-inc"});
        treeWorkclass.add(new String[]{"gov","Federal-gov"});
        treeWorkclass.add(new String[]{"gov","Local-gov"});
        treeWorkclass.add(new String[]{"gov","State-gov"});
        treeWorkclass.add(new String[]{"Without-pay"});
        treeWorkclass.add(new String[]{"Private"});

        // [5,26] education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool
        ArrayList<String[]> treeEducation = new ArrayList<>();
        treeEducation.add(new String[]{"high-ed","HS-college","HS-grad"});
        treeEducation.add(new String[]{"high-ed","HS-college","Some-college"});

        treeEducation.add(new String[]{"high-ed","Assoc","Assoc-acdm"});
        treeEducation.add(new String[]{"high-ed","Assoc","Assoc-voc"});

        treeEducation.add(new String[]{"low-ed","Pre-6th","Preschool"});
        treeEducation.add(new String[]{"low-ed","Pre-6th","1st-4th"});
        treeEducation.add(new String[]{"low-ed","Pre-6th","5th-6th"});

        treeEducation.add(new String[]{"low-ed","7th-12th","7th-8th"});
        treeEducation.add(new String[]{"low-ed","7th-12th","9th"});
        treeEducation.add(new String[]{"low-ed","7th-12th","10th"});
        treeEducation.add(new String[]{"low-ed","7th-12th","11th"});
        treeEducation.add(new String[]{"low-ed","7th-12th","12th"});

        treeEducation.add(new String[]{"high-ed","Uni","Bachelors"});
        treeEducation.add(new String[]{"high-ed","Uni","Masters"});
        treeEducation.add(new String[]{"high-ed","Uni","Prof-school"});

        // [4,11] marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse
        ArrayList<String[]> treeMarital = new ArrayList<>();
        treeMarital.add(new String[]{"married","has-spouse","Married-civ-spouse"});
        treeMarital.add(new String[]{"not-married","was-married","Divorced"});
        treeMarital.add(new String[]{"not-married","never-married","Never-married"});
        treeMarital.add(new String[]{"not-married","was-married","Separated"});
        treeMarital.add(new String[]{"married","alone","Widowed"});
        treeMarital.add(new String[]{"married","alone","Married-spouse-absent"});
        treeMarital.add(new String[]{"married","has-spouse","Married-AF-spouse"});

        // [3, 18] occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces
        ArrayList<String[]> treeOccupation = new ArrayList<>();
        treeOccupation.add(new String[]{"Group1","Tech-support"});
        treeOccupation.add(new String[]{"Group1","Craft-repair"});
        treeOccupation.add(new String[]{"Group1","Other-service"});
        treeOccupation.add(new String[]{"Group1","Sales"});
        treeOccupation.add(new String[]{"Group1","Exec-managerial"});
        treeOccupation.add(new String[]{"Group2","Prof-specialty"});
        treeOccupation.add(new String[]{"Group2","Handlers-cleaners"});
        treeOccupation.add(new String[]{"Group2","Machine-op-inspct"});
        treeOccupation.add(new String[]{"Group2","Adm-clerical"});
        treeOccupation.add(new String[]{"Group4","Farming-fishing"});
        treeOccupation.add(new String[]{"Group4","Transport-moving"});
        treeOccupation.add(new String[]{"Group4","Priv-house-serv"});
        treeOccupation.add(new String[]{"Group4","Protective-serv"});
        treeOccupation.add(new String[]{"Group4","Armed-Forces"});

        // [3, 9] relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried
        ArrayList<String[]> treeRelationship = new ArrayList<>();
        treeRelationship.add(new String[]{"Family","Wife"});
        treeRelationship.add(new String[]{"Family","Own-child"});
        treeRelationship.add(new String[]{"Family","Husband"});
        treeRelationship.add(new String[]{"Alone","Not-in-family"});
        treeRelationship.add(new String[]{"With-others","Other-relative"});
        treeRelationship.add(new String[]{"With-others","Unmarried"});

        // [3, 8] race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black
        ArrayList<String[]> treeRace = new ArrayList<>();
        treeRace.add(new String[]{"Group1","White"});
        treeRace.add(new String[]{"Group2","Asian-Pac-Islander"});
        treeRace.add(new String[]{"Group1","Amer-Indian-Eskimo"});
        treeRace.add(new String[]{"Other"});
        treeRace.add(new String[]{"Group2","Black"});

        // [2, 3] sex: Female, Male
        ArrayList<String[]> treeSex = new ArrayList<>();
        treeSex.add(new String[]{"Female"});
        treeSex.add(new String[]{"Male"});

        // [2, 3] sex: >50K, <=50K
        ArrayList<String[]> treeIncome = new ArrayList<>();
        treeIncome.add(new String[]{">50K"});
        treeIncome.add(new String[]{"<=50K"});

        ArrayList<ArrayList<CastleRule>> ruleSets = new ArrayList<>();

        ArrayList<CastleRule> rulesSet1 = new ArrayList<>();
        rulesSet1.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f,90f), false));
        rulesSet1.add(new CastleRule(10, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,99999f), false));
        rulesSet1.add(new CastleRule(11, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,4356f), false));
        rulesSet1.add(new CastleRule(12, CastleFunction.Generalization.AGGREGATION, Tuple2.of(1f,99f), false));
        rulesSet1.add(new CastleRule(13, CastleFunction.Generalization.NONE, true));
        rulesSet1.add(new CastleRule(14, CastleFunction.Generalization.NONE, false));

        ArrayList<CastleRule> rulesSet2 = new ArrayList<>();
        rulesSet2.add(new CastleRule(3, CastleFunction.Generalization.NONNUMERICAL, treeWorkclass, false));
        rulesSet2.add(new CastleRule(4, CastleFunction.Generalization.NONNUMERICAL, treeEducation, false));
        rulesSet2.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false));
        rulesSet2.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false));
        rulesSet2.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeRelationship, false));
        rulesSet2.add(new CastleRule(8, CastleFunction.Generalization.NONNUMERICAL, treeRace, false));
        rulesSet2.add(new CastleRule(9, CastleFunction.Generalization.NONNUMERICAL, treeSex, false));
        rulesSet2.add(new CastleRule(13, CastleFunction.Generalization.NONE, true));
        rulesSet2.add(new CastleRule(14, CastleFunction.Generalization.NONNUMERICAL, treeIncome, false));

        ArrayList<CastleRule> rulesSet3 = new ArrayList<>();
        rulesSet3.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f,90f), false));
        rulesSet3.add(new CastleRule(10, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,99999f), false));
        rulesSet3.add(new CastleRule(11, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,4356f), false));
        rulesSet3.add(new CastleRule(12, CastleFunction.Generalization.AGGREGATION, Tuple2.of(1f,99f), false));
        rulesSet3.add(new CastleRule(3, CastleFunction.Generalization.NONNUMERICAL, treeWorkclass, false));
        rulesSet3.add(new CastleRule(4, CastleFunction.Generalization.NONNUMERICAL, treeEducation, false));
        rulesSet3.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false));
        rulesSet3.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false));
        rulesSet3.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeRelationship, false));
        rulesSet3.add(new CastleRule(8, CastleFunction.Generalization.NONNUMERICAL, treeRace, false));
        rulesSet3.add(new CastleRule(9, CastleFunction.Generalization.NONNUMERICAL, treeSex, false));
        rulesSet3.add(new CastleRule(13, CastleFunction.Generalization.NONE, true));
        rulesSet3.add(new CastleRule(14, CastleFunction.Generalization.NONNUMERICAL, treeIncome, false));

//        ruleSets.add(rulesSet1);
//        ruleSets.add(rulesSet2);
        ruleSets.add(rulesSet3);

        ArrayList<Arguments> arguments = new ArrayList<>();

        for(int k: kValues){
            for(int l: lValues) {
                for (int delta : deltaValues) {
                    for (int beta : betaValues) {
                        for (int zeta : zetaValues) {
                            for (int mu : muValues) {
                                for (ArrayList<CastleRule> rules: ruleSets) {
                                    arguments.add(Arguments.of(k, l, delta, beta, zeta, mu, rules));
                                }
                            }
                        }
                    }
                }
            }
        }
        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource("provideAgeEvalParameters")
    public void prinkAgeDataSetEvaluation(int k, int l, int delta, int beta, int zeta, int mu, ArrayList<CastleRule> rules) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        PerformanceSink.values.clear();
        PerformanceSink.durations.clear();
        PerformanceSink.infoLoss.clear();

        final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("C:\\Users\\Groneberg\\Desktop\\Studium\\AAA_Master\\5_Masterarbeit\\DataSets\\AdultDataSet\\adult.csv")).build();

        // start the data generator and arrange for watermarking
        DataStream<Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> fares = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "file-source").map(new StringToTuple());

        // Prink setup
        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));


        DataStream<CastleRule> ruleStream = env.fromCollection(rules);
        BroadcastStream<CastleRule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);

        // create a stream of custom elements and apply transformations
        DataStream<Tuple> privateFares = fares
                .keyBy(tuple -> tuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction(0, k, l, delta, beta, zeta, mu, true, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple16<Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object>>(){}));


        privateFares.addSink(new PerformanceSink("Prink;k=" + k + ";l=" + l + ";delta=" + delta + ";beta=" + beta + ";zeta=" + zeta + ";mu=" + mu + ";rules=" + rules.toString()));

        // execute
        env.execute();

        // verify your results
        StringBuilder sb = new StringBuilder();

        OptionalDouble averageInfoLoss = PerformanceSink.infoLoss
                .stream()
                .mapToDouble(a -> a)
                .average();

        OptionalDouble averageDuration = PerformanceSink.durations
                .stream()
                .mapToDouble(a -> a)
                .average();

        String timestamp = new SimpleDateFormat("MM-dd-HH-mm-ss").format(new Date());

        sb.append(timestamp).append(";parameters{k=").append(k).append(";l=").append(l).append(";delta=").append(delta).append(";beta=").append(beta).append(";zeta=").append(zeta).append(";mu=").append(mu).append(";AvgInfoLoss=").append(averageInfoLoss.getAsDouble()).append(";AvgDuration=").append(averageDuration.getAsDouble())
                .append(";valueSize:").append(PerformanceSink.values.size())
                .append(";rules=").append(rules).append(System.lineSeparator());

        try {
            Files.write(Paths.get("C:\\Users\\Groneberg\\Desktop\\Studium\\AAA_Master\\5_Masterarbeit\\Evaluation\\03.09.22\\test_eval.txt"), sb.toString().getBytes(), StandardOpenOption.APPEND);

            sb.append("InfoLoss=").append(PerformanceSink.infoLoss.toString()).append(System.lineSeparator());
            sb.append("Durations=").append(PerformanceSink.durations.toString()).append(System.lineSeparator());
            Files.write(Paths.get("C:\\Users\\Groneberg\\Desktop\\Studium\\AAA_Master\\5_Masterarbeit\\Evaluation\\03.09.22\\details\\" + timestamp +".txt"), sb.toString().getBytes(), StandardOpenOption.CREATE);
        }catch (IOException e) {
            System.out.println("ERROR: while writing file e:" + e);
            //exception handling left as an exercise for the reader
        }
        assertTrue(PerformanceSink.values.size() >= (inputData.size()-100));
    }

    // create a testing sink
    private static class PerformanceSink implements SinkFunction<Tuple> {

        // must be static
        public static final List<Tuple> values = Collections.synchronizedList(new ArrayList<>());
        public static final List<Long> durations = Collections.synchronizedList(new ArrayList<>());
        public static final List<Float> infoLoss = Collections.synchronizedList(new ArrayList<>());
        private final String description;

        public PerformanceSink(String desc){
            super();
            this.description = desc;
        }

        @Override
        public void invoke(Tuple input, SinkFunction.Context context) throws Exception {

            long startTime = input.getField(2) != null ? (long) input.getField(2) : 0L;

            values.add(input);
            durations.add(context.currentProcessingTime() - startTime);
            infoLoss.add((float) input.getField(input.getArity()-1));

//            System.out.println(input +
//                            ";" + description +
//                            ";" + (context.currentProcessingTime() - startTime));

        }
    }

    public static class StringToTuple implements MapFunction<String, Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> {

        long counter = 0;

        @Override
        public Tuple15<Object, Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object> map(String input) {
            String[] splitValues = input.split(", ");
            counter++;
            if(splitValues.length >= 15) {
                // remove 'fnlwgt' and add 'id' for user identification (expect every entry to be individual)
                return Tuple15.of(counter, Float.parseFloat(splitValues[0]), null, splitValues[1], splitValues[3],
                        splitValues[5], splitValues[6], splitValues[7], splitValues[8],
                        splitValues[9], Float.parseFloat(splitValues[10]), Float.parseFloat(splitValues[11]), Float.parseFloat(splitValues[12]), splitValues[13], splitValues[14]);
            }else{
                return new Tuple15<>(counter, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
            }
        }
    }

}