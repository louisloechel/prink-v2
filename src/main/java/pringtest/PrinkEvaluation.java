package pringtest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import pringtest.datatypes.CastleRule;

import java.util.ArrayList;

/**
 * Class to test out prink functionality
 */
public class PrinkEvaluation {

    // Source currently not used
    private final SourceFunction<Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> source;
    private final SinkFunction<Tuple> sink;
    int k = 5;
    int l = 2;
    int delta = 200;
    int beta = 50;
    int zeta = 50;
    int mu = 10;

    /** Creates a job using the source and sink provided. */
    public PrinkEvaluation(SourceFunction<Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> source, SinkFunction<Tuple> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        PrinkEvaluation job = new PrinkEvaluation(null, new PerformanceSink());

        job.execute();
    }

    /**
     * Create and execute the information reduction pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

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
        treeEducation.add(new String[]{"Without-Post-Secondary","Preschool"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","1st-4th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","5th-6th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","7th-8th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Junior-Secondary","9th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Junior-Secondary","10th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","11th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","12th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","HS-grad"});
        treeEducation.add(new String[]{"Post-Secondary","Some-college"});
        treeEducation.add(new String[]{"Post-Secondary","Assoc","Assoc-acdm"});
        treeEducation.add(new String[]{"Post-Secondary","Assoc","Assoc-voc"});
        treeEducation.add(new String[]{"Post-Secondary","University","Bachelors"});
        treeEducation.add(new String[]{"Post-Secondary","University","Prof-school"});
        treeEducation.add(new String[]{"Post-Secondary","University","Post-grad","Masters"});
        treeEducation.add(new String[]{"Post-Secondary","University","Post-grad","Doctorate"});

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
        treeOccupation.add(new String[]{"White-collar","Exec-managerial"});
        treeOccupation.add(new String[]{"White-collar","Prof-specialty"});
        treeOccupation.add(new String[]{"White-collar","Sales"});
        treeOccupation.add(new String[]{"White-collar","Adm-clerical"});
        treeOccupation.add(new String[]{"Blue-collar","Tech-support"});
        treeOccupation.add(new String[]{"Blue-collar","Craft-repair"});
        treeOccupation.add(new String[]{"Blue-collar","Machine-op-inspct"});
        treeOccupation.add(new String[]{"Blue-collar","Handlers-cleaners"});
        treeOccupation.add(new String[]{"Blue-collar","Priv-house-serv"});
        treeOccupation.add(new String[]{"Blue-collar","Transport-moving"});
        treeOccupation.add(new String[]{"Other","Protective-serv"});
        treeOccupation.add(new String[]{"Other","Armed-Forces"});
        treeOccupation.add(new String[]{"Other","Farming-fishing"});
        treeOccupation.add(new String[]{"Other","Other-service"});

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

        // [] nation: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands
        ArrayList<String[]> treeNation = new ArrayList<>();
        treeNation.add(new String[]{"America", "North-America","United-States"});
        treeNation.add(new String[]{"America", "North-America","Canada"});

        treeNation.add(new String[]{"America", "Middle-America","Puerto-Rico"});
        treeNation.add(new String[]{"America", "Middle-America","Cuba"});
        treeNation.add(new String[]{"America", "Middle-America","Honduras"});
        treeNation.add(new String[]{"America", "Middle-America","Jamaica"});
        treeNation.add(new String[]{"America", "Middle-America","Mexico"});
        treeNation.add(new String[]{"America", "Middle-America","Dominican-Republic"});
        treeNation.add(new String[]{"America", "Middle-America","Haiti"});
        treeNation.add(new String[]{"America", "Middle-America","Guatemala"});
        treeNation.add(new String[]{"America", "Middle-America","Nicaragua"});
        treeNation.add(new String[]{"America", "Middle-America","El-Salvador"});
        treeNation.add(new String[]{"America", "Middle-America","Trinadad&Tobago"});

        treeNation.add(new String[]{"America", "South-America","Ecuador"});
        treeNation.add(new String[]{"America", "South-America","Columbia"});
        treeNation.add(new String[]{"America", "South-America","Peru"});

        treeNation.add(new String[]{"Asia", "South-East-Asia","Cambodia"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Philippines"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Vietnam"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Laos"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Thailand"});

        treeNation.add(new String[]{"Asia", "South-Asia","India"});

        treeNation.add(new String[]{"Asia", "East-Asia","Japan"});
        treeNation.add(new String[]{"Asia", "East-Asia","China"});
        treeNation.add(new String[]{"Asia", "East-Asia","Taiwan"});
        treeNation.add(new String[]{"Asia", "East-Asia","Hong"});
        treeNation.add(new String[]{"Asia", "East-Asia","South"});

        treeNation.add(new String[]{"Asia", "West-Asia","Iran"});

        treeNation.add(new String[]{"Europe", "South-Europe","Greece"});
        treeNation.add(new String[]{"Europe", "South-Europe","Italy"});
        treeNation.add(new String[]{"Europe", "South-Europe","Portugal"});

        treeNation.add(new String[]{"Europe", "West-Europe","England"});
        treeNation.add(new String[]{"Europe", "West-Europe","Germany"});
        treeNation.add(new String[]{"Europe", "West-Europe","Ireland"});
        treeNation.add(new String[]{"Europe", "West-Europe","France"});
        treeNation.add(new String[]{"Europe", "West-Europe","Scotland"});
        treeNation.add(new String[]{"Europe", "West-Europe","Holand-Netherlands"});

        treeNation.add(new String[]{"Europe", "East-Europe","Poland"});
        treeNation.add(new String[]{"Europe", "East-Europe","Hungary"});
        treeNation.add(new String[]{"Europe", "East-Europe","Yugoslavia"});

        treeNation.add(new String[]{"Outlying-US(Guam-USVI-etc)"});

        // Set up rules
        ArrayList<CastleRule> rules = new ArrayList<>();
//        // Alternative rule set
//        rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));
//        rules.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f, 90f), false, 1.0)); // age
//        rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
//        rules.add(new CastleRule(3, CastleFunction.Generalization.NONNUMERICAL, treeWorkclass, false)); // workclass
//        rules.add(new CastleRule(4, CastleFunction.Generalization.NONE, false)); // final weight
//        rules.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeEducation,false));
//        rules.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false, 1.0));
//        rules.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false, 1.0));
//        rules.add(new CastleRule(8, CastleFunction.Generalization.NONNUMERICAL, treeRelationship, false, 1.0));
//        rules.add(new CastleRule(9, CastleFunction.Generalization.NONNUMERICAL, treeRace, false, 1.0));
//        rules.add(new CastleRule(10, CastleFunction.Generalization.NONNUMERICAL, treeSex, false, 1.0));
//        rules.add(new CastleRule(11, CastleFunction.Generalization.NONE, true));
//        rules.add(new CastleRule(12, CastleFunction.Generalization.NONE, true));
//        rules.add(new CastleRule(13, CastleFunction.Generalization.NONE, true));
//        rules.add(new CastleRule(14, CastleFunction.Generalization.NONE, false)); //NONNUMERICAL, false));

        rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));
        rules.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f, 90f), false, 0.05)); // age
        rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
        rules.add(new CastleRule(3, CastleFunction.Generalization.NONE, treeWorkclass, true)); // workclass
        rules.add(new CastleRule(4, CastleFunction.Generalization.AGGREGATION, Tuple2.of(13492f, 1490400f), false, 0.05)); // final weight
        rules.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeEducation,false, 0.2));
        rules.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false, 0.2));
        rules.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false, 0.2));
        rules.add(new CastleRule(8, CastleFunction.Generalization.NONE, treeRelationship, true)); // relationship
        rules.add(new CastleRule(9, CastleFunction.Generalization.NONE, treeRace, false)); // race
        rules.add(new CastleRule(10, CastleFunction.Generalization.NONE, treeSex, false)); // sex
        rules.add(new CastleRule(11, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 99999f), false, 0.05)); // capital-gain
        rules.add(new CastleRule(12, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 4356f), false, 0.05)); // capital-loss
        rules.add(new CastleRule(13, CastleFunction.Generalization.AGGREGATION, Tuple2.of(1f, 99f), false, 0.05)); // hours per week
        rules.add(new CastleRule(14, CastleFunction.Generalization.NONNUMERICAL, treeNation, false, 0.2)); // nation

        String evalDesc = "Agg. 0.05 | Non-Num. 0.2";

        // Set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("C:/Flink/flink-1.14.3/prink/adult_big_clean.csv")).build();


        // Start the data generator and arrange for watermarking
        DataStream<Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> sourceData =
              env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source").map(new StringToTuple());

        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));

        BroadcastStream<CastleRule> ruleBroadcastStream = env.fromCollection(rules)
                .broadcast(ruleStateDescriptor);

        // Create a stream of custom elements and apply transformations
        DataStream<Tuple> privateFares = sourceData
                .keyBy(tuple -> tuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction(0, k, l, delta, beta, zeta, mu, true, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple16<Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object>>(){}));

        String evalDescription = evalDesc + "Prink Evaluation (k= " + k + " l=" + l + " delta=" + delta + ")";
        ((PerformanceSink) sink).setDescription(evalDescription);

        privateFares.addSink(sink).name("Testing Sink");

        // Execute the transformation pipeline
        return env.execute(evalDescription);
    }

//    public static class AdultDataGenerator implements SourceFunction<Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> {
//
//        private volatile boolean running = true;
//        private int maxIterations = -1;
//
//        public AdultDataGenerator(int iterations){
//            this.maxIterations = iterations;
//        }
//
//        public AdultDataGenerator(){}
//
//        @Override
//        public void run(SourceContext<Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> ctx) throws Exception {
//
//            int id = 1; //long id = 1;
//
//
//            while (running) {
////                TaxiFare fare = new TaxiFare(id);
//
//                if (maxIterations > 0 && id >= maxIterations){
//                    break;
//                }
//
//                ++id;
//                ctx.collect(stringToTuple(id, "39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K"));
//
//                // match our event production rate to that of the TaxiRideGenerator
//                Thread.sleep(TaxiRideGenerator.SLEEP_MILLIS_PER_EVENT);
//            }
//        }
//
//        @Override
//        public void cancel() {
//            running = false;
//        }
//
//        private Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object> stringToTuple(int counter, String input){
//            String[] splitValues = input.split(", ");
//            if(splitValues.length >= 15) {
//                // remove 'fnlwgt' and add 'id' for user identification (expect every entry to be individual)
//                return Tuple15.of(counter, Float.parseFloat(splitValues[0]), null, splitValues[1], splitValues[3],
//                        splitValues[5], splitValues[6], splitValues[7], splitValues[8],
//                        splitValues[9], Float.parseFloat(splitValues[10]), Float.parseFloat(splitValues[11]), Float.parseFloat(splitValues[12]), splitValues[13], splitValues[14]);
//            }else{
//                return new Tuple15<>(counter, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
//            }
//        }
//    }

    public static class StringToTuple implements MapFunction<String, Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> {

        long counter = 0;

        @Override
        public Tuple15<Object, Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object> map(String input) {
            String[] splitValues = input.split(", ");
            counter++;
            if(splitValues.length >= 15) {
                // remove 'education-num' and add 'id' for user identification (expect every entry to be an individual)
                return Tuple15.of(counter,
                        Float.parseFloat(splitValues[0]),   // age: continuous
                        null,                               // placeholder time
                        splitValues[1],                     // workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
                        Float.parseFloat(splitValues[2]),   // fnlwgt: continuous.
                        splitValues[3],                     // education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
//                        splitValues[4],                       // education-num: continuous.
                        splitValues[5],                     // marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
                        splitValues[6],                     // occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
                        splitValues[7],                     // relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
                        splitValues[8],                     // race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
                        splitValues[9],                     // sex: Female, Male.
                        Float.parseFloat(splitValues[10]),  // capital-gain: continuous.
                        Float.parseFloat(splitValues[11]),  // capital-loss: continuous.
                        Float.parseFloat(splitValues[12]),  // hours-per-week: continuous.
                        splitValues[13]);                   // native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
//                          splitValues[13]                     // label: >50K, <=50K
            }else{
                System.out.println("DEBUG:ERROR: could not load:" + input);
                return new Tuple15<>(counter, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
            }
        }
    }

    private static class PerformanceSink implements SinkFunction<Tuple> {

        private String description;

        public void setDescription(String desc){
            this.description = desc;
        }

        @Override
        public void invoke(Tuple input, SinkFunction.Context context) {

            // To use timer, enable time set inside CastleFunction
            long startTime = input.getField(2) != null ? (long) input.getField(2) : 0L;

            System.out.println(input.getField(0) + //";" +  input +
                    ";" + description +
                    ";" + (context.currentProcessingTime() - startTime) +
                    ";" + (input.getField(input.getArity()-1)));
        }
    }
}
