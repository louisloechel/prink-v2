# Prink

Prink (Privacy-Preserving Flink) is an implementation of the [CASTLE library](https://ieeexplore.ieee.org/document/5374415) into the streaming framework [Apache Flink](https://flink.apache.org/).

## Getting started
- Install Apache Flink following the official installation [instructions](https://nightlies.apache.org/flink/flink-docs-release-1.15//docs/try-flink/local_installation/).
- Clone repository

## Usage
To use Prink add the `CastleFunction` as a process into your data stream using `.process()`:

	.process(new CastleFunction())

Make sure to key your data stream by the user identifier using the `.keyBy()` function (in the example below user_id is at field position 0):

	.keyBy(dataTuple -> dataTuple.getField(0))

Add a `ruleBroadcastStream` using the `.connect()` function to be able to communicate with Prink and to set rules:

	.connect(ruleBroadcastStream)

You can use your own implementation for the `ruleBroadcastStream` or if you only want to transfer some rules at startup a collection generated stream (see below for example).
Prink uses a `MapState` for rule broascasting:

	MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));

    ArrayList<CastleRule> rules = new ArrayList<>();
        rules.add(new CastleRule(0, CastleFunction.Generalization.REDUCTION, false));
        rules.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f,90f), false));
        rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, true));

	BroadcastStream<CastleRule> ruleBroadcastStream = env
                .fromCollection(rules)
                .broadcast(ruleStateDescriptor);

Currently it is needed to specify the return type of the data stream. Use the `.returns()` function to do so (change the `Tuple` size for your project).

	.returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>(){}));

The complete Prink integration should look something like this (excluding the rule broadcast stream):

	DataStream<Tuple4<Object, Object, Object, Object>> myDataStream = env
                .addSource(new MySource())

	DataStream<Tuple4<Object, Object, Object, Object>> myPrinkDataStream = myDataStream
                .keyBy(dataTuple -> dataTuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction())
                .returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object,>>(){}));

To run your Flink job follow the offical Flink [documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/) or if you use the local installation run the following commands.

To start your Flink cluster run:

    $ ./bin/start-cluster.sh

To start your Flink job run:

	$ ./bin/flink run <your-flink-job>.jar

To stop your Flink cluster run:

	$ ./bin/stop-cluster.sh

---

To change the parameters of Prink add them when creating the `CastleFunction`:

	new CastleFunction(0, 50, 2, 200, 50, 50, 50, true, 2)

The parameters are the following:

| Name             | Type    | Default | Description                                                                                     |
|------------------|---------|---------|-------------------------------------------------------------------------------------------------|
| posTupleId       | int     | 0       | Position of the id value inside the tuples                                                      |
| k                | int     | 5       | Value k for k-anonymity                                                                         |
| l                | int     | 2       | Value l for l-diversity (if 0 l-diversity is not applied)                                       |
| delta            | int     | 20      | Number of 'delayed' tuples                                                                      |
| beta             | int     | 50      | Max number of clusters in bigGamma                                                              |
| zeta             | int     | 10      | Max number of clusters in bigOmega                                                              |
| mu               | int     | 10      | Number of last infoLoss values considered for tau                                               |
| showInfoLoss     | boolean | false   | If true Prink adds the info loss of a tuple at the end of the tuple (tuple size increases by 1) |
| suppressStrategy | int     | 2       | Defines how to handle tuples that need to be suppressed.                                        |

## Prink Rules
For the anonymization of the data stream Prink uses `CastleRules` to define how each data attribute should be handeld. These rules are broadcasted to the `CastleFunction` process and have the following structure:

| Name                   | Type                          | Allowed Values       | Usage                                                                    |
|------------------------|-------------------------------|----------------------|--------------------------------------------------------------------------|
| position               | int                           | 0-max size of Tuple  | Defines the position inside the data tuple that the rule should apply to |
| generalizationType     | CastleFunction.Generalization | enum provided values | Defines how the attribute at the defined position should be generalized  |
| domain (Optional)      | Tuple2<Float,Float>           | Tuple2<min Value, max Value>  | Defines to minimal and maximal value for the AggregationGeneralizer      |
| treeEntries (Optional) | ArrayList<String[]>           | ArrayList<String[]>  | Defines the tree structure to be used for the NonNumericalGeneralizer    |
| isSensibleAttribute    | boolean                       | [true, false]        | Defines if the position inside the data tuple is a sensible attribute    |

## Prink Generalizers
Currently Prink provides the following Generalizers for the data:

| generalizationType                  | Example Rule                                                                                                   | Generalization Result |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------|-----------------------|
| REDUCTION                           | `new CastleRule(5, CastleFunction.Generalization.REDUCTION, false)`                                              | [123456, 12789, 12678] -> 12****             |
| AGGREGATION                         | `new CastleRule(5, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,100f), false)`                        |  [20, 22, 35] -> [20-35]                   |
| NONNUMERICAL                        | `new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeEntries, false)`                              | (see Non-Numercial-Generalizer Chapter) |
| REDUCTION_WITHOUT_GENERALIZATION    | `new CastleRule(5, CastleFunction.Generalization.REDUCTION_WITHOUT_GENERALIZATION, false)`                       | [123456, 12789, 12678] -> [123456, 12789, 12678] |
| AGGREGATION_WITHOUT_GENERALIZATION  | `new CastleRule(5, CastleFunction.Generalization.AGGREGATION_WITHOUT_GENERALIZATION, Tuple2.of(0f,100f), false)` | [20, 22, 35] -> [20, 22, 35] |
| NONNUMERICAL_WITHOUT_GENERALIZATION | `new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL_WITHOUT_GENERALIZATION, treeEntries, false)`       | No data change (see Non-Numercial-Generalizer Chapter) |
| NONE                                | `new CastleRule(5, CastleFunction.Generalization.NONE, false)` | No data change  |

### Non-Numerical-Generalizer
*WIP*

### Add your own Generalizer
*WIP*

## Metrics
*WIP*

## Contributing
*WIP*

## License
The project will be open source. *WIP*
