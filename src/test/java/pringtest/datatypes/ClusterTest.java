package pringtest.datatypes;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import pringtest.CastleFunction;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class ClusterTest {

    @BeforeEach
    void setUp() {
    }

    @Test
    void enlargementValueCluster() {
        CastleRule[] rules = new CastleRule[]{};
        Cluster subject = new Cluster(rules, false);
        Cluster otherCluster = new Cluster(rules, false);

        assertEquals(0.0f, subject.enlargementValue(otherCluster));
    }

    @Test
    void EnlargementValueTuple() {
    }

    @Test
    void informationLossWith() {
    }

    @Test
    void testInformationLossWith() {
    }

    @Nested
    @DisplayName("Cluster with no entries")
    class NoEntriesTests {

        Cluster subject;

        @BeforeEach
        void setUp() {
            ArrayList<CastleRule> rulesTemp = new ArrayList<>();
            rulesTemp.add(new CastleRule(0, CastleFunction.Generalization.REDUCTION, false));
            rulesTemp.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, false));
            rulesTemp.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
            rulesTemp.add(new CastleRule(3, CastleFunction.Generalization.NONE, true));

            CastleRule[] rules = rulesTemp.toArray(new CastleRule[rulesTemp.size()]);

            subject = new Cluster(rules, false);
        }
        @Test
        void enlargementValueCluster() {
            CastleRule[] rules = new CastleRule[]{};
            Cluster otherCluster = new Cluster(rules, false);
            assertEquals(0.0f, subject.enlargementValue(otherCluster));
        }

        @Test
        void enlargementValueTuple() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            assertEquals(0.0f, subject.enlargementValue(tuple));
        }

        @Test
        void informationLossWithCluster() {
            CastleRule[] rules = new CastleRule[]{};
            Cluster otherCluster = new Cluster(rules, false);
            assertEquals(0.0f, subject.informationLossWith(otherCluster));
        }

        @Test
        void informationLossWithTuple() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            assertEquals(0.0f, subject.informationLossWith(tuple));
        }

        @Test
        void infoLoss() {
            assertEquals(0.0f, subject.infoLoss());
        }

        @Test
        void generalize() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            Tuple result = subject.generalize(tuple);
            assertAll("All values null",
                    () -> assertNull(result.getField(0)),
                    () -> assertNull(result.getField(1)),
                    () -> assertNull(result.getField(2)),
                    () -> assertNull(result.getField(3))
            );
        }

        @Test
        void generalizeMax() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            Tuple result = subject.generalize(tuple);
            assertAll("All values null",
                    () -> assertNull(result.getField(0)),
                    () -> assertNull(result.getField(1)),
                    () -> assertNull(result.getField(2)),
                    () -> assertNull(result.getField(3))
            );
        }

        @Test
        void contains() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>();
            assertFalse(subject.contains(tuple));
        }

        @Test
        void size() {
            assertEquals(0,subject.size());
        }

        @Test
        void diversity() {
            assertEquals(0,subject.diversity(new int[]{2}));
        }
    }

    @Test
    void infoLossWithOneEntry() {
        CastleRule[] rules = new CastleRule[]{};
        Cluster subject = new Cluster(rules, false);
        subject.addEntry(new Tuple4<>(1, 10f, "Test", "Test"));
        assertEquals(0.0f, subject.infoLoss());
    }

    @Test
    void generalize() {
    }

    @Test
    void generalizeMax() {
    }

    @Test
    void addEntry() {
    }

    @Test
    void addAllEntries() {
    }

    @Test
    void removeEntry() {
    }

    @Test
    void removeAllEntries() {
    }

    @Test
    void getAllEntries() {
    }

    @Test
    void contains() {
    }

    @Test
    void size() {
    }

    @Test
    void diversity() {
    }
}