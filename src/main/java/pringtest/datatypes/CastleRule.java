package pringtest.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;
import pringtest.CastleFunction;

import java.util.ArrayList;

public class CastleRule {

    private final int position;
    private final CastleFunction.Generalization generalizationType;
    private Tuple2<Float, Float> domain;
    private ArrayList<String[]> treeEntries;

    public CastleRule(int position, CastleFunction.Generalization generalizationType, Tuple2<Float,Float> domain){
        this.position = position;
        this.generalizationType = generalizationType;
        this.domain = domain;
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, ArrayList<String[]> treeEntries){
        this.position = position;
        this.generalizationType = generalizationType;
        this.treeEntries = treeEntries;
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType){
        this.position = position;
        this.generalizationType = generalizationType;
    }

    public int getPosition() {
        return position;
    }

    public CastleFunction.Generalization getGeneralizationType() {
        return generalizationType;
    }

    public Tuple2<Float, Float> getDomain() {
        return domain;
    }

    public ArrayList<String[]> getTreeEntries() {
        return treeEntries;
    }

    @Override
    public String toString() {
        return "CastleRule{" +
                "position=" + position +
                ", generalizationType=" + generalizationType +
                '}';
    }
}
