package pringtest.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;
import pringtest.CastleFunction;

import java.util.ArrayList;

public class CastleRule {

    private final int position;
    private final CastleFunction.Generalization generalizationType;
    private Tuple2<Float, Float> domain;
    private ArrayList<String[]> treeEntries;
    private boolean isSensibleAttribute;

    public CastleRule(int position, CastleFunction.Generalization generalizationType, Tuple2<Float,Float> domain, boolean isSensibleAttribute){
        this.position = position;
        this.generalizationType = generalizationType;
        this.domain = domain;
        this.isSensibleAttribute = isSensibleAttribute;
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, ArrayList<String[]> treeEntries, boolean isSensibleAttribute){
        this.position = position;
        this.generalizationType = generalizationType;
        this.treeEntries = treeEntries;
        this.isSensibleAttribute = isSensibleAttribute;
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, boolean isSensibleAttribute){
        this.position = position;
        this.generalizationType = generalizationType;
        this.isSensibleAttribute = isSensibleAttribute;
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

    public boolean getIsSensibleAttribute() {
        return isSensibleAttribute;
    }

    @Override
    public String toString() {
        return "CastleRule{" +
                "position=" + position +
                ", generalizationType=" + generalizationType +
                ", isSensibleAttribute=" + isSensibleAttribute +
                '}';
    }
}
