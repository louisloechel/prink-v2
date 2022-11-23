package prink.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;
import prink.CastleFunction;

import java.util.ArrayList;

/**
 * Rule definition function used by Prink to configure rule set
 */
public class CastleRule {

    private final int position;
    private final CastleFunction.Generalization generalizationType;
    private Tuple2<Float, Float> domain;
    private ArrayList<String[]> treeEntries;
    private final boolean isSensibleAttribute;
    private double infoLossMultiplier;

    public CastleRule(int position, CastleFunction.Generalization generalizationType, Tuple2<Float,Float> domain, boolean isSensibleAttribute, double infoLossMultiplier){
        this.position = position;
        this.generalizationType = generalizationType;
        this.domain = domain;
        this.isSensibleAttribute = isSensibleAttribute;
        this.infoLossMultiplier = infoLossMultiplier;
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, ArrayList<String[]> treeEntries, boolean isSensibleAttribute, double infoLossMultiplier){
        this.position = position;
        this.generalizationType = generalizationType;
        this.treeEntries = treeEntries;
        this.isSensibleAttribute = isSensibleAttribute;
        this.infoLossMultiplier = infoLossMultiplier;
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, boolean isSensibleAttribute, double infoLossMultiplier){
        this.position = position;
        this.generalizationType = generalizationType;
        this.isSensibleAttribute = isSensibleAttribute;
        this.infoLossMultiplier = infoLossMultiplier;
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, Tuple2<Float,Float> domain, boolean isSensibleAttribute){
        this(position, generalizationType, domain, isSensibleAttribute, 0.0d);
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, ArrayList<String[]> treeEntries, boolean isSensibleAttribute){
        this(position, generalizationType, treeEntries, isSensibleAttribute, 0.0d);
    }

    public CastleRule(int position, CastleFunction.Generalization generalizationType, boolean isSensibleAttribute){
        this(position, generalizationType, isSensibleAttribute, 0.0d);
    }

    /**
     * Sets the rule multiplier value that gets multiplied with the resulting information loss of this rule
     * for the overall cluster information loss value
     * @param multiplier The new multiplier (needs to be bigger than 0)
     */
    public void setInfoLossMultiplier(float multiplier) {
        if(multiplier > 0) infoLossMultiplier = multiplier;
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

    public double getInfoLossMultiplier() {
        return infoLossMultiplier;
    }

    @Override
    public String toString() {
        return "CastleRule{" +
                "pos=" + position +
                ",gType=" + generalizationType +
                ",SAtt=" + isSensibleAttribute +
                ",ILM=" + infoLossMultiplier +
                '}';
    }
}
