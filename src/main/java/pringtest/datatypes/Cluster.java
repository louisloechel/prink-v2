package pringtest.datatypes;

import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;

public class Cluster {

    private ArrayList<Object> entries = new ArrayList<>();
    private float informationLoss = 0;

    // TODO check if float is needed or can be replaced with int
    public Float enlargementValue(Cluster input) {
        return informationLossWith(input) - informationLoss;
    }

    // TODO check if float is needed or can be replaced with int
    public Float enlargementValue(Object input) {
        return informationLossWith(input) - informationLoss;
    }

    public float informationLossWith(Cluster input) {
        // TODO implement logic
        return 1.0f;
    }

    public float informationLossWith(Object input) {
        // TODO implement logic
        return 1.0f;
    }

    public Object generalize(Object input) {
        // Long, Long, String, Float
        System.out.println("Generalized:" + input);
        return new Tuple4<>(((TaxiFare) input).driverId,((TaxiFare) input).taxiId,"Test",((TaxiFare) input).tolls);
    }

    public float infoLoss() {
        // TODO
        return informationLoss;
    }

    public void addEntry(Object input) {
        System.out.println("Added " + input.toString() + " to cluster: " + this.toString() + " size:" + this.entries.size());
        entries.add(input);
    }

    public void addAllEntries(ArrayList<Object> input){
        entries.addAll(input);
    }

    public void removeEntry(Object input) {
        entries.remove(input);
    }

    public ArrayList<Object> getAllEntries(){
        return entries;
    }

    public boolean contains(Object input) {
        return entries.contains(input);
    }

    public int size() {
        return entries.size();
    }

}
