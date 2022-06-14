package pringtest.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import pringtest.CastleFunction;

import java.util.*;

public class NonNumericalGeneralizer implements BaseGeneralizer{

    private final CastleFunction.Generalization[] config;
    private final HashMap<Integer, TreeNode> trees = new HashMap<>();

    public NonNumericalGeneralizer(CastleFunction.Generalization[] config){
        this.config = config;
    }

    @Override
    public Tuple2<?, Float> generalize(int pos) {
        // TODO add code
        return Tuple2.of("TEST", 1.0f);
    }

    @Override
    public Tuple2<?, Float> generalize(List<Tuple> withTuples, int pos) {
        // TODO add code
        return Tuple2.of("TEST", 1.0f);
    }

    public void updateTree(Tuple input) {
        for (int i = 0; i < config.length; i++) {
            if (config[i] == CastleFunction.Generalization.NONNUMERICAL) {
//                System.out.println("Update Tree class:" + input.getField(i).getClass());

                if (!trees.containsKey(i)) {
                    trees.put(i, new TreeNode(input.getField(i)));
                } else if(!trees.get(i).containsContent(input.getField(i))){
                    trees.get(i).addNode(input.getField(i));
                }
            }
        }
    }

    public class TreeNode<T> implements Comparable<TreeNode>{
        private final TreeNode<T> parent;
        private final T content;
        private final Set<TreeNode<T>> children = new TreeSet<>(); //TODO check if TreeSet is the best choice

        public TreeNode(T content, TreeNode<T> parent){
            this.content = content;
            this.parent = parent;
        }

        public TreeNode(T content){
            this.content = content;
            this.parent = null;
        }

        public void addNode(T input){
            TreeNode newNode = new TreeNode(input, this);
            children.add(newNode);
        }

        public boolean containsContent(Object field) {
            // TODO implement functionality
            return false;
        }

        @Override
        public int compareTo(NonNumericalGeneralizer.TreeNode o) {
            return String.CASE_INSENSITIVE_ORDER.compare((String) this.content, (String) o.content);
        }
    }

}
