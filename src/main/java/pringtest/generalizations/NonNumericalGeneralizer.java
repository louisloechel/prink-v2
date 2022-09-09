package pringtest.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pringtest.CastleFunction;
import pringtest.datatypes.CastleRule;
import pringtest.datatypes.TreeNode;

import java.util.HashMap;
import java.util.List;

public class NonNumericalGeneralizer implements BaseGeneralizer{

    private final CastleRule[] config;
    private final String ROOT_NAME = "<blank>";
    private final HashMap<Integer, TreeNode> trees = new HashMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(NonNumericalGeneralizer.class);

    public NonNumericalGeneralizer(CastleRule[] rules){
        this.config = rules;
        // Add rule defined trees to the generalizer
        for(int i = 0; i < rules.length; i++){
            if(rules[i].getGeneralizationType() == CastleFunction.Generalization.NONNUMERICAL && rules[i].getTreeEntries() != null){
                TreeNode tree = new TreeNode(ROOT_NAME);
                for(String[] treeEntry: rules[i].getTreeEntries()){
                    tree.addByArray(treeEntry, true);
                }
                tree.removeTempNodes();
                trees.put(i, tree);
            }
        }
    }

    @Override
    public Tuple2<String, Float> generalize(int pos) {
        if(!trees.containsKey(pos)){
            LOG.error("Try to generalize with a non existing tree! Tree-Key: {} Existing Tree-Keys: {}", pos, trees.keySet());
            return Tuple2.of("ERROR: Missing tree for position:" + pos, 1.0f);
        }
        return trees.get(pos).getGeneralization(false);
    }

    @Override
    public Tuple2<String, Float> generalize(List<Tuple> withTuples, int pos) {
        if(!trees.containsKey(pos)){
            LOG.error("Try to generalize with a non existing tree! Tree-Key: {} Existing Tree-Keys: {}", pos, trees.keySet());
            return Tuple2.of("ERROR: Missing tree for position:" + pos, 9999.0f);
        }

        // Add withTuples temporary to the tree
        for(Tuple input: withTuples){
            if(input.getField(pos).getClass().isArray()){
                trees.get(pos).addByArray(input.getField(pos), true);
            }else{
                trees.get(pos).addByName(input.getField(pos), true);
            }
        }
        // Calculate generalization
        Tuple2<String, Float> output = trees.get(pos).getGeneralization(true);

        // Remove temporary nodes
        trees.get(pos).removeTempNodes();

        return output;
    }

    @Override
    public Tuple2<String, Float> generalizeMax(int pos) {
        return Tuple2.of(ROOT_NAME, 1.0f);
    }

    public void updateTree(Tuple input) {
        for(int i = 0; i < config.length; i++){
            if(config[i].getGeneralizationType() == CastleFunction.Generalization.NONNUMERICAL){
                // If attribute has no existing tree create a new one
                if(!trees.containsKey(i)) {
                    trees.put(i, new TreeNode("<blank>"));
                }
                // Add new values to tree
                if(input.getField(i).getClass().isArray()){
                    trees.get(i).addByArray(input.getField(i), false);
                }else{
                    trees.get(i).addByName(input.getField(i), false);
                }
            }
        }
    }
}
