package pringtest.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Set;
import java.util.TreeSet;

public class TreeNode implements Comparable<TreeNode>{
    private final TreeNode parent;
    private final String content;
    private final Set<TreeNode> children = new TreeSet<>(); //TODO check if TreeSet is the best choice
    private int coverage = 0;
    /** Parameters to measure the enlargement value/infoLoss for new node */
    private int coverageTemporary = 0;
    private boolean isTemporary;

    public TreeNode(String content, TreeNode parent, boolean isTemporary){
        this.content = content;
        this.parent = parent;
        this.isTemporary = isTemporary;
    }

    public TreeNode(String content){
        this.content = content;
        this.parent = null;
    }

    /**
     * Adds a node as a child
     * @param input Value to add as new node
     * @param isTemporary If the node is just temporary added to the tree
     * @return The added tree node
     */
    private TreeNode addNode(String input,boolean isTemporary){
        TreeNode newNode = new TreeNode(input, this, isTemporary);
        children.add(newNode);
        return newNode;
    }

    /**
     * Checks if the input value is contained inside the tree
     * @param input Value to search for
     * @return The node that includes the input value. If the value in not contained inside the tree returns null
     */
    public TreeNode containsContent(String input) {
        if(content.equals(input)){
            return this;
        }else{
            // Search recursively through the tree
            for(TreeNode child: children){
                TreeNode temp = child.containsContent(input);
                if(temp != null) return temp;
            }
            return null;
        }
    }

    /**
     * Check if the input values are already present inside the tree and if not add them at the correct position
     * @param input Values to check in the hierarchy that should be used
     */
    public void addByArray(String[] input, boolean isTemporary){
        // Check if a value of the array is already present inside the tree
        for(int i = input.length-1; i >= 0; i--) {
            TreeNode foundNode = containsContent(input[i]);
            if(foundNode != null){
                // ContainingNode is the smallest value of the input (the value with the most information aka the leaf)
                TreeNode containingNode = foundNode;
                for(int j = i+1; j < input.length; j++) {
                    containingNode = containingNode.addNode(input[j], isTemporary);
                }
                containingNode.updateParentCoverage(isTemporary);
                return;
            }
        }
        // If no value is present add hierarchy to the root
        // ContainingNode is the smallest value of the input (the value with the most information aka the leaf)
        TreeNode containingNode = this;
        for(String value: input) {
            containingNode = containingNode.addNode(value, isTemporary);
        }
        containingNode.updateParentCoverage(isTemporary);
    }

    /**
     * Check if the input is already present inside the tree and if not add it to the root
     * @param input Value to check and if not present add
     */
    public void addByName(String input, boolean isTemporary){
        TreeNode containingNode = containsContent(input);
        if(containingNode == null){
            containingNode = addNode(input, isTemporary);
        }
        containingNode.updateParentCoverage(isTemporary);
    }

    /**
     * Add one to the coverage of all the parents (including root)
     */
    private void updateParentCoverage(boolean isTemporary){
        if(isTemporary){
            coverageTemporary++;
            if(parent != null) parent.updateParentCoverage(true);
        }else{
            coverage++;
            // Also update the temporary coverage to reflect the change
            coverageTemporary = coverage;
            if(parent != null) parent.updateParentCoverage(false);
        }
    }

    @Override
    public int compareTo(TreeNode o) {
        return String.CASE_INSENSITIVE_ORDER.compare(this.content, o.content);
    }

    /**
     * Get the best generalization of the tree
     * (the root that calls this function defines the needed coverage)
     * @return best generalisation as a Tuple2 containing generalisation and information loss
     */
    public Tuple2<String, Float> getGeneralization(boolean isTemporary) {
        int neededCoverage = (isTemporary ? coverageTemporary : coverage);
        TreeNode generalizationNode = this;
        for(TreeNode child: children){
            if(!isTemporary && child.coverage == neededCoverage) generalizationNode = child.getGeneralizationNode(neededCoverage, false);
            if(isTemporary && child.coverageTemporary == neededCoverage) generalizationNode = child.getGeneralizationNode(neededCoverage, true);
        }
        // Calculate information loss based on generalization node
        return Tuple2.of(generalizationNode.content, generalizationNode.infoLoss(numOfLeaves()));
    }

    /**
     * Returns the child with the needed coverage and if no such child exists itself
     * @param neededCoverage the amount of tuple coverage needed
     * @return TreeNode with the required coverage or itself
     */
    private TreeNode getGeneralizationNode(int neededCoverage, boolean isTemporary) {
        for(TreeNode child: children){
            if(!isTemporary && child.coverage == neededCoverage) return child.getGeneralizationNode(neededCoverage, false);
            if(isTemporary && child.coverageTemporary == neededCoverage) return child.getGeneralizationNode(neededCoverage, true);
        }
        return this;
    }

    /**
     * Returns the information loss of the TreeNode in relation to the total number of leave nodes
     * information loss = ((count of subtree leaves) - 1) / ((total count of leaves) - 1)
     * @param totalNumLeaves Total number of leave nodes inside the tree
     * @return information loss of the tree node
     */
    private float infoLoss(int totalNumLeaves) {
        int subtreeLeaves = numOfLeaves()-1;
        // Return 0 if the subtree in question is a leaf node
        if(subtreeLeaves <= 0) System.out.println("DEBUG: infoLoss test: (" + numOfLeaves() + "-1)/(" + totalNumLeaves + "-1) = " + 0);
        if(subtreeLeaves <= 0) return 0f;
        System.out.println("DEBUG: infoLoss test: (" + numOfLeaves() + "-1)/(" + totalNumLeaves + "-1) = " + ((float)(numOfLeaves()-1)/(float)(totalNumLeaves-1)));
        return ((float)(numOfLeaves()-1)/(float)(totalNumLeaves-1));
    }

    /**
     * Calculate the number of leaves inside the tree
     * @return Number of existing leaves inside this root node
     */
    private int numOfLeaves(){
        if(children.size() <= 0) return 1;
        int output = 0;
        for(TreeNode child: children){
            output = output + child.numOfLeaves();
        }
        return output;
    }

    /**
     * Removes all added temporary nodes and resets coverageTemporary back to 0
     */
    public void removeTempNodes() {
        coverageTemporary = coverage;
        // TODO test if keeping "empty" nodes improves infoLoss
//            children.removeIf(child -> child.isTemporary);
        for(TreeNode child: children){
            child.removeTempNodes();
        }
    }

    public void printTree(int depth) {
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < depth; i++) {
            builder.append("-");
        }
        builder.append("> ").append(content).append(" coverage:").append(coverage).append(" coverageTemp:").append(coverageTemporary).append(" (parent:").append((parent != null) ? parent.content : "X").append(") isTem:").append(isTemporary);
        System.out.println(builder.toString());

        for(TreeNode node: children){
            node.printTree(depth+1);
        }
    }

}
