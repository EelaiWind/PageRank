package PageRank;

import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashSet;
import java.util.Collection;

public class RankAndAdjacentNodesPair implements Writable{
	private double rank;
	private HashSet<Integer> adjacentNodes;

	public RankAndAdjacentNodesPair(){
		adjacentNodes = new HashSet<Integer>();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(rank);
		out.writeInt(adjacentNodes.size());
		for (Integer adjacentNode : adjacentNodes){
			out.writeInt(adjacentNode);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		rank = in.readDouble();
		int outDegree = in.readInt();
		adjacentNodes.clear();
		adjacentNodes.clear();
		for (int i = 0 ; i < outDegree; i++){
			adjacentNodes.add(in.readInt());
		}
	}

	public void setRank(double rank){
		this.rank = rank;
	}

	public void clearAdjacentNodes(){
		adjacentNodes.clear();
	}

	public void addAdjacentNode(int nodeId){
		adjacentNodes.add(nodeId);
	}

	public void addAdjacentNodes(Collection<Integer> collection){
		adjacentNodes.addAll(collection);
	}

	public double getRank(){
		return rank;
	}

	public Integer[] getAdjacentNodes(){
		Integer[] nodes = new Integer[adjacentNodes.size()];
		return adjacentNodes.toArray(nodes);
	}

	public int getOutDegree(){
		return adjacentNodes.size();
	}

	public String toString(){
		String result = "";
		result += rank;
		for (Integer adjacentNode : adjacentNodes){
			result += " " + adjacentNode;
		}
		return result;
	}

}