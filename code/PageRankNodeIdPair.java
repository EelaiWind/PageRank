package PageRank;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.Math;

public class PageRankNodeIdPair implements WritableComparable{
	private double pageRank;
	private int nodeId;
	public PageRankNodeIdPair(){
		pageRank = -1;
		nodeId = -1;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(pageRank);
		out.writeInt(nodeId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageRank = in.readDouble();
		nodeId = in.readInt();
	}

	@Override
	public int compareTo(Object another) {
		PageRankNodeIdPair obj = (PageRankNodeIdPair) another;

		int pageRankComparision = Double.compare(getPageRank(), obj.getPageRank());
		if ( pageRankComparision == 0 ){
			return Integer.compare( getNodeId(), obj.getNodeId() );
		}
		else{
			return -1*pageRankComparision;
		}
	}

	public double getPageRank(){
		return pageRank;
	}

	public int getNodeId(){
		return nodeId;
	}

	public void setPageRank(double pageRank){
		this.pageRank = pageRank;
	}

	public void setNodeId(int nodeId){
		this.nodeId = nodeId;
	}

	public String toString(){
		return nodeId+" "+pageRank;
	}
}