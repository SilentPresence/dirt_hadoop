package mapreduce.paths.extraction.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

public class PartialTripleData implements Writable  {
	//|p,slot,w|
	private VLongWritable slotWordOccurence;
	//|p,slot,*|
	private VLongWritable slotOccurenceInPath;

	public PartialTripleData(){
		set(new VLongWritable(),new VLongWritable());
	}
	public PartialTripleData(VLongWritable wordPairOccurence,VLongWritable slotOccurenceInPath){
		set(wordPairOccurence,slotOccurenceInPath);
	}
	public void set(VLongWritable slotWordOccurence,VLongWritable slotOccurenceInPath){
		this.slotWordOccurence=slotWordOccurence;
		this.slotOccurenceInPath=slotOccurenceInPath;
	}
	public void set(long slotWordOccurence,long slotOccurenceInPath){
		this.slotWordOccurence.set(slotWordOccurence);
		this.slotOccurenceInPath.set(slotOccurenceInPath);
	}
	public void set(PartialTripleData data){
		this.slotWordOccurence=data.getSlotWordOccurence();
		this.slotOccurenceInPath=data.getSlotOccurenceInPath();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		slotWordOccurence.readFields(in);
		slotOccurenceInPath.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		slotWordOccurence.write(out);
		slotOccurenceInPath.write(out);
	}
	public VLongWritable getSlotWordOccurence(){
		return this.slotWordOccurence;
	}
	public VLongWritable getSlotOccurenceInPath(){
		return this.slotOccurenceInPath;
	}
	public void setSlotWordOccurence(VLongWritable slotWordOccurence){
		this.slotWordOccurence=slotWordOccurence;
	}
	public void setSlotOccurenceInPath(VLongWritable slotOccurenceInPath){
		this.slotOccurenceInPath=slotOccurenceInPath;
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(slotWordOccurence);
		builder.append("\t");
		builder.append(slotOccurenceInPath);
		return builder.toString();
	}
}
