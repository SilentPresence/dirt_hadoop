package mapreduce.paths.extraction.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

public class TripleData implements Writable {
	//|p,slot,w|
	private VLongWritable slotWordOccurence;
	//|p,slot,*|
	private VLongWritable slotOccurenceInPath;
	//|*,slot,w|
	private VLongWritable totalWordInSlot;
	//|*,slot,*|
	private VLongWritable slotFillers;

	public TripleData(){
		set(new VLongWritable(),new VLongWritable(),new VLongWritable(),new VLongWritable());
	}
	public TripleData(VLongWritable slotWordOccurence,VLongWritable slotOccurenceInPath,VLongWritable totalWordInSlot,VLongWritable slotFillers){
		set(slotWordOccurence,slotOccurenceInPath,totalWordInSlot,slotFillers);
	}
	public void set(VLongWritable slotWordOccurence,VLongWritable slotOccurenceInPath,VLongWritable totalWordInSlot,VLongWritable slotFillers){
		this.slotWordOccurence=slotWordOccurence;
		this.slotOccurenceInPath=slotOccurenceInPath;
		this.totalWordInSlot=totalWordInSlot;
		this.slotFillers=slotFillers;
	}
	public void set(long slotWordOccurence,long slotOccurenceInPath){
		this.slotWordOccurence.set(slotWordOccurence);
		this.slotOccurenceInPath.set(slotOccurenceInPath);
	}
	public void set(PartialTripleData data,long totalWordInSlot,long slotFillers){
		this.slotWordOccurence=data.getSlotWordOccurence();
		this.slotOccurenceInPath=data.getSlotOccurenceInPath();
		this.totalWordInSlot.set(totalWordInSlot);
		this.slotFillers.set(slotFillers);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		slotWordOccurence.readFields(in);
		slotOccurenceInPath.readFields(in);
		totalWordInSlot.readFields(in);
		slotFillers.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		slotWordOccurence.write(out);
		slotOccurenceInPath.write(out);
		totalWordInSlot.write(out);
		slotFillers.write(out);
	}
	public VLongWritable getSlotWordOccurence(){
		return this.slotWordOccurence;
	}
	public VLongWritable getSlotOccurenceInPath(){
		return this.slotOccurenceInPath;
	}
	public VLongWritable getTotalWordInSlot(){
		return this.totalWordInSlot;
	}
	public VLongWritable getSlotFillers(){
		return this.slotFillers;
	}
	public void setSlotWordOccurence(VLongWritable slotWordOccurence){
		this.slotWordOccurence=slotWordOccurence;
	}
	public void setSlotOccurenceInPath(VLongWritable slotOccurenceInPath){
		this.slotOccurenceInPath=slotOccurenceInPath;
	}
	public void setTotalWordInSlot(VLongWritable totalWordInSlot){
		this.totalWordInSlot=totalWordInSlot;
	}
	public void setSlotFillers(VLongWritable slotFillers){
		this.slotFillers=slotFillers;
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(slotWordOccurence);
		builder.append("\t");
		builder.append(slotOccurenceInPath);
		builder.append("\t");
		builder.append(totalWordInSlot);
		builder.append("\t");
		builder.append(slotFillers);
		return builder.toString();
	}
}

