package mapreduce.paths.extraction.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FeatureVector implements Writable {
	
	private DoubleWritable tfidf;
	private DoubleWritable dice;
	private DoubleWritable pmi;
	
	public FeatureVector(){
		this.tfidf=new DoubleWritable();
		this.dice=new DoubleWritable();
		this.pmi=new DoubleWritable();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		tfidf.readFields(in);
		dice.readFields(in);
		pmi.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		tfidf.write(out);
		dice.write(out);
		pmi.write(out);	
	}
	public void set(double tfidf,double dice,double pmi){
		this.tfidf.set(tfidf);
		this.dice.set(dice);
		this.pmi.set(pmi);
	}
	public DoubleWritable getTfidf(){
		return this.tfidf;
	}
	public DoubleWritable getDice(){
		return this.dice;
	}
	public DoubleWritable getPmi(){
		return this.pmi;
	}
	public void calculateVector(double slotWordOccurence,double slotOccurenceInPath,double totalWordInSlot,double slotFillers){
		calculateTfidf(slotWordOccurence,totalWordInSlot);
		calculateDice(slotWordOccurence,slotOccurenceInPath,totalWordInSlot);
		calculatePmi(slotWordOccurence,slotOccurenceInPath,totalWordInSlot,slotFillers);
	}
	public void calculateVector(PartialTripleData data,double totalWordInSlot,double slotFillers){
		calculateTfidf((double)data.getSlotWordOccurence().get(),totalWordInSlot);
		calculateDice((double)data.getSlotWordOccurence().get(),(double)data.getSlotOccurenceInPath().get(),totalWordInSlot);
		calculatePmi((double)data.getSlotWordOccurence().get(),(double)data.getSlotOccurenceInPath().get(),totalWordInSlot,slotFillers);
	}
	public void calculateTfidf(double slotWordOccurence,double totalWordInSlot){
		double denominator=1.0+Math.log10(totalWordInSlot);
		this.tfidf.set((slotWordOccurence/denominator));
	}
	public void calculateDice(double slotWordOccurence,double slotOccurenceInPath,double totalWordInSlot){
		this.dice.set(((2*slotWordOccurence)/(slotOccurenceInPath+totalWordInSlot)));
	}
	public void calculatePmi(double slotWordOccurence,double slotOccurenceInPath,double totalWordInSlot,double slotFillers){
		this.pmi.set(Math.log10(slotWordOccurence)+Math.log10(slotFillers)-Math.log10(slotOccurenceInPath)-Math.log10(totalWordInSlot));
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(this.tfidf.get());
		builder.append("\t");
		builder.append(this.dice.get());
		builder.append("\t");
		builder.append(this.pmi.get());
		return builder.toString();
	}
}
