package mapreduce.paths.similarity.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mapreduce.paths.similarity.writables.values.CompositeValue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

public class FeatureVectorWithMarker implements Writable {
	
	private VIntWritable pathNumber;
	private DoubleWritable tfidf;
	private DoubleWritable tfidfSum;
	private DoubleWritable dice;
	private DoubleWritable diceSum;
	private DoubleWritable pmi;
	private DoubleWritable pmiSum;
	
	public FeatureVectorWithMarker(){
		this.pathNumber=new VIntWritable();
		this.tfidfSum=new DoubleWritable();
		this.diceSum=new DoubleWritable();
		this.pmiSum=new DoubleWritable();
		this.tfidf=new DoubleWritable();
		this.dice=new DoubleWritable();
		this.pmi=new DoubleWritable();
	}
	public void set(FeatureVectorWithMarker temp){
		this.pathNumber.set(temp.getPathNumber().get());
		this.tfidfSum.set(temp.getTfidfSum().get());
		this.diceSum.set(temp.getDiceSum().get());
		this.pmiSum.set(temp.getPmiSum().get());
		this.tfidf.set(temp.getTfidf().get());
		this.dice.set(temp.getDice().get());
		this.pmi.set(temp.getPmi().get());
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		pathNumber.readFields(in);
		tfidfSum.readFields(in);
		diceSum.readFields(in);
		pmiSum.readFields(in);
		tfidf.readFields(in);
		dice.readFields(in);
		pmi.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		pathNumber.write(out);
		tfidfSum.write(out);
		diceSum.write(out);
		pmiSum.write(out);
		tfidf.write(out);
		dice.write(out);
		pmi.write(out);	
	}

	public void set(CompositeValue data){
		this.pathNumber=data.getPathNumber();
		this.dice=data.getDice();
		this.pmi=data.getPmi();
		this.tfidf=data.getTfidf();
		this.tfidfSum=data.getTfidfSum();
		this.diceSum=data.getDiceSum();
		this.pmiSum=data.getPmiSum();
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(this.pathNumber.get());
		builder.append("\t");
		builder.append(this.tfidf.get());
		builder.append("\t");
		builder.append(this.dice.get());
		builder.append("\t");
		builder.append(this.pmi.get());
		builder.append("\t");
		builder.append(this.tfidfSum.get());
		builder.append("\t");
		builder.append(this.diceSum.get());
		builder.append("\t");
		builder.append(this.pmiSum.get());
		return builder.toString();
	}
	public VIntWritable getPathNumber() {
		return pathNumber;
	}
	public void setPathNumber(VIntWritable pathNumber) {
		this.pathNumber = pathNumber;
	}
	public DoubleWritable getTfidf() {
		return tfidf;
	}
	public void setTfidf(DoubleWritable tfidf) {
		this.tfidf = tfidf;
	}
	public DoubleWritable getTfidfSum() {
		return tfidfSum;
	}
	public void setTfidfSum(DoubleWritable tfidfSum) {
		this.tfidfSum = tfidfSum;
	}
	public DoubleWritable getDice() {
		return dice;
	}
	public void setDice(DoubleWritable dice) {
		this.dice = dice;
	}
	public DoubleWritable getDiceSum() {
		return diceSum;
	}
	public void setDiceSum(DoubleWritable diceSum) {
		this.diceSum = diceSum;
	}
	public DoubleWritable getPmi() {
		return pmi;
	}
	public void setPmi(DoubleWritable pmi) {
		this.pmi = pmi;
	}
	public DoubleWritable getPmiSum() {
		return pmiSum;
	}
	public void setPmiSum(DoubleWritable pmiSum) {
		this.pmiSum = pmiSum;
	}
}
