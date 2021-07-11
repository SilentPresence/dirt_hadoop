package mapreduce.paths.similarity.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FeatureIntersection implements Writable {
	
	private Text word;
	private DoubleWritable tfidfIntersection;
	private DoubleWritable tfidfSumProduct;
	private DoubleWritable diceIntersection;
	private DoubleWritable diceFirstSum;
	private DoubleWritable pmiIntersection;
	private DoubleWritable pmiSum;
	
	public FeatureIntersection(){
		this.word=new Text();
		this.tfidfIntersection=new DoubleWritable();
		this.tfidfSumProduct=new DoubleWritable();
		this.diceIntersection=new DoubleWritable();
		this.diceFirstSum=new DoubleWritable();
		this.pmiIntersection=new DoubleWritable();
		this.pmiSum=new DoubleWritable();

	}
	public void set(FeatureVectorWithMarker first,FeatureVectorWithMarker second,Text word){
		this.word.set(word.toString());;
		tfidfIntersection.set(first.getTfidf().get()*second.getTfidf().get());
		tfidfSumProduct.set(first.getTfidfSum().get()*second.getTfidfSum().get());
		diceIntersection.set(first.getDice().get());
		diceFirstSum.set(first.getDiceSum().get());
		pmiIntersection.set(first.getPmi().get()+second.getPmi().get());
		pmiSum.set(first.getPmiSum().get()+second.getPmiSum().get());
	}	

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		tfidfIntersection.readFields(in);
		tfidfSumProduct.readFields(in);
		diceIntersection.readFields(in);
		diceFirstSum.readFields(in);
		pmiIntersection.readFields(in);
		pmiSum.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		tfidfIntersection.write(out);
		tfidfSumProduct.write(out);
		diceIntersection.write(out);
		diceFirstSum.write(out);
		pmiIntersection.write(out);
		pmiSum.write(out);
	}

	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(word);
		builder.append("\t");
		builder.append(tfidfIntersection.get());
		builder.append("\t");
		builder.append(tfidfSumProduct.get());
		builder.append("\t");
		builder.append(diceIntersection.get());
		builder.append("\t");
		builder.append(diceFirstSum.get());
		builder.append("\t");
		builder.append(pmiIntersection.get());
		builder.append("\t");
		builder.append(pmiSum.get());
		return builder.toString();
	}
	public DoubleWritable getTfidfIntersection() {
		return tfidfIntersection;
	}
	public void setTfidfIntersectionProduct(DoubleWritable tfidfIntersection) {
		this.tfidfIntersection = tfidfIntersection;
	}
	public DoubleWritable getTfidfSumProduct() {
		return tfidfSumProduct;
	}
	public void setTfidfSumProduct(DoubleWritable tfidfSumProduct) {
		this.tfidfSumProduct = tfidfSumProduct;
	}
	public DoubleWritable getDiceIntersection() {
		return diceIntersection;
	}
	public void setDiceIntersectionSum(DoubleWritable diceIntersection) {
		this.diceIntersection = diceIntersection;
	}
	public DoubleWritable getDiceFirstSum() {
		return diceFirstSum;
	}
	public void setDiceFirstSum(DoubleWritable diceFirstSum) {
		this.diceFirstSum = diceFirstSum;
	}
	public DoubleWritable getPmiIntersection() {
		return pmiIntersection;
	}
	public void setPmiIntersectionSum(DoubleWritable pmiIntersection) {
		this.pmiIntersection = pmiIntersection;
	}
	public DoubleWritable getPmiSum() {
		return pmiSum;
	}
	public void setPmiSum(DoubleWritable pmiSum) {
		this.pmiSum = pmiSum;
	}
	public Text getWord() {
		return word;
	}
	public void setWord(Text word) {
		this.word = word;
	}
	public void setTfidfIntersection(DoubleWritable tfidfIntersection) {
		this.tfidfIntersection = tfidfIntersection;
	}
	public void setDiceIntersection(DoubleWritable diceIntersection) {
		this.diceIntersection = diceIntersection;
	}
	public void setPmiIntersection(DoubleWritable pmiIntersection) {
		this.pmiIntersection = pmiIntersection;
	}
}
