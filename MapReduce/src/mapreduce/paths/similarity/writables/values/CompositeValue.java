package mapreduce.paths.similarity.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

public class CompositeValue implements Writable {
	
	private VIntWritable pathNumber;
	private VIntWritable type;
	private Text word;
	private DoubleWritable tfidf;
	private DoubleWritable tfidfSum;
	private DoubleWritable dice;
	private DoubleWritable diceSum;
	private DoubleWritable pmi;
	private DoubleWritable pmiSum;
	
	public CompositeValue(){
		this.pathNumber=new VIntWritable();
		this.word=new Text();
		this.type=new VIntWritable();
		this.tfidfSum=new DoubleWritable(0);
		this.diceSum=new DoubleWritable(0);
		this.pmiSum=new DoubleWritable(0);
		this.tfidf=new DoubleWritable(0);
		this.dice=new DoubleWritable(0);
		this.pmi=new DoubleWritable(0);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		pathNumber.readFields(in);
		type.readFields(in);
		word.readFields(in);
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
		type.write(out);
		word.write(out);
		tfidfSum.write(out);
		diceSum.write(out);
		pmiSum.write(out);
		tfidf.write(out);
		dice.write(out);
		pmi.write(out);	
	}
	public void set(PathCompositeValue value){
		this.word=value.getWord();
		this.dice=value.getDice();
		this.diceSum=value.getDiceSum();
		this.type=value.getType();
		this.pmi=value.getPmi();
		this.pmiSum=value.getPmiSum();
		this.tfidf=value.getTfidf();
		this.tfidfSum=value.getTfidfSum();
	}
	public VIntWritable getType() {
		return type;
	}
	public void setType(VIntWritable type) {
		this.type = type;
	}
	public Text getWord() {
		return word;
	}
	public void setWord(Text word) {
		this.word = word;
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
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(pathNumber);
		builder.append("\t");
		builder.append(type);
		builder.append("\t");
		builder.append(word);
		builder.append("\t");
		builder.append(tfidf);
		builder.append("\t");
		builder.append(dice);
		builder.append("\t");
		builder.append(pmi);
		builder.append("\t");
		builder.append(tfidfSum);
		builder.append("\t");
		builder.append(diceSum);
		builder.append("\t");
		builder.append(pmiSum);
		return builder.toString();
	}
	public VIntWritable getPathNumber() {
		return pathNumber;
	}
	public void setPathNumber(VIntWritable pathNumber) {
		this.pathNumber = pathNumber;
	}
	public void setPathNumber(int pathNumber) {
		this.pathNumber.set(pathNumber);
	}
}
