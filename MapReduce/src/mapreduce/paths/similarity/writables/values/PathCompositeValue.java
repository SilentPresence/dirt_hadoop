package mapreduce.paths.similarity.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

public class PathCompositeValue implements Writable {
	
	private Text path;
	
	private VIntWritable type;
	private Text word;
	private DoubleWritable tfidf;
	private DoubleWritable tfidfSum;
	private DoubleWritable dice;
	private DoubleWritable diceSum;
	private DoubleWritable pmi;
	private DoubleWritable pmiSum;
	
	public PathCompositeValue(){
		this.word=new Text();
		this.type=new VIntWritable();
		this.path=new Text();
		this.tfidfSum=new DoubleWritable();
		this.diceSum=new DoubleWritable();
		this.pmiSum=new DoubleWritable();
		this.tfidf=new DoubleWritable();
		this.dice=new DoubleWritable();
		this.pmi=new DoubleWritable();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		path.readFields(in);
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
		path.write(out);
		type.write(out);
		word.write(out);
		tfidfSum.write(out);
		diceSum.write(out);
		pmiSum.write(out);
		tfidf.write(out);
		dice.write(out);
		pmi.write(out);	
	}
	public void setFromCorpus(String[] tokens){
		this.type.set(Integer.parseInt(tokens[1]));
		this.word.set(tokens[2]);
		this.tfidf.set(Double.parseDouble(tokens[3]));
		this.dice.set(Double.parseDouble(tokens[4]));
		this.pmi.set(Double.parseDouble(tokens[5]));
		this.tfidfSum.set(Double.parseDouble(tokens[6]));
		this.diceSum.set(Double.parseDouble(tokens[7]));
		this.pmiSum.set(Double.parseDouble(tokens[8]));
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
		builder.append(path);
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
	public Text getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path.set(path);
	}
	public void setPath(Text path) {
		this.path = path;
	}
}
