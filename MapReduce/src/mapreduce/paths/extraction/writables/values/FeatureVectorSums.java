package mapreduce.paths.extraction.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mapreduce.paths.similarity.writables.values.CompositeValue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FeatureVectorSums implements Writable {
	
	
	private Text debug;
	private DoubleWritable tfidf;
	private DoubleWritable tfidfSum;
	private DoubleWritable dice;
	private DoubleWritable diceSum;
	private DoubleWritable pmi;
	private DoubleWritable pmiSum;
	
	public FeatureVectorSums(){
		this.debug=new Text();
		this.tfidfSum=new DoubleWritable(0);
		this.diceSum=new DoubleWritable(0);
		this.pmiSum=new DoubleWritable(0);
		this.tfidf=new DoubleWritable(0);
		this.dice=new DoubleWritable(0);
		this.pmi=new DoubleWritable(0);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		debug.readFields(in);
		tfidfSum.readFields(in);
		diceSum.readFields(in);
		pmiSum.readFields(in);
		tfidf.readFields(in);
		dice.readFields(in);
		pmi.readFields(in);
	}
	public void debug(String debug){
		this.debug.set(debug);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		debug.write(out);
		tfidfSum.write(out);
		diceSum.write(out);
		pmiSum.write(out);
		tfidf.write(out);
		dice.write(out);
		pmi.write(out);	
	}
	public void set(FeatureVector data,DoubleWritable tfidfSum,DoubleWritable diceSum,DoubleWritable pmiSum){
		this.debug.set("");
		this.dice=data.getDice();
		this.pmi=data.getPmi();
		this.tfidf=data.getTfidf();
		this.tfidfSum=tfidfSum;
		this.diceSum=diceSum;
		this.pmiSum=pmiSum;
	}
	public void set(CompositeValue data){
		this.dice=data.getDice();
		this.pmi=data.getPmi();
		this.tfidf=data.getTfidf();
		this.tfidfSum=data.getTfidfSum();
		this.diceSum=data.getDiceSum();
		this.pmiSum=data.getPmiSum();
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
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
}
