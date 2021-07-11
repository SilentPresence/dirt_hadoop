package mapreduce.paths.similarity.writables.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class SimilarityVector implements Writable {

	private DoubleWritable cosine;
	private DoubleWritable cover;
	private DoubleWritable sim;

	public SimilarityVector(){
		this.cosine=new DoubleWritable();
		this.cover=new DoubleWritable();
		this.sim=new DoubleWritable();
	}
	public void set(double tfidfSumProduct,double diceFirstSum,double pmiSum,double tfidfProductSum,double diceIntersectionSum,double pmiIntersectionSum){
		cosine.set((tfidfProductSum)/(tfidfSumProduct));
		cover.set((diceIntersectionSum)/(diceFirstSum));
		sim.set((pmiIntersectionSum)/(pmiSum));
	}
	public void set(double cosine,double cover,double sim){
		this.cosine.set(cosine);
		this.cover.set(cover);
		this.sim.set(sim);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		cosine.readFields(in);
		cover.readFields(in);
		sim.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		cosine.write(out);
		cover.write(out);
		sim.write(out);
	}

	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(String.format("%.6f", cosine.get()));
		builder.append("\t");
		builder.append(String.format("%.6f", cover.get()));
		builder.append("\t");
		builder.append(String.format("%.6f", sim.get()));
		return builder.toString();
	}
	public DoubleWritable getCosine() {
		return cosine;
	}
	public void setCosine(DoubleWritable cosine) {
		this.cosine = cosine;
	}
	public DoubleWritable getCover() {
		return cover;
	}
	public void setCover(DoubleWritable cover) {
		this.cover = cover;
	}
	public DoubleWritable getSim() {
		return sim;
	}
	public void setSim(DoubleWritable sim) {
		this.sim = sim;
	}
}
