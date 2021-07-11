package mapreduce.paths.similarity.writables.key;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class TemplateKey implements WritableComparable<TemplateKey> {
	private Text path;
	private Text candidate;

	public TemplateKey(){
		set(new Text(),new Text());
	}
	public TemplateKey(Text path,Text candidate){
		set(path,candidate);
	}
	public void set(Text path,Text candidate){
		this.path=path;
		this.candidate=candidate;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		path.readFields(in);
		candidate.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		path.write(out);
		candidate.write(out);
	}

	@Override
	public int compareTo(TemplateKey templateKey) {
		int compare=path.compareTo(templateKey.path);
		if(compare!=0){
			return compare;
		}
		else{
			return candidate.compareTo(templateKey.candidate);
		}
	}
	public Text getPath(){
		return this.path;
	}
	public void setPath(String path){
		this.path.set(path);
	}
	public int hashCode(){
		int hash=17;
		hash=hash*31+path.hashCode();
		hash=hash*31+candidate.hashCode();
		return hash;
	}
	public boolean equals(Object o){
		if(o instanceof TemplateKey){
			TemplateKey templateKey=(TemplateKey)o;
			return path.equals(templateKey.path)&&candidate.equals(templateKey.candidate);
		}
		return false;
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(path);
		builder.append("\t");
		builder.append(candidate);
		return builder.toString();
	}
	public static class ComparatorTagPath extends WritableComparator{
		private static final Text.Comparator TEXT_COMARATOR=new Text.Comparator();
		public ComparatorTagPath(){
			super(TemplateKey.class);
		}

		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
			try {
				int firstStrLength1 = WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
				int firstStrLength2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
				//Compare the path
				int compare=TEXT_COMARATOR.compare(b1, s1, firstStrLength1, b2, s2, firstStrLength2);
				if(compare!=0){
					return compare;
				}
				else{
					int secondStrLength1=WritableUtils.decodeVIntSize(b1[s1+firstStrLength1])+readVInt(b1,s1+firstStrLength1);
					int secondStrLength2=WritableUtils.decodeVIntSize(b2[s2+firstStrLength1])+readVInt(b2,s2+firstStrLength1);
					return TEXT_COMARATOR.compare(b1, s1, firstStrLength1+secondStrLength1, b2, s2, firstStrLength2+secondStrLength2);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return l1;
		}
	}
	public Text getCandidate() {
		return candidate;
	}
	public void setCandidate(Text candidate) {
		this.candidate = candidate;
	}
	public void setCandidate(String candidate) {
		this.candidate.set(candidate);
	}
	public void setPath(Text path) {
		this.path = path;
	}
}
