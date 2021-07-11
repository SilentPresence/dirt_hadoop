package mapreduce.paths.similarity.writables.key;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class TemplateTripleWithoutWord implements WritableComparable<TemplateTripleWithoutWord> {
	private Text template;
	private VIntWritable type;

	public TemplateTripleWithoutWord(){
		set(new Text(),new VIntWritable(-1),new Text());
	}
	public TemplateTripleWithoutWord(TemplateKey key){
		set(key.getPath().toString()+"\t"+key.getCandidate().toString(),new VIntWritable(-1),new Text());
	}
	public TemplateTripleWithoutWord(int type){
		set(new Text(),new VIntWritable(type),new Text());
	}
	public TemplateTripleWithoutWord(int type,String word){
		set(new Text(),new VIntWritable(type),new Text(word));
	}
	public TemplateTripleWithoutWord(String template,int type,String word){
		set(new Text(template),new VIntWritable(type),new Text(word));
	}
	public TemplateTripleWithoutWord(Text template,int type,Text word){
		set(template,new VIntWritable(type),word);
	}	
	public void set(TemplateTriple oldKey){
		this.template=oldKey.getTemplate();
		this.type=oldKey.getType();
	}
	public void set(String string, VIntWritable type, Text word) {
		this.template.set(string);
		this.type=type;

	}
	public void setTemplate(TemplateKey key) {
		this.template.set(key.getPath().toString()+"\t"+key.getCandidate().toString());
	}

	public void set(String template,int type,String word){
		this.type.set(type);
		this.template.set(template);
	}
	public void set(Text template,VIntWritable type,Text word){
		this.type=type;
		this.template=template;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		template.readFields(in);
		type.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		template.write(out);
		type.write(out);
	}

	@Override
	public int compareTo(TemplateTripleWithoutWord sp) {
		int compare=template.compareTo(sp.template);
		if(compare!=0){
			return compare;
		}
		else{
			return type.compareTo(sp.type);
		}
	}
	public Text getTemplate(){
		return this.template;
	}
	public VIntWritable getType(){
		return this.type;
	}
	public void setTemplate(Text template){
		this.template=template;
	}
	public void setSlotType(VIntWritable type){
		this.type=type;
	}
	public int hashCode(){
		int hash=17;
		hash=hash*31+template.hashCode();
		return hash;
	}
	public boolean equals(Object o){
		if(o instanceof TemplateTripleWithoutWord){
			TemplateTripleWithoutWord wp=(TemplateTripleWithoutWord)o;
			return type.equals(wp.type)&&template.equals(wp.template);
		}
		return false;
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(template);
		builder.append("\t");
		builder.append(type);
		return builder.toString();
	}
	public static class RawComparatorTemplateType extends WritableComparator{
		private static final Text.Comparator TEXT_COMARATOR=new Text.Comparator();
		public RawComparatorTemplateType(){
			super(TemplateTripleWithoutWord.class);
		}

		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
				try 
				{
				int firstStrLength1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
				int firstStrLength2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
				int compare=TEXT_COMARATOR.compare(b1, s1, firstStrLength1, b2, s2, firstStrLength1);
				if(compare!=0){
					return compare;
				}
				else{
					long type1=readVLong(b1, s1+firstStrLength1);
					long type2=readVLong(b2, s2+firstStrLength2);
					return ( type1 < type2 )? -1 : ( type1 == type2 ) ? 0: 1;
				}
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
				return l2;
			}
		}
}
