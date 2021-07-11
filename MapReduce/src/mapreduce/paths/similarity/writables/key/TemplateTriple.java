package mapreduce.paths.similarity.writables.key;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class TemplateTriple implements WritableComparable<TemplateTriple> {
	private Text template;
	private VIntWritable type;
	private Text word;

	public TemplateTriple(){
		set(new Text(),new VIntWritable(),new Text());
	}
	public TemplateTriple(TemplateKey key){
		set(key.getPath().toString()+"\t"+key.getCandidate().toString(),new VIntWritable(-1),new Text());
	}
	public TemplateTriple(int type){
		set(new Text(),new VIntWritable(type),new Text());
	}
	public TemplateTriple(int type,String word){
		set(new Text(),new VIntWritable(type),new Text(word));
	}
	public TemplateTriple(String template,int type,String word){
		set(new Text(template),new VIntWritable(type),new Text(word));
	}
	public TemplateTriple(Text template,int type,Text word){
		set(template,new VIntWritable(type),word);
	}	

	public void set(String string, VIntWritable type, Text word) {
		this.template.set(string);
		this.type=type;
		this.word=word;

	}
	public void setTemplate(TemplateKey key) {
		this.template.set(key.getPath().toString()+"\t"+key.getCandidate().toString());
	}
	public void set(VIntWritable type,Text word){
		this.type=type;
		this.word=word;
	}

	public void set(String template,int type,String word){
		this.type.set(type);
		this.template.set(template);
		this.word.set(word);
	}
	public void set(Text template,VIntWritable type,Text word){
		this.type=type;
		this.template=template;
		this.word=word;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		template.readFields(in);
		type.readFields(in);
		word.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		template.write(out);
		type.write(out);
		word.write(out);
	}

	@Override
	public int compareTo(TemplateTriple sp) {
		int compare=template.compareTo(sp.template);
		if(compare!=0){
			return compare;
		}
		else{
			compare=type.compareTo(sp.type);
			if(compare!=0){
				return compare;
			}
			return word.compareTo(sp.word);
		}
	}
	public Text getTemplate(){
		return this.template;
	}
	public Text getWord(){
		return this.word;
	}
	public VIntWritable getType(){
		return this.type;
	}
	public void setTemplate(Text template){
		this.template=template;
	}
	public void setWord(Text word){
		this.word=word;;
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
		if(o instanceof TemplateTriple){
			TemplateTriple wp=(TemplateTriple)o;
			return type.equals(wp.type)&&word.equals(wp.word)&&template.equals(wp.template);
		}
		return false;
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(template);
		builder.append("\t");
		builder.append(type);
		builder.append("\t");
		builder.append(word);
		return builder.toString();
	}
	public static class RawComparatorPathTypeWord extends WritableComparator{
		private static final Text.Comparator TEXT_COMARATOR=new Text.Comparator();
		public RawComparatorPathTypeWord(){
			super(TemplateTriple.class);
		}

		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
			try {
				int firstStrLength1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
				int firstStrLength2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
				//Compare the path
				int compare=TEXT_COMARATOR.compare(b1, s1, firstStrLength1, b2, s2, firstStrLength2);
				if(compare!=0){
					return compare;
				}
				else{
					long type1=readVLong(b1, s1+firstStrLength1);
					long type2=readVLong(b2, s2+firstStrLength2);
					//Compare the type
					compare = ( type1 < type2 )? -1 : ( type1 == type2 ) ? 0: 1;
					if(compare!=0){
						return compare;
					}
					int typeLength1=WritableUtils.decodeVIntSize(b1[s1+firstStrLength1]);
					int typeLength2=WritableUtils.decodeVIntSize(b2[s2+firstStrLength2]);
					//Compare the word
					return TEXT_COMARATOR.compare(b1,s1+firstStrLength1+typeLength1,l1-firstStrLength1-typeLength1,
							b2,s2+firstStrLength2+typeLength2,l2-firstStrLength2-typeLength2);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return l2;
		}
	}
	public static class RawComparatorTypeWordPath extends WritableComparator{
		private static final Text.Comparator TEXT_COMARATOR=new Text.Comparator();
		public RawComparatorTypeWordPath(){
			super(TemplateTriple.class);
		}

		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
			try {
				int firstStrLength1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
				int firstStrLength2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
				long type1=readVLong(b1, s1+firstStrLength1);
				long type2=readVLong(b2, s2+firstStrLength2);
				//Compare the type
				int compare = ( type1 < type2 )? -1 : ( type1 == type2 ) ? 0: 1;
				if(compare!=0){
					return compare;
				}
				else{
					int typeLength1=WritableUtils.decodeVIntSize(b1[s1+firstStrLength1]);
					int typeLength2=WritableUtils.decodeVIntSize(b2[s2+firstStrLength2]);
					//Compare the word
					compare=TEXT_COMARATOR.compare(b1,s1+firstStrLength1+typeLength1,l1-firstStrLength1-typeLength1,
							b2,s2+firstStrLength2+typeLength2,l2-firstStrLength2-typeLength2);


					if(compare!=0){
						return compare;
					}
					//Compare the path
					return compare=TEXT_COMARATOR.compare(b1, s1, firstStrLength1, b2, s2, firstStrLength2);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return l2;
		}
	}
	public static class RawComparatorPathWordType extends WritableComparator{
		private static final Text.Comparator TEXT_COMARATOR=new Text.Comparator();
		public RawComparatorPathWordType(){
			super(TemplateTriple.class);
		}

		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
			try {
				int firstStrLength1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
				int firstStrLength2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
				//Compare the path
				int compare=TEXT_COMARATOR.compare(b1, s1, firstStrLength1, b2, s2, firstStrLength2);


				if(compare!=0){
					return compare;
				}
				else{
					int typeLength1=WritableUtils.decodeVIntSize(b1[s1+firstStrLength1]);
					int typeLength2=WritableUtils.decodeVIntSize(b2[s2+firstStrLength2]);

					//Compare the word
					compare=TEXT_COMARATOR.compare(b1,s1+firstStrLength1+typeLength1,l1-firstStrLength1-typeLength1,
							b2,s2+firstStrLength2+typeLength2,l2-firstStrLength2-typeLength2);
					if(compare!=0){
						return compare;
					}
					long type1=readVLong(b1, s1+firstStrLength1);
					long type2=readVLong(b2, s2+firstStrLength2);
					//Compare the type
					return ( type1 < type2 )? -1 : ( type1 == type2 ) ? 0: 1;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return l2;
		}
	}
}
