package mapreduce.paths.extraction.writables.keys;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class PathTriple implements WritableComparable<PathTriple> {
	private Text path;
	private VIntWritable type;
	private Text word;

	public PathTriple(){
		set(new Text(),new VIntWritable(),new Text());
	}
	public PathTriple(int slotType){
		set(new Text(),new VIntWritable(slotType),new Text());
	}
	public PathTriple(int slotType,String word){
		set(new Text(),new VIntWritable(slotType),new Text(word));
	}
	public PathTriple(String path,int type,String word){
		set(new Text(path),new VIntWritable(type),new Text(word));
	}
	public PathTriple(Text path,int type,Text word){
		set(path,new VIntWritable(type),word);
	}
	public PathTriple(int slotType, Text word) {
		set(new Text(),new VIntWritable(slotType),word);
	}
	public PathTriple(Text path, int slotType) {
		set(path,new VIntWritable(slotType),new Text());
	}
	public void set(String path,int type,String word){
		this.type.set(type);
		this.path.set(path);
		this.word.set(word);
	}
	public void set(Text path,VIntWritable type,Text word){
		this.type=type;
		this.path=path;
		this.word=word;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		path.readFields(in);
		type.readFields(in);
		word.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		path.write(out);
		type.write(out);
		word.write(out);
	}

	@Override
	public int compareTo(PathTriple sp) {
		int compare=path.compareTo(sp.path);
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
	public Text getPath(){
		return this.path;
	}
	public Text getWord(){
		return this.word;
	}
	public VIntWritable getType(){
		return this.type;
	}
	public void setPath(Text path){
		this.path=path;
	}
	public void setWord(Text word){
		this.word=word;;
	}
	public void setSlotType(VIntWritable type){
		this.type=type;
	}
	public int hashCode(){
		int hash=17;
		hash=hash*31+path.hashCode();
		return hash;
	}
	public boolean equals(Object o){
		if(o instanceof PathTriple){
			PathTriple wp=(PathTriple)o;
			return type.equals(wp.type)&&word.equals(wp.word)&&path.equals(wp.path);
		}
		return false;
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(path);
		builder.append("\t");
		builder.append(type);
		builder.append("\t");
		builder.append(word);
		return builder.toString();
	}
	public static class RawComparatorPathTypeWord extends WritableComparator{
		private static final Text.Comparator TEXT_COMARATOR=new Text.Comparator();
		public RawComparatorPathTypeWord(){
			super(PathTriple.class);
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
			super(PathTriple.class);
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
			super(PathTriple.class);
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
