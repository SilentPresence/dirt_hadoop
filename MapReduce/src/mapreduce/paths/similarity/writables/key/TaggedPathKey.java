package mapreduce.paths.similarity.writables.key;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class TaggedPathKey implements WritableComparable<TaggedPathKey> {
	private Text path;
	//a tag that indicates whether the path is from the test-set or from the corpus-set
	private BooleanWritable tag;
	//indicates if the path is the path that entails or the entailed (the right path or the left path in the test-set)
	private BooleanWritable isInversed;

	public TaggedPathKey(){
		set(new Text(),new BooleanWritable(),new BooleanWritable());
	}
	public TaggedPathKey(BooleanWritable tag,BooleanWritable isInversed){
		set(new Text(),tag,isInversed);
	}
	public void set(String path){
		this.path.set(path);
	}
	public void set(Text path,BooleanWritable tag,BooleanWritable isInversed){
		this.path=path;
		this.tag=tag;
		this.isInversed=isInversed;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		path.readFields(in);
		tag.readFields(in);
		isInversed.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		path.write(out);
		tag.write(out);
		isInversed.write(out);
	}

	@Override
	public int compareTo(TaggedPathKey taggedKey) {
		int compare=path.compareTo(taggedKey.path);
		if(compare!=0){
			return compare;
		}
		else{
			compare=tag.compareTo(taggedKey.tag)*-1;
			if(compare!=0){
				return compare;
			}
			else{
				return isInversed.compareTo(taggedKey.isInversed)*-1;
			}
		}
	}
	public Text getPath(){
		return this.path;
	}
	public boolean isTagged(){
		return this.tag.get();
	}
	public boolean isInversed(){
		return this.isInversed.get();
	}
	public void setPath(String path){
		this.path.set(path);
	}
	public int hashCode(){
		int hash=17;
		hash=hash*31+path.hashCode();
		return hash;
	}
	public boolean equals(Object o){
		if(o instanceof TaggedPathKey){
			TaggedPathKey taggedKey=(TaggedPathKey)o;
			return path.equals(taggedKey.path)&&tag.equals(taggedKey.tag)&&isInversed.equals(taggedKey.isInversed);
		}
		return false;
	}
	public String toString(){
		StringBuilder builder=new StringBuilder();
		builder.append(tag);
		builder.append("\t");
		builder.append(isInversed);
		builder.append("\t");
		builder.append(path);
		return builder.toString();
	}
	public BooleanWritable getTag() {
		return tag;
	}
	public void setTag(BooleanWritable tag) {
		this.tag = tag;
	}
	public BooleanWritable getIsInversed() {
		return isInversed;
	}
	public void setIsInversed(BooleanWritable isInversed) {
		this.isInversed = isInversed;
	}
	public void setPath(Text path) {
		this.path = path;
	}
	public static class ComparatorTagPath extends WritableComparator{
		private static final Text.Comparator TEXT_COMARATOR=new Text.Comparator();
		public ComparatorTagPath(){
			super(TaggedPathKey.class);
		}

		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
			//Compare the path
			try {
				int firstStrLength1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
				int firstStrLength2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
				int compare=TEXT_COMARATOR.compare(b1, s1+2, firstStrLength1, b2, s2+2, firstStrLength2);
				if(compare!=0){
					return compare;
				}
				else{
					//Compare the tag
					compare=compareBytes(b1, s1+firstStrLength1, 1, b2, s2+firstStrLength2, 1)*-1;
					if(compare!=0){
						return compare;
					}
					else{
						//Compare the inversion
						return compare=compareBytes(b1, s1+firstStrLength1+1, 1, b2, s2+firstStrLength2+1, 1)*-1;
					}
				}
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			return l2;
		}
	}
}
