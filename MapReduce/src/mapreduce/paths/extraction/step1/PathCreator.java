package mapreduce.paths.extraction.step1;
import java.io.IOException;
import java.util.List;

import mapreduce.paths.extraction.Groups;
import mapreduce.paths.extraction.writables.keys.PathTriple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PathCreator{
	public static class MapperClass extends Mapper<LongWritable, Text, PathTriple, VLongWritable> {
		private Text xWord=new Text();
		private Text yWord=new Text();
		private Text path=new Text();
		private Text orderInverter=new Text(" ");
		private Text invertedPath=new Text();
		//Used to count the total dependency path 
		private PathTriple pathOrderInverter=new PathTriple(Groups.PATH_INVERSER_INVERSER,orderInverter);
		//Used to count the occurrences of a word in slot X
		private PathTriple xPathPair=new PathTriple(Groups.PATH_SLOT_X_WORD,orderInverter);
		//Used to count the occurrences of a word in slot Y
		private PathTriple yPathPair=new PathTriple(Groups.PATH_SLOT_Y_WORD,orderInverter);

		private VLongWritable occurences=new VLongWritable();
		private List<List<String[]>> relationsList=null;
		private final SyntacticNGram nGramParser=new SyntacticNGram();
		private String firstHalf="";
		private String secondHalf="";

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			relationsList=nGramParser.processLine(value.toString());
			if(relationsList!=null){
				occurences.set(nGramParser.getOccurences());
				for(int i=0;i<relationsList.size();i++){
					firstHalf=nGramParser.forwardBuildPathToHead(relationsList.get(i));
					xWord.set(relationsList.get(i).get(0)[0]);
					for(int j=i;j<relationsList.size();j++){
						if( i!=j && !nGramParser.hasSharedPath(relationsList.get(i),relationsList.get(j))){
							secondHalf=nGramParser.forwardBuildPathFromHead(relationsList.get(j));
							yWord.set(relationsList.get(j).get(0)[0]);

							path.set("X"+firstHalf+secondHalf+"Y");
							invertedPath.set("Y"+firstHalf+secondHalf+"X");

							pathOrderInverter.setPath(path);
							//Write (p,*,*)
							context.write(pathOrderInverter, occurences);
							pathOrderInverter.setPath(invertedPath);
							//Write (inverted p,*,*)
							context.write(pathOrderInverter, occurences);

							xPathPair.setWord(xWord);
							yPathPair.setWord(yWord);

							xPathPair.setPath(path);
							//Write (p,slot x,w)
							context.write(xPathPair, occurences);
							xPathPair.setPath(invertedPath);
							//Write (inverted p,slot x,w)
							context.write(xPathPair, occurences);

							yPathPair.setPath(path);
							//Write (p,slot y,w)
							context.write(yPathPair, occurences);
							yPathPair.setPath(invertedPath);
							//Write (inverted p,slot y,w)
							context.write(yPathPair, occurences);
						}
					}
				}
			}
		}
	}

	public static class Combiner extends Reducer<PathTriple,VLongWritable,PathTriple,VLongWritable> {
		private VLongWritable occurence=new VLongWritable();
		public void reduce(PathTriple key,Iterable<VLongWritable> values,Context context) throws IOException, InterruptedException{
			long sum=0;
			for(VLongWritable val:values){
				sum+=val.get();
			}
			occurence.set(sum);
			context.write(key, occurence);
		}
	}
	public static class PathTypeWordComparator extends WritableComparator{
		public PathTypeWordComparator(){
			super(PathTriple.class,true);
		}
		public int compare(WritableComparable wc1,WritableComparable wc2){
			PathTriple pt1=(PathTriple)wc1;
			PathTriple pt2=(PathTriple)wc2;
			int compare=pt1.getPath().compareTo(pt2.getPath());
			if(compare!=0){
				return compare;
			}
			else{
				compare=pt1.getType().compareTo(pt2.getType());
				if(compare!=0){
					return compare;
				}
				else{
					return pt1.getWord().compareTo(pt2.getWord());
				}
			}
		}
	}
	public static class CustomPartitioner extends Partitioner<PathTriple, VLongWritable>{
		@Override
		public int getPartition(PathTriple key, VLongWritable value,int numReduceTasks) {
			int hash=17;
			hash=hash*31+key.getPath().hashCode();
			return (hash&Integer.MAX_VALUE)%numReduceTasks;
		}
	}
	public static class Reduce extends Reducer<PathTriple,VLongWritable,PathTriple,VLongWritable> {
		private long dpMinCount=0;
		private boolean passedDPMinCount=false;
		private VLongWritable occurence=new VLongWritable();

		public void setup(Context context){
			Configuration conf=context.getConfiguration();
			dpMinCount=Long.parseLong(conf.get("DPMinCount"));
		}

		public void reduce(PathTriple key,Iterable<VLongWritable> values,Context context) throws IOException, InterruptedException{
			long sum=0;
			for(VLongWritable val:values){
				sum+=val.get();
			}
			if(key.getType().get()==Groups.PATH_INVERSER_INVERSER){
				if(sum>=dpMinCount){
					passedDPMinCount=true;
				}
				else{
					passedDPMinCount=false;
				}
			}
			else if(passedDPMinCount) {
				occurence.set(sum);
				context.write(key, occurence);
			}
		}
	}
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==3){
			Configuration conf=new Configuration();
			conf.set("DPMinCount", args[1]);
			int fileNum=Integer.parseInt(args[2]);
			Job job=new Job(conf,"Path Creator");
			job.setMapperClass(mapreduce.paths.extraction.step1.PathCreator.MapperClass.class);
			job.setJarByClass(mapreduce.paths.extraction.step1.PathCreator.class);
			job.setReducerClass(mapreduce.paths.extraction.step1.PathCreator.Reduce.class);
			job.setCombinerClass(mapreduce.paths.extraction.step1.PathCreator.Combiner.class);
			
			//job.setSortComparatorClass(PathTriple.RawComparatorPathTypeWord.class);
			job.setSortComparatorClass(mapreduce.paths.extraction.step1.PathCreator.PathTypeWordComparator.class);

			job.setPartitionerClass(mapreduce.paths.extraction.step1.PathCreator.CustomPartitioner.class);
			
			job.setMapOutputKeyClass(PathTriple.class);
			job.setMapOutputValueClass(VLongWritable.class);

			job.setOutputKeyClass(PathTriple.class);
			job.setOutputValueClass(VLongWritable.class);

			job.setInputFormatClass(SequenceFileInputFormat.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
			for(int i=0;i<fileNum&&i<10;i++){
				FileInputFormat.addInputPath(job, new Path("s3n://dsp152/syntactic-ngram/biarcs/biarcs.0"+i+"-of-99"));
			}
			for(int i=10;i<fileNum;i++){
				FileInputFormat.addInputPath(job, new Path("s3n://dsp152/syntactic-ngram/biarcs/biarcs."+i+"-of-99"));
			}
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			System.exit(job.waitForCompletion(true) ? 0 : 1) ;
		}
		else{
			System.out.println("Not enough or too much arguments provided");
			System.exit(1);
		}
	}
}