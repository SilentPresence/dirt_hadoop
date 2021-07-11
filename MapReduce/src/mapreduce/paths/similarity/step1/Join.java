package mapreduce.paths.similarity.step1;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mapreduce.paths.similarity.PathNumber;
import mapreduce.paths.similarity.writables.key.TaggedPathKey;
import mapreduce.paths.similarity.writables.key.TemplateKey;
import mapreduce.paths.similarity.writables.values.CompositeValue;
import mapreduce.paths.similarity.writables.values.PathCompositeValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Join{
	public static class CorpusMapper extends Mapper<LongWritable, Text, TaggedPathKey, PathCompositeValue> {
		private final  BooleanWritable falseBoolean=new BooleanWritable(false);
		private TaggedPathKey taggedPath=new TaggedPathKey(falseBoolean,falseBoolean);
		private PathCompositeValue compositeValue=new PathCompositeValue();
		private  String[] line=null;
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			line=value.toString().split("\t");
			compositeValue.setFromCorpus(line);
			taggedPath.setPath(line[0].trim());
			context.write(taggedPath, compositeValue);
		}
	}
	public static class TestSetMapper extends Mapper<LongWritable, Text, TaggedPathKey, PathCompositeValue> {
		private final  BooleanWritable trueBoolean= new BooleanWritable(true);
		private final  BooleanWritable falseBoolean=new BooleanWritable(false);
		private TaggedPathKey taggedPath=new TaggedPathKey(trueBoolean,falseBoolean);
		private TaggedPathKey taggedPathInversed=new TaggedPathKey(trueBoolean,trueBoolean);
		private PathCompositeValue compositeValue=new PathCompositeValue();
		private  String[] line=null;
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			line=value.toString().split("\t");
			taggedPath.set(line[0].trim());
			taggedPathInversed.set(line[1].trim());
			compositeValue.setPath(line[1].trim());
			context.write(taggedPath, compositeValue);
			compositeValue.setPath(line[0].trim());
			context.write(taggedPathInversed, compositeValue);
		}
	}
	public static class Reduce extends Reducer<TaggedPathKey,PathCompositeValue,TemplateKey,CompositeValue> {
		// P -> C
		private Set<String> entailedPaths=new HashSet<String>();
		// C -> P
		private Set<String> entailPaths=new HashSet<String>();
		private String currentKey=new String();
		private CompositeValue data=new CompositeValue();
		// P -> C
		private TemplateKey entailedKey=new TemplateKey();
		// C -> P
		private TemplateKey entailKey=new TemplateKey();

		public void reduce(TaggedPathKey key,Iterable<PathCompositeValue> values,Context context) throws IOException, InterruptedException{
			if(!currentKey.equals(key.getPath().toString())){
				entailedPaths.clear();
				entailPaths.clear();
			}
			if(key.getTag().get()==true){
				if(key.getIsInversed().get()==true){
					for(PathCompositeValue value:values){
						entailPaths.add(value.getPath().toString());
					}
				}
				else{
					for(PathCompositeValue value:values){
						entailedPaths.add(value.getPath().toString());
					}
				}
			}
			else{
				entailedKey.setPath(key.getPath());
				entailKey.setCandidate(key.getPath());
				for(PathCompositeValue value:values){
					data.set(value);
					data.setPathNumber(PathNumber.leftPath);
					for(String entailed:entailedPaths){
						entailedKey.setCandidate(entailed);
						context.write(entailedKey, data);
					}
					data.setPathNumber(PathNumber.rightPath);
					for(String entail:entailPaths){
						entailKey.setPath(entail);
						context.write(entailKey, data);
					}
				}

			}
			currentKey=key.getPath().toString();
		}
	}
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==3){
			Configuration conf=new Configuration();

			Job job=new Job(conf,"Path Creator");
			job.setJarByClass(mapreduce.paths.similarity.step1.Join.class);

			job.setReducerClass(mapreduce.paths.similarity.step1.Join.Reduce.class);

			//job.setSortComparatorClass(TaggedPathKey.ComparatorTagPath.class);

			job.setMapOutputKeyClass(TaggedPathKey.class);
			job.setMapOutputValueClass(PathCompositeValue.class);

			job.setOutputKeyClass(TemplateKey.class);
			job.setOutputValueClass(CompositeValue.class);

			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, mapreduce.paths.similarity.step1.Join.CorpusMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, mapreduce.paths.similarity.step1.Join.TestSetMapper.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1) ;
		}
		else{
			System.out.println("Not enough or too much arguments provided");
			System.exit(1);
		}
	}
}