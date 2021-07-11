package local.similarity;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class SimilarityDriver {

	private static final String firstStepUri="s3://dsp152-third/jar/similarityStep1.jar";
	private static final String secondStepUri="s3://dsp152-third/jar/similarityStep2.jar";
	private static final String thirdStepUri="s3://dsp152-third/jar/similarityStep3.jar";
	private static final String fourthStepUri="s3://dsp152-third/jar/similarityStep4.jar";
	
	private static final String testUri="s3://dsp152-third/test/";

	
	private static final String input20Uri="s3://dsp152-third/debugoutput/";
	private static final String input100Uri="s3://dsp152-third/output/1c5d1673-b691-48bb-9922-4281852070e3/";

	private static final String logUri="s3://dsp152-third/logs/";
	private static final String intermediatePrefixUri="s3://dsp152-third/similarityIntermediate";
	private static final String outputPrefixUri="s3://dsp152-third/similarityOutput/";
	public static void main(String[] args) throws FileNotFoundException, IllegalArgumentException, IOException{

		if(args.length==1){
			File credentialsFile=new File("dsp.properties");
			AWSCredentials credentials=new PropertiesCredentials(credentialsFile);
			AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
			int inputNum =Integer.parseInt(args[0]);
			String input ="";
			JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
			if(inputNum==20){
				input=input20Uri;
				instances.withInstanceCount(2)
				.withMasterInstanceType(InstanceType.M1Medium.toString())
				.withSlaveInstanceType(InstanceType.M1Medium.toString());
			}
			else if(inputNum==100){
				input=input100Uri;
				instances.withInstanceCount(5)
				.withMasterInstanceType(InstanceType.M1Medium.toString())
				.withSlaveInstanceType(InstanceType.M1Medium.toString());
			}
			else{
				System.out.println("Unsuppoerted input number");
			}
			
			String uuid= UUID.randomUUID().toString();

			instances.withHadoopVersion("2.4.0").withEc2KeyName("teiresias")
			.withKeepJobFlowAliveWhenNoSteps(false)
			.withPlacement(new PlacementType("us-east-1a"));

			HadoopJarStepConfig firstMapSetup = new HadoopJarStepConfig();
			firstMapSetup.withJar(firstStepUri)
			.withArgs(input,testUri,intermediatePrefixUri+"1/"+uuid);

			HadoopJarStepConfig secondMapSetup = new HadoopJarStepConfig()
			.withJar(secondStepUri)
			.withArgs(intermediatePrefixUri+"1/"+uuid, intermediatePrefixUri+"2/"+uuid);

			HadoopJarStepConfig thirdMapSetup = new HadoopJarStepConfig()
			.withJar(thirdStepUri)
			.withArgs(intermediatePrefixUri+"2/"+uuid, intermediatePrefixUri+"3/"+uuid);

			HadoopJarStepConfig fourthMapSetup = new HadoopJarStepConfig()
			.withJar(fourthStepUri)
			.withArgs(intermediatePrefixUri+"3/"+uuid, outputPrefixUri+uuid);

			StepFactory stepFactory=new StepFactory();

			StepConfig debugging  = new StepConfig()
			.withName("Enable debugging")
			.withHadoopJarStep(stepFactory.newEnableDebuggingStep())
			.withActionOnFailure("TERMINATE_JOB_FLOW");

			StepConfig firstStepConfig = new StepConfig()
			.withName("First MapReduce")
			.withHadoopJarStep(firstMapSetup)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

			StepConfig secondStepConfig = new StepConfig()
			.withName("Second MapReduce")
			.withHadoopJarStep(secondMapSetup)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

			StepConfig thirdStepConfig = new StepConfig()
			.withName("Third MapReduce")
			.withHadoopJarStep(thirdMapSetup)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

			StepConfig fourthStepConfig = new StepConfig()
			.withName("Fourth MapReduce")
			.withHadoopJarStep(fourthMapSetup)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

			ScriptBootstrapActionConfig bootstrapScriptConfig = new ScriptBootstrapActionConfig()
			.withPath("s3://elasticmapreduce/bootstrap-actions/install-ganglia");

			BootstrapActionConfig gangliaBootstrap = new BootstrapActionConfig()
			.withName("Ganglia installation")
			.withScriptBootstrapAction(bootstrapScriptConfig);

			RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
			.withName("Similarity Calculation")
			.withInstances(instances)
			.withServiceRole("EMR_DefaultRole")
			.withJobFlowRole("EMR_EC2_DefaultRole")
			.withSteps(debugging,firstStepConfig,secondStepConfig,thirdStepConfig,fourthStepConfig)
			.withAmiVersion("latest")
			.withBootstrapActions(gangliaBootstrap)
			.withLogUri(logUri);

			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);

		}
		else{
			System.out.println("Too much or not enough arguments");
			System.out.println("Usage: java -jar extractPaths.jar DPMinCount MinFeatureNum NumberOfFiles");
		}

	}	
}
