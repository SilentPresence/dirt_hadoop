package local.extraction;

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

public class ExtractionDriver {

	private static final String firstStepUri="s3://dsp152-third/jar/extractionStep1.jar";
	private static final String secondStepUri="s3://dsp152-third/jar/extractionStep2.jar";
	private static final String thirdStepUri="s3://dsp152-third/jar/extractionStep3.jar";
	private static final String fourthStepUri="s3://dsp152-third/jar/extractionStep4.jar";
	private static final String fifthStepUri="s3://dsp152-third/jar/extractionStep5.jar";

	private static final String logUri="s3://dsp152-third/logs/";
	private static final String intermediatePrefixUri="s3://dsp152-third/intermediate";
	private static final String outputPrefixUri="s3://dsp152-third/output/";
	public static void main(String[] args) throws FileNotFoundException, IllegalArgumentException, IOException{

		if(args.length==3){
			File credentialsFile=new File("dsp.properties");
			AWSCredentials credentials=new PropertiesCredentials(credentialsFile);
			AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
			int dPMinCount =Integer.parseInt(args[0]);
			int minFeatureNum =Integer.parseInt(args[1]);
			int numberOfFiles =Integer.parseInt(args[2]);

			String uuid= UUID.randomUUID().toString();

			if(dPMinCount<0){
				System.out.println("Error: DPMinCount has to be positive or zero: "+dPMinCount);
				System.exit(1);
			}
			if(minFeatureNum<0){
				System.out.println("Error: MinFeatureNum has to be positive or zero: "+minFeatureNum);
				System.exit(1);
			}
			if(numberOfFiles<0||numberOfFiles>99){
				System.out.println("The number of files has to be in the range of 1 to 99 "+numberOfFiles);
				System.exit(1);
			}
			JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
			if(numberOfFiles<=10){
				instances.withInstanceCount(3)
				.withMasterInstanceType(InstanceType.M3Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M3Xlarge.toString());
			}
			else if(numberOfFiles>10&&numberOfFiles<=20){
				instances.withInstanceCount(4)
				.withMasterInstanceType(InstanceType.M3Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M3Xlarge.toString());
			}
			else{
				instances.withInstanceCount((int)(Math.ceil(numberOfFiles/10.0)+1))
				.withMasterInstanceType(InstanceType.M3Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M3Xlarge.toString());
			}

			instances.withHadoopVersion("2.4.0").withEc2KeyName("teiresias")
			.withKeepJobFlowAliveWhenNoSteps(false)
			.withPlacement(new PlacementType("us-east-1a"));

			HadoopJarStepConfig firstMapSetup = new HadoopJarStepConfig();
			firstMapSetup.withJar(firstStepUri)
			.withArgs(intermediatePrefixUri+"1/"+uuid,args[0],args[2]);

			HadoopJarStepConfig secondMapSetup = new HadoopJarStepConfig()
			.withJar(secondStepUri)
			.withArgs(intermediatePrefixUri+"1/"+uuid, intermediatePrefixUri+"2/"+uuid,args[1]);

			HadoopJarStepConfig thirdMapSetup = new HadoopJarStepConfig()
			.withJar(thirdStepUri)
			.withArgs(intermediatePrefixUri+"2/"+uuid, intermediatePrefixUri+"3/"+uuid);

			HadoopJarStepConfig fourthMapSetup = new HadoopJarStepConfig()
			.withJar(fourthStepUri)
			.withArgs(intermediatePrefixUri+"3/"+uuid, intermediatePrefixUri+"4/"+uuid);

			HadoopJarStepConfig fifthMapSetup = new HadoopJarStepConfig()
			.withJar(fifthStepUri)
			.withArgs(intermediatePrefixUri+"4/"+uuid, outputPrefixUri+uuid);

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

			StepConfig fifthStepConfig = new StepConfig()
			.withName("Fifth MapReduce")
			.withHadoopJarStep(fifthMapSetup)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

			ScriptBootstrapActionConfig bootstrapScriptConfig = new ScriptBootstrapActionConfig()
			.withPath("s3://elasticmapreduce/bootstrap-actions/install-ganglia");

			BootstrapActionConfig gangliaBootstrap = new BootstrapActionConfig()
			.withName("Ganglia installation")
			.withScriptBootstrapAction(bootstrapScriptConfig);

			RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
			.withName("DP Extraction")
			.withInstances(instances)
			.withServiceRole("EMR_DefaultRole")
			.withJobFlowRole("EMR_EC2_DefaultRole")
			.withSteps(debugging,firstStepConfig,secondStepConfig,thirdStepConfig,fourthStepConfig,fifthStepConfig)
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
