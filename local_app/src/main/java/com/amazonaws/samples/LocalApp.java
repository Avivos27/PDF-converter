package com.amazonaws.samples;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class LocalApp {
    public static void main(String[] args) throws Exception {
    	boolean terminate = false;
    	String inputPath="";
		String outPath="";
		int n=1;
    	if(args.length>2){
    		inputPath = args[0];
    		outPath = args[1];
    		n = Integer.parseInt(args[2]);
    		
    		if(args.length==4){
    			if(args[3].equals("terminate")){
    				terminate = true;
    			}
    		}
    	}
    	else{
    		System.out.println("please insert the right number of arguments");
    		System.exit(0);
    	}
    	
    	File input = new File(inputPath);
    	//SHOULD GET ARGS BY SYSTEM.in
    	
    	String key = "input" + UUID.randomUUID();
    	String inputBucketName = "input-files-s3";
    	String outputBucketName = "s3-manager-response";
    	String responseQueueName = "responseQueue-" + UUID.randomUUID();
    	String responseQueueUrl = "";
    	String manager_queue_url = "";
        String manager_id = "";
        boolean manager_is_actice = false;
         
        AmazonSQS sqs = null;
        AmazonS3 s3 = null;
        AmazonEC2 ec2 = null;
        AWSCredentials credentials = null;
        DescribeInstancesRequest request = null;
        DescribeInstancesResult result = null;
        List<Reservation> reservations = null;
        
        /*
         * 		Authenticate credentials located at default folder
         */
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\username\\.aws\\credentials), and is in valid format.",e);
        }
        
        // Initialize response queue 
        sqs = new AmazonSQSClient(credentials);
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(responseQueueName);
        responseQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        
        // Initialize s3 ec2 objects
        s3 = new AmazonS3Client(credentials);
        ec2 = new AmazonEC2Client(credentials);
        
        uploadToS3(s3, inputBucketName, key, input);

        /*
         * Iterate through all ec2 active instances and determine if manager is active
         */
        request = new DescribeInstancesRequest();
        result = ec2.describeInstances(request);
        reservations = result.getReservations();
        for (Reservation reservation : reservations) {
    		for (Instance instance : reservation.getInstances()) { 
    			 if(instance.getPublicIpAddress()!= null){ 										// look only in only active ec2s
    				 if(instance.getTags() != null && !instance.getTags().isEmpty()){			// have tags
    					for(int i=0; i<instance.getTags().size();i++){							// fetch information from tags
    						if("type".equals(instance.getTags().get(i).getKey()) && "manager".equals(instance.getTags().get(i).getValue())){
    							System.out.println("Manager is already active");
    							manager_is_actice = true;
    							for(int j=0; j<instance.getTags().size();j++){ 				 
    								if("localApp".equals(instance.getTags().get(j).getKey())){
    									manager_queue_url = instance.getTags().get(j).getValue(); 
    									System.out.println("Found localApp queue url : " + manager_queue_url);
    									break;
    								}
    								
    							}
    						}
    					}
    				 }
    			 }
    		}
        }
        
        
        
        if(!manager_is_actice){  
        	// Manager is not active, launching a manager ec2 instance
	   	     try {
	   	    	 //launch an instance
	   	         RunInstancesRequest instanceRequest = new RunInstancesRequest("ami-c58c1dd3", 1, 1);  
	   	         instanceRequest.setUserData(getManagerScript(s3));
	   	         instanceRequest.setInstanceType(InstanceType.T2Micro.toString());
	   	         List<Instance> instances = ec2.runInstances(instanceRequest).getReservation().getInstances();           
	   	         System.out.println("Launch a manager instance: " + instances);
	   	         
	   	         //tag as manager
	   	         manager_id = instances.get(0).getInstanceId();
	   	         CreateTagsRequest tag_request = new CreateTagsRequest();
	   	         tag_request = tag_request.withResources(manager_id).withTags(new Tag("type", "manager"));
	   	         ec2.createTags(tag_request);
	   	         
	   	         // iterate through ec2 instances, find manager and get manager_queue_url from tag
	   	         System.out.println("Waiting for manager...");
	   	          boolean manager_tag_ready = false;
				  while(!manager_tag_ready){
			   	      request = new DescribeInstancesRequest();
			          result = ec2.describeInstances(request);
			          reservations = result.getReservations();
			          for (Reservation reservation : reservations) {
			      		for (Instance instance : reservation.getInstances()) {
			      			 if(instance.getPublicIpAddress()!= null){
			      				if(instance.getTags() != null && !instance.getTags().isEmpty()){
			      					for(int i=0; i<instance.getTags().size();i++){
			      						if("type".equals(instance.getTags().get(i).getKey()) && "manager".equals(instance.getTags().get(i).getValue())){
			      							for(int j=0; j<instance.getTags().size();j++){ 				 
			    								if("localApp".equals(instance.getTags().get(j).getKey())){
			    									manager_queue_url = instance.getTags().get(j).getValue(); 
			    									System.out.println("Found localApp queue url : " + manager_queue_url);
			    									manager_tag_ready = true;
			    									break;
			    								}
			    							}
			      						}
			      					}
			      				}
			      			 }
			      		}
			          }
				  }

 
	   	     } catch (AmazonServiceException ase) {
	   	         System.out.println("Caught Exception: " + ase.getMessage());
	   	         System.out.println("Reponse Status Code: " + ase.getStatusCode());
	   	         System.out.println("Error Code: " + ase.getErrorCode());
	   	         System.out.println("Request ID: " + ase.getRequestId());
	   	     }
        }
        
        // send a new task message to manager delimited by whitespace
        // message format is : "MessageType SqsUrlToRespond BucketName FileKey" 
        System.out.println("Sending a new task message to MyQueue.\n");
        String message = createMessage("new_task", responseQueueUrl, inputBucketName , key , n, terminate);
        sqs.sendMessage(new SendMessageRequest(manager_queue_url, message));
        waitForResponse(sqs, responseQueueUrl,s3, outPath);

        /* 
         * Terminating Local application
         */
        try {
        	
	        System.out.println("Deleting input object from bucket");
	        s3.deleteObject(inputBucketName, key);
	        
	        System.out.println("Deleting input object from bucket");
	        s3.deleteObject(outputBucketName, key);
	        
	        System.out.println("Deleting the local app response queue.\n");
            sqs.deleteQueue(new DeleteQueueRequest(responseQueueUrl));
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    
	private static String createMessage(String type, String responseQueueUrl, String bucketName, String key, int n, boolean terminate) {
		if(terminate)
			return type + " " + responseQueueUrl + " " + bucketName + " " + key + " " + n + " terminate";
		else
			return type + " " + responseQueueUrl + " " + bucketName + " " + key + " " + n;
	}
	
	// set and return (string base 64) the manager script to run at startup of the manager node, script will download manager jar and run it
	private static String getManagerScript(AmazonS3 s3) {
		ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");
        lines.add("wget https://s3.amazonaws.com/data-s3-302550009/man.jar");
        lines.add("java -jar man.jar");
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
	}
	
	// helper function to build manager script
	private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

	// local application will loop until getting a response message from manager that the task is done
	private static void waitForResponse(AmazonSQS sqs, String queueUrl,AmazonS3 s3, String outPath) throws IOException {
		System.out.println("Waiting for respoense...");
		boolean flag = true;
		while(flag){
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
	        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	        for (Message message : messages) {
	        	System.out.println("Local App got Message:");
	        	System.out.println("       "+ message.getBody());
	        	String[] messageArr = message.getBody().split(Pattern.quote(" "));
	        	
	        	if(messageArr[0].equals("ERROR")){
	        		System.out.println("");
	        	}
	        	
	        	
	        	String bucketName = messageArr[0];
	        	String key = messageArr[1];
	        	S3Object object = s3.getObject(bucketName, key);
	        	InputStream reader = new BufferedInputStream(object.getObjectContent());
    			File file = new File(outPath);      
    			OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
    			int read = -1;
    			while ( ( read = reader.read() ) != -1 ) {
    			    writer.write(read);
    			}
    			writer.flush();
    			writer.close();
    			reader.close();
    	
	        	
	            // Delete the message
	            String messageReceiptHandle = message.getReceiptHandle();
	            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
	            flag = false;
	            break;
	            
	        }
		}
		
	}

	
	private static void uploadToS3(AmazonS3 s3, String bucketName,String key,File input){
	    try {
	        //System.out.println("Creating bucket " + bucketName);
	        //s3.createBucket(bucketName);
	        System.out.println("Uploading input file: " + key + " to S3");
	        s3.putObject(new PutObjectRequest(bucketName, key, input));
	        System.out.println("Uploading successful"); 

	    }
	    catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it "
	                + "to Amazon S3, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    } catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered "
	                + "a serious internal problem while trying to communicate with S3, "
	                + "such as not being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	    }
	}

}



	