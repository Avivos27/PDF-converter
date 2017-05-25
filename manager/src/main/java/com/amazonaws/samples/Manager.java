package com.amazonaws.samples;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.IOUtils;


public class Manager{
	
	private static String manager_id = "";
	private static String localAppQueueUrl = "";
	private static String workersQueueUrl = "";
	private static String workersResponseQueueUrl = "";
	
	private static int activeWorkers = 0;
	private static int sentJobsCounter = 0;
	private static int recievedJobsCounter = 0;
	private static int threadsPerWorker = 8;
	private static boolean terminate = false;
	
	private static AmazonSQS sqs = null;
	private static AmazonEC2 ec2 = null;
	private static AmazonS3 s3 = null;
	private static BasicAWSCredentials credentials = null;
	private static ArrayList<String> workersEC2Ids = null;
	private static Map<String, MessageHolder> tasksMap = null;
	
	
	
	
	public static void main(String[] args) throws IOException{
		
		//hard-coded credentials

        try {
            credentials = new BasicAWSCredentials("your-access-key", "your-secret-key");
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\username\\.aws\\credentials), and is in valid format.",
                    e);
        }
		
        ec2 = new AmazonEC2Client(credentials);
        s3 = new AmazonS3Client(credentials);
        sqs = new AmazonSQSClient(credentials);
		
		// suppose to work from within an ec2 instance and provide the credentials
//		ec2  = AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(new InstanceProfileCredentialsProvider(false)).build();
//		s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(new InstanceProfileCredentialsProvider(false)).build();
//		sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(new InstanceProfileCredentialsProvider(false)).build();
        
		workersEC2Ids = new ArrayList<String>();
		tasksMap = new HashMap<String, MessageHolder>();
		
		createSqsQueues();
		listen(); 
		terminateManager();
	}
	
	// Creates a queue for local application and add the url as a tag
	private static void createSqsQueues(){
        try {
            // Create a queue for local applications 
        	// Manager is listening on this queue for incoming tasks from local applications
            System.out.println("Creating a new SQS queue called localApp.");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("localApp");
            localAppQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();    
            
            // Create a queue for workers 
        	// Manager is sending job messages to workers from this queue 
            System.out.println("Creating a new SQS queue called workers.");
            createQueueRequest = new CreateQueueRequest("workers");
            workersQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            // Create a queue for workers responses 
        	// Manager is listening for job completion messages from workers on this queue 
            System.out.println("Creating a new SQS queue called workers_response.");
            createQueueRequest = new CreateQueueRequest("workers_response");
            workersResponseQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            //add all queues urls as a tags to the manager ec2 node, to make visible to all
            addUrlTags();

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
	
	// Adding the localApp url as a tag to the manager ec2 node
	private static void addUrlTags() {
		System.out.println("Adding queue URLs as tags");
        // Itereate on all ec2 instances, find manager and add queue urls as tag
        boolean found = false;
        while(!found){
        	DescribeInstancesRequest request = new DescribeInstancesRequest();
            DescribeInstancesResult result = ec2.describeInstances(request);
            List<Reservation> reservations = result.getReservations();
	        for (Reservation reservation : reservations) {
	    		for (Instance instance : reservation.getInstances()) {
	    			 if(instance.getPublicIpAddress()!= null)
	    				 if(instance.getTags() != null && !instance.getTags().isEmpty()){  				//look for ec2 node with tags
	    					 for(int i=0; i<instance.getTags().size();i++){
	    						 if("type".equals(instance.getTags().get(i).getKey()) && "manager".equals(instance.getTags().get(i).getValue())){ 	// look for manager
				    					 manager_id = instance.getInstanceId();
				    					 CreateTagsRequest tag_request = new CreateTagsRequest();
				    		   	         tag_request = tag_request.withResources(manager_id).withTags(new Tag("localApp", localAppQueueUrl));
				    		   	         ec2.createTags(tag_request);
				    		   	         tag_request = tag_request.withResources(manager_id).withTags(new Tag("workersQueue", workersQueueUrl));
				    		   	         ec2.createTags(tag_request);
				    		   	         tag_request = tag_request.withResources(manager_id).withTags(new Tag("workersResponseQueue", workersResponseQueueUrl));
				    		   	         ec2.createTags(tag_request);
				    		   	         found = true;
				    		   	         System.out.println("Tagging URLs done");
				    		   	         break;
	    						 }
	    					 }
	    				 }
	    		}
	        }
        }	
	}

	// Endlessly looping in this function (or until a terminate message)
	// Listening to all sqs queues and handling incoming\outgoing tasks
	public static void listen() throws IOException{
		System.out.println("Starts listening for incoming messages..");
		while(!terminate){
			// Receive messages
			 fetchFromQueue(localAppQueueUrl);
			 fetchFromQueue(workersResponseQueueUrl);
	
	     }
	}
	

	//check localApp queue for new messages and handles new tasks
	private static void fetchFromQueue(String url) throws IOException {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(url);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
       	 	System.out.println("Manager got message on " + url + " queue:");
            System.out.println("    Body:          " + message.getBody());
            
            // Check for termination message
            if(message.getBody().equals("Admin-Terminate")){
          	 	terminate = true;
            }
            
            // Message is not a termination message
            // localApp queue contains only new task messages: split by whitespace and handle task.
            String[] messageArr = message.getBody().split(Pattern.quote(" "));
            handleMessage(messageArr);
            

            // Delete a message
            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(url, messageReceiptHandle));

        }
	}

	// handles new_task and done_pdf_task incoming message types
	private static void handleMessage(String[] messageArr) throws IOException {
		String messageType = "";
		if(messageArr == null || messageArr.length == 0)
			return;
		
		messageType = messageArr[0];
		if("new_task".equals(messageType))
			handleNewTaskMessage(messageArr);
		
		
		if("done_pdf_task".equals(messageType)){
			handleDonePDFTaskMessage(messageArr);
		}

	}

	private static void handleDonePDFTaskMessage(String[] messageArr) throws IOException {
//		System.out.println("INSIDE HANDLE_DONE_PDF");
		String pdfUrlSource = messageArr[1];
		String operation = messageArr[2];
		String resultUrl = messageArr[3];
		String key = messageArr[4];
		String s3Key = messageArr[5];

		MessageHolder holder = tasksMap.get(key);
		boolean notDuplicate;
		if(resultUrl.equals("error")){
//			System.out.println("GOT AN ERROR MESSAGE");
			String errorDesc = messageArr[6];
			for(int i=7;i<messageArr.length;i++)
				errorDesc += " " +messageArr[i];
//			System.out.println("errorDesc: "+errorDesc);
//			System.out.println("CHECKING DUPLICATION BY KEY   " +operation+"_"+pdfUrlSource);
			notDuplicate = holder.add(operation+"_"+pdfUrlSource, operation + " " + pdfUrlSource + " ERROR: " + errorDesc);
		}
		else
			notDuplicate = holder.add(operation+"_"+pdfUrlSource, operation + " " + pdfUrlSource + " "  + resultUrl);
		
		
		if(notDuplicate){
			recievedJobsCounter++;
			System.out.println("recievedJobsCounter : " + recievedJobsCounter +" out of " + sentJobsCounter);
		}
		else{
			System.out.println("WARNING: caught duplication: key:  "+operation+"_"+pdfUrlSource);
			if(!s3Key.equals("")){
				System.out.println("removing file due to duplication " + s3Key);
				s3.deleteObject("workers-uploads", s3Key);
			}
		}
		
		if(holder.isFinished()){
			sendFinishedTask(holder, key);
		}
		
	}

	private static void sendFinishedTask(MessageHolder holder, String key) throws IOException {
		//create HTML file from message holder uploads it and send message to localapp
		ArrayList<String> responseList = holder.getUrlList();
		File htmlFile = new File("summary_" + key + ".html");
		BufferedWriter bw = new BufferedWriter(new FileWriter(htmlFile));
		bw.write("<html><head><title>New Page</title></head><body><p>");
		for(int i=0; i<responseList.size(); i++){
			bw.write(responseList.get(i) + "<br>");
		}
		bw.write("</p></body></html>");
		bw.close();
		
		
		//create key and upload HTML file
		System.out.println("Uploading html summary file: " + key + " to S3 bucket: s3-manager-response");
        s3.putObject(new PutObjectRequest("s3-manager-response", key, htmlFile));
        System.out.println("Uploading successful");
        sqs.sendMessage(new SendMessageRequest(holder.getResponseUrl(), "s3-manager-response " + key));
        
        htmlFile.delete();
        
	}

	private static void handleNewTaskMessage(String[] messageArr) throws IOException {
		
		String responseQueueUrl = messageArr[1];
		String bucketName = messageArr[2];
		String key = messageArr[3];
		int n = Integer.parseInt(messageArr[4]);
		int workersNeddedForNewTask = 0;
		if(messageArr.length > 5 && messageArr[5].equals("terminate")){
			terminate = true;
			System.out.println("terminate is on");
		}
		else
			System.out.println("terminate is off");
		
		//download localApplication input file from s3
        System.out.println("Downloading a new task input object");
        S3Object s3Object;
        try{
		s3Object = s3.getObject(new GetObjectRequest(bucketName, key));
        }
        catch (AmazonS3Exception e_s3){
        	System.out.println("ERROR: caught exception could not find new task input file");
        	try{
        	sqs.sendMessage(new SendMessageRequest(responseQueueUrl, "ERROR could not download new_Task input file from s3"));
        	return;
        	} catch (AmazonSQSException e_sqs){
        		System.out.println("ERROR: could not resolve sqs to send error message, giving up on message");
        		return;
        	}
        }
        byte[] byteArray = IOUtils.toByteArray(s3Object.getObjectContent());
        String msg_str = new String(byteArray);
        String[] lines = msg_str.split("\\r?\\n");
        workersNeddedForNewTask = (int) (Math.ceil(((lines.length/n)/threadsPerWorker)));
        if(workersNeddedForNewTask == 0)
        	workersNeddedForNewTask = 1;
        System.out.println("workers needed for new task " + workersNeddedForNewTask);
        
        //remove file from s3
        System.out.println("Deleting new_task input file\n");
        s3.deleteObject(bucketName, key);
        System.out.println("Delete successful");
        
        MessageHolder holder = new MessageHolder(lines.length, responseQueueUrl);
        //add the current task to taskMap with new MessageHolder
        tasksMap.put(key, holder);
        
        //send each line of input file as new_pdf_task to workers queue
        int messageCounter = 0;
        for(int i=0;i<lines.length;i++){
        	String message_to_send = createMessageFromLine(lines[i], key);
        	if(message_to_send != null){ 
        		String[] parsedLine = message_to_send.split(Pattern.quote(" "));
        		String operation = parsedLine[1];
        		String sourceUrl = parsedLine[2];

            	if(holder.addSourceUrl(operation+"_"+sourceUrl)){ //return true only if message is not duplicated
	        		sqs.sendMessage(new SendMessageRequest(workersQueueUrl, message_to_send));
	            	sentJobsCounter++;
	            	messageCounter++;
            	}
        	}
        }
//        System.out.println("======== HASH MAP KEYS: ============");
//        Set<String> set = holder.sourceUrlsHashMap.keySet();
//        Iterator<String> it = set.iterator();
//        while(it.hasNext()){
//            System.out.println(it.next());
//         }
//        System.out.println("======== HASH MAP KEYS: ============");
        
        //check to see if failed to create some messages for some reason 
        System.out.println("Sent " + messageCounter + " new_pdf_task messages to workers queue");
        if(messageCounter < lines.length){
        	String messagesFails = Integer.toString(lines.length - messageCounter);
        	System.out.println("WARNING: failed to sent " + messagesFails + " new_pdf_tasks due to duplicates");
        }
        
        // determine how many new workers are needed
        int workersToCreate = 0;
        if(workersNeddedForNewTask > activeWorkers)
        	workersToCreate = workersNeddedForNewTask - activeWorkers;
        System.out.println("Launching " + workersToCreate + " new workers");
        //launch the amount of needed new workers 
        for(int i=0;i<workersToCreate;i++){
        	if(launchNewWorker())
        		activeWorkers++;
        }
        
	}

	private static boolean launchNewWorker() {
		 try {
   	    	 //launch a new worker ec2 instance
   	        // RunInstancesRequest instanceRequest = new RunInstancesRequest("ami-51792c38", 1, 1);  
			 RunInstancesRequest instanceRequest = new RunInstancesRequest("ami-c58c1dd3", 1, 1);  
   	         instanceRequest.setUserData(getWorkerScript());
   	         instanceRequest.setInstanceType(InstanceType.T2Micro.toString());
   	         List<Instance> instances = ec2.runInstances(instanceRequest).getReservation().getInstances();           
   	         System.out.println("Launch a worker instance: " + instances);
   	         
   	         //tag as worker
   	         String worker_id = instances.get(0).getInstanceId();
   	         CreateTagsRequest tag_request = new CreateTagsRequest();
   	         tag_request = tag_request.withResources(worker_id).withTags(new Tag("worker", Integer.toString(activeWorkers+1)));
   	         ec2.createTags(tag_request);
   	         
   	         //add id to for termination purposes later
   	         workersEC2Ids.add(worker_id);
   	         return true;
   	         

   	     } catch (AmazonServiceException ase) {
   	         System.out.println("Caught Exception: " + ase.getMessage());
   	         System.out.println("Reponse Status Code: " + ase.getStatusCode());
   	         System.out.println("Error Code: " + ase.getErrorCode());
   	         System.out.println("Request ID: " + ase.getRequestId());
   	         return false;
   	     }
		
	}

	private static String getWorkerScript() {
		ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");
        lines.add("wget https://s3.amazonaws.com/data-s3-302550009/wor.jar");
        lines.add("java -jar wor.jar");
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
	}
	
	// helper function to build scripts
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

	//builds a new_pdf_task message from input line
	private static String createMessageFromLine(String messageLine,String key) {
		String[] parsedLine = messageLine.split(Pattern.quote("\t"));
		if (parsedLine.length != 2){
			System.out.println("ERROR: Wrong line format");
			return null;
		}
			
		String operation = parsedLine[0];
		String pdfURL = parsedLine[1];
		return "new_pdf_task " + operation + " " + pdfURL + " " + key;		
	}

	public static void terminateManager() throws IOException{
		System.out.println("Handling remaining messages");
		while(recievedJobsCounter < sentJobsCounter){
			fetchFromQueue(workersResponseQueueUrl);
		}
		
		
		System.out.println("Terminatin Manager:");
		
        
        
        System.out.println("	Shutting down ec2 instances");
        workersEC2Ids.add(manager_id);
        TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest();
        terminateRequest.setInstanceIds(workersEC2Ids);
        try{
        	ec2.terminateInstances(terminateRequest);
        } catch (AmazonEC2Exception ase) {
            System.out.println("ERROR CLOSE MANUALLY EC2 MADAFACKA");
        }
        
        System.out.println("	Deleting the queues");
        sqs.deleteQueue(new DeleteQueueRequest(localAppQueueUrl));
        sqs.deleteQueue(new DeleteQueueRequest(workersQueueUrl));
        sqs.deleteQueue(new DeleteQueueRequest(workersResponseQueueUrl));
        
        System.out.println("Terminating Manager is done.");
	}

}

class MessageHolder{
	private int messageCount;
	private ArrayList<String> urlList;
	private String responseUrl;
	private boolean sentOnce;
	public  Map<String, Boolean> sourceUrlsHashMap; 
	
	MessageHolder(int count, String url){
		messageCount = count;
		responseUrl = url;
		urlList = new ArrayList<String>();
		sourceUrlsHashMap = new HashMap<String, Boolean>();
		sentOnce = false;
	}
	
	public boolean addSourceUrl(String sourceUrl) {
		if(!sourceUrlsHashMap.containsKey(sourceUrl)){
			sourceUrlsHashMap.put(sourceUrl, new Boolean(true));
			return true;
		}
		messageCount--;
		return false;
	}

	public boolean add(String key,String url){
		if(sourceUrlsHashMap.containsKey(key) && sourceUrlsHashMap.get(key).booleanValue() == true){
			urlList.add(url);
			sourceUrlsHashMap.put(key, new Boolean(false));
			return true;
		}
		return false;
	}
	
	public boolean isFinished(){
		if(messageCount == urlList.size() && sentOnce == false){
			sentOnce = true;
			return true;
		}
		return false;
	}
	
	public ArrayList<String> getUrlList(){
		return urlList;
	}
	
	public String getResponseUrl(){
		return responseUrl;
	}
}

