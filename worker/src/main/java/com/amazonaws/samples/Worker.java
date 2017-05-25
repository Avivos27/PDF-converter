package com.amazonaws.samples;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;



public class Worker {
	private static String responseQueueUrl = "";
	private static String recieveQueueUrl = "";
	private static AmazonSQS sqs = null;
	private static AmazonEC2 ec2 = null;
	private static AmazonS3 s3 = null;



	public static void main(String[] args){	
		System.out.println("Worker launched");	
		ec2 = new AmazonEC2Client();
		sqs = new AmazonSQSClient();
		s3 = new AmazonS3Client();
		
		System.out.println("created temp "+ new File("temp").mkdir());
		
		
		//iterate on ec2 instances, find manager and get queues URLs stored in tags
		boolean found1 = false;
        boolean found2 = false;
        System.out.println("Looking for manger instance..");
        while(!found1 || !found2){
		
        	DescribeInstancesRequest request = new DescribeInstancesRequest();
        	DescribeInstancesResult result = ec2.describeInstances(request);
        	List<Reservation> reservations = result.getReservations();
    
	        for (Reservation reservation : reservations) {
	    		for (Instance instance : reservation.getInstances()) {
	    			 if(instance.getPublicIpAddress()!= null){ 								//check only active ec2 nodes
	    				 if(instance.getTags() != null && !instance.getTags().isEmpty()){  		//look for ec2 node with tags
	    					 for(int i=0; i<instance.getTags().size();i++){
	    						 if("type".equals(instance.getTags().get(i).getKey()) && "manager".equals(instance.getTags().get(i).getValue())){ 	// look for manager
//	    							 System.out.println("in manager");
	    							 for(int j=0; j<instance.getTags().size();j++){ 	
		    								if("workersQueue".equals(instance.getTags().get(j).getKey())){
		    									recieveQueueUrl = instance.getTags().get(j).getValue(); 
		    									found1 = true;
		    								}
		    								if("workersResponseQueue".equals(instance.getTags().get(j).getKey())){
		    									responseQueueUrl = instance.getTags().get(j).getValue(); 
		    									found2 = true;
		    								}

		    						}
	    						 }
	    					 }
	    				 }
	    			 }
	    		}
	        }
        }
        
        System.out.println("found URLS:");
        System.out.println("workersQueue: " + recieveQueueUrl);
        System.out.println("workersResponseQueue: " + responseQueueUrl);
        
        // create threads
        ExecutorService executor = Executors.newFixedThreadPool(8);
        for (int i = 0; i < 8; i++) {
        	System.out.println("creating new worker thread with ID:"+i);
        	Runnable worker = new WorkerThread(""+i);
            executor.execute(worker);
          }
        executor.shutdown();
        while (!executor.isTerminated()) {
        	//wait until all threads are done
        }    
        
        // shut-down main program  
        System.out.println("Worker main thread is shutting down");
        try {
			FileUtils.deleteDirectory(new File("temp"));
		} catch (IOException e) {
			System.out.println("Failed to delete temp folder");
		}
	}
	
	
	/*  =====================================================
	 *  |||||||||||||||||||||||||||||||||||||||||||||||||||||
	 * 	========= WorkerThread Implementation ===============
	 *  |||||||||||||||||||||||||||||||||||||||||||||||||||||
	    ===================================================== */ 
	static class WorkerThread implements Runnable{
		
		private File pdfFile = null;
		private File txtFile = null;
		private File jpgFile = null;
		private File htmlFile = null;
		private String tid = null;
		
		
		public WorkerThread(String id){
			tid = id;
		}
		
		public void run() {
			System.out.println("Worker_"+tid+" starts listening");
			boolean terminate = false;
			while(!terminate){
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(recieveQueueUrl);
				List<Message> messages = null;
				try{
					messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				}catch (QueueDoesNotExistException e){
					System.out.println("Worker_"+tid+" Caught an sqs queue does not exists, means manager is terminated");
					System.out.println("Terminating worker thread_"+tid);
					terminate = true;
					break;
				}
				
		        for (Message message : messages) {
		        	System.out.println();
		        	System.out.println("Worker_"+tid+" got message");
		        	System.out.println("       "+message.getBody());
		        	
		        	
		        	
		        	//response queue message format is : new_pdf_task operation pdfURL inputKey
		        	//split by whitespace into array
		        	 String[] messageArr = message.getBody().split(Pattern.quote(" "));
			        		
		        	 // check for right format of message
		        	 if(messageArr == null || messageArr.length != 4 ){
	        		    String error = "ERROR: Message is empty, or not in the right format";
	 	        		String errorMessage = "done_pdf_task unknown_pdf_url unknown_operation error unknown_inputKey " + error;
	 	        		try{
	 	        			sqs.sendMessage(responseQueueUrl, errorMessage);
	        				System.out.println("Worker_"+tid+" sent an ERROR message to manger: " + errorMessage);
	        			
	        				System.out.println("Worker_"+tid+" removing error new_task_pdf message: " + message.getBody());
	 	        			String messageReceiptHandle = message.getReceiptHandle();
	 	        			sqs.deleteMessage(new DeleteMessageRequest(recieveQueueUrl, messageReceiptHandle));	
	 	        			continue;
	 	        		}catch (QueueDoesNotExistException e){
	 						System.out.println("Worker_"+tid+" Caught an sqs queue does not exists, means manager is terminated");
	 						System.out.println("Terminating worker thread_"+tid);
	 						terminate = true;
	 						break;
	 					}
		        	}
	
		        	 
		        	 //change message visibility to 30 seconds
		        	 try{
		        		 sqs.changeMessageVisibility(recieveQueueUrl, message.getReceiptHandle(), 75);
		        	 } catch (AmazonSQSException e){
		        		 // tried to pull message that is already handled by other thread, leave it alone
		        		 continue; 
		        	 }
		        	 
		        	 //extract new_pdf_task parts
		        	 //String type = messageArr[0];
		        	 String operation = messageArr[1];
		        	 String pdfURL = messageArr[2];
		        	 String inputKey = messageArr[3];
		        	 String s3Key = "";

		        	 boolean replacedHttp = false;
		        	 PDDocument doc = null;
		        	 
		        	 try{
		        		 UrlToFile(pdfURL); //instantiates the pdfFile file
		        		 doc = PDDocument.load(pdfFile);
		        	 }
		        	//we will try again with https instead
		        	 catch(IOException e) { 	
		        		 System.out.println("Worker_"+tid+" Replacing to https");
		        		 pdfURL = pdfURL.replace("http", "https");
		        		 replacedHttp = true;
		        		 try{
		        			 UrlToFile(pdfURL); //instantiates the pdfFile file
		        			 doc = PDDocument.load(pdfFile);
		        		 }catch (IOException e1){
							System.out.println("Worker_"+tid+" WARNING: Exception message: " + e1.getMessage());
							terminate = handleException("IOException " + e1.getMessage(), message,s3Key);
							continue;
		        		}	
		        	}

		        	 try {

		 				String resultUrl = "";
		 				
			        	if (doc!=null){
			        		if(operation.equals("ToHTML")){
			        			s3Key = toHTML(doc);
			        			resultUrl = "https://s3.amazonaws.com/workers-uploads/"+s3Key;
			        		}
			        		else if(operation.equals("ToImage")){
			        			doc.close();
			        			s3Key = toImage();
			        			resultUrl = "https://s3.amazonaws.com/workers-uploads/"+s3Key;
			        		}
			        		else if(operation.equals("ToText")){
			        			s3Key = toText(doc);
			        			resultUrl = "https://s3.amazonaws.com/workers-uploads/"+s3Key;
			        		}	
			        	}
			        	
			        	// replace back to http if needed (because manager is working with hashMap and url is key)
			        	if(replacedHttp)
			        		pdfURL = pdfURL.replace("https", "http");
			        	
	
		        		pdfFile.delete();
		 	        	pdfFile = null;
			        	
		 	        	// error checking
		 	        	if(doc == null || resultUrl == null){ // should not be able to reach here
		 	        		try{
			 	        		System.out.println("Worker_"+tid+" ERROR: result URL or doc not working");
			 	        		String errorDesc = "ERROR: worker failed to load pdf (received null pointer)";
			 	        		if(resultUrl == null)
			 	        			errorDesc = "ERROR: worker failed to upload result to s3";
			 	        		String errorMessage = "done_pdf_task " + pdfURL + " " + operation + " " + "error" + inputKey + " " + s3Key + " " + errorDesc;
		 	        			sqs.sendMessage(responseQueueUrl, errorMessage);
		 	        			System.out.println("Worker_"+tid+" sent an ERROR message to manger: " + errorMessage);
		 	        			
		 	        			System.out.println("Worker_"+tid+" removing error new_task_pdf message: " + message.getBody());
			 	        		String messageReceiptHandle = message.getReceiptHandle();
			 	        		sqs.deleteMessage(new DeleteMessageRequest(recieveQueueUrl, messageReceiptHandle));
		 	        		}catch (QueueDoesNotExistException e){
		 						System.out.println("Worker_"+tid+" Caught an sqs queue does not exists, means manager is terminated");
		 						System.out.println("Terminating worker thread "+tid);
		 						terminate = true;
		 						break;
		 					}
	
		 	        	}
		 	        	
		 	        	// send a done_task_pdf message to manager and remove the new_pdf_task message 
		 	        	else{
		 	        		String completionMessage = "done_pdf_task " + pdfURL + " " + operation + " " + resultUrl +" "+ inputKey +" "+ s3Key;
		 	        		try{
		 	        			sqs.sendMessage(responseQueueUrl, completionMessage);
			 	        		System.out.println("Worker_"+tid+" sent a SUCCESS message to manger: " + completionMessage);
			 	        		
			 	        		System.out.println("Worker_"+tid+" removing new_task_pdf message: " + message.getBody());
			 	        		String messageReceiptHandle = message.getReceiptHandle();
			 	        		sqs.deleteMessage(new DeleteMessageRequest(recieveQueueUrl, messageReceiptHandle));
		 	        		}catch (QueueDoesNotExistException e){
		 						System.out.println("Worker_"+tid+" Caught an sqs queue does not exists, means manager is terminated");
		 						System.out.println("Terminating worker thread "+tid);
		 						terminate = true;
		 						break;
		 					}
		 	        	}	
		 	        	
		        	}catch (MalformedURLException e){
						System.out.println("Worker_"+tid+" WARNING: Exception message: " + e.getMessage());
						terminate = handleException("MalformedURLException " + e.getMessage(), message,s3Key);

					} catch (InvalidPasswordException e) {
						System.out.println("Worker_"+tid+" WARNING: Exception message: " + e.getMessage());
						terminate = handleException("InvalidPasswordException " + e.getMessage(), message,s3Key);

					} catch (IOException e) {
						System.out.println("Worker_"+tid+" WARNING: Exception message: " + e.getMessage());
						terminate = handleException("IOException " + e.getMessage(), message, s3Key);
					}  
		        }
			}
		}
		
		private boolean handleException(String error,Message message,String s3Key) {
			try{
				//extract new_pdf_task parts
				String[] messageArr = message.getBody().split(Pattern.quote(" "));
			   	String operation = messageArr[1];
			   	String pdfURL = messageArr[2];
			   	String inputKey = messageArr[3];
				String errorMessage = "done_pdf_task " + pdfURL + " " + operation + " " + "error " + inputKey +  " "+ s3Key + " " + error;
				sqs.sendMessage(responseQueueUrl, errorMessage);
				System.out.println("Worker_"+tid+" sent an error message to manger: " + errorMessage);
				System.out.println("Worker_"+tid+" removing error new_task_pdf message: " + message.getBody());
		 		String messageReceiptHandle = message.getReceiptHandle();
		 		sqs.deleteMessage(new DeleteMessageRequest(recieveQueueUrl, messageReceiptHandle));
		 		return false;
			}catch (QueueDoesNotExistException e){
					System.out.println("Worker_"+tid+" Caught an sqs queue does not exists, means manager is terminated");
					System.out.println("Terminating worker thread "+tid);
					return true;
				}
		}
	
		private boolean uploadToS3(String bucketName,String key, String type){
		    try {
				if(type.equals("text")){
			        System.out.println("Uploading text file: " + key + " to S3 bucket: " + bucketName);
			        s3.putObject(new PutObjectRequest(bucketName, key, txtFile).withCannedAcl(CannedAccessControlList.PublicRead));
			        System.out.println("Uploading successful");
			        
				}
				else if(type.equals("image")){
			        System.out.println("Uploading image file: " + key + " to S3 bucket: " + bucketName);
			        s3.putObject(new PutObjectRequest(bucketName, key, jpgFile).withCannedAcl(CannedAccessControlList.PublicRead));
			        System.out.println("Uploading successful");
			        
				}
				if(type.equals("html")){
			        System.out.println("Uploading image file: " + key + " to S3 bucket: " + bucketName);
			        s3.putObject(new PutObjectRequest(bucketName, key, htmlFile).withCannedAcl(CannedAccessControlList.PublicRead));
			        System.out.println("Uploading successful");
			        
				}
				return true;
	
		    }
		    catch (AmazonServiceException ase) {
		        System.out.println("Caught an AmazonServiceException, which means your request made it "
		                + "to Amazon S3, but was rejected with an error response for some reason.");
		        System.out.println("Error Message:    " + ase.getMessage());
		        System.out.println("HTTP Status Code: " + ase.getStatusCode());
		        System.out.println("AWS Error Code:   " + ase.getErrorCode());
		        System.out.println("Error Type:       " + ase.getErrorType());
		        System.out.println("Request ID:       " + ase.getRequestId());
		        return false;
		    } catch (AmazonClientException ace) {
		        System.out.println("Caught an AmazonClientException, which means the client encountered "
		                + "a serious internal problem while trying to communicate with S3, "
		                + "such as not being able to access the network.");
		        System.out.println("Error Message: " + ace.getMessage());
		        return false;
		    }
		}
		
		//strips first page of PDF to text and uploads to s3, returns file url or null if fails
		private String toText(PDDocument doc) throws IOException {
			
			System.out.println("Creating text file from pdf..");
			
			//extract first page to new file
			PDDocument docFirstPage = new PDDocument();
			docFirstPage.addPage((PDPage) doc.getDocumentCatalog().getPages().get(0));   
			docFirstPage.save("temp\\"+tid+"_firstPage.pdf");  
			docFirstPage.close(); 
			
			
		    //strip first_page to text file txtFile.txt
			PDFTextStripper textStripper = new PDFTextStripper();
			String text =  textStripper.getText(docFirstPage);
			txtFile = new File("temp\\"+tid+"_txtFile.txt");
			BufferedWriter writer = new BufferedWriter(new FileWriter(txtFile));
			writer.write(text);
			writer.close();
			doc.close();
			
			//create key and upload text file
			String key = "txt_"+UUID.randomUUID();
			boolean uploaded = uploadToS3("workers-uploads", key, "text");
			
			//delete created files from local worker file system
			deleteByPath("temp\\"+tid+"_firstPage.pdf");
			deleteByPath("temp\\"+tid+"_txtFile.txt");
	//		deleteByPath("fullPdf.pdf");
			
			System.out.println("Finsihed creating text file from pdf");
			
			if (uploaded)
				return key;
			else 
				return null;
		}
	
		private String toImage() throws IOException{
			
			System.out.println("Creating image file from pdf..");
			PDDocument document = PDDocument.load(pdfFile);
			PDFRenderer pdfRenderer = new PDFRenderer(document);
			BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
			// suffix in filename will be used as the file format
			String filename = "temp\\"+tid+"_image.png";
			ImageIOUtil.writeImage(bim, filename, 300);
			jpgFile = new File(filename);
			document.close();
			String key = "image_"+UUID.randomUUID() + ".png";
			boolean uploaded = uploadToS3("workers-uploads", key, "image");
			//delete created files from local worker file system
			deleteByPath(filename);
			
			System.out.println("Finsihed creating image file from pdf");
			
			if (uploaded)
				return key;
			else 
				return null;
		}
	
		private String toHTML(PDDocument doc) throws IOException {
			
			System.out.println("Creating HTML file from pdf..");
			
			//extract first page to new file
			PDDocument docFirstPage = new PDDocument();
			docFirstPage.addPage((PDPage) doc.getDocumentCatalog().getPages().get(0));   
			docFirstPage.save("temp\\"+tid+"_firstPage.pdf");  
			docFirstPage.close(); 
			
			
		    //strip first_page to string
			PDFTextStripper textStripper = new PDFTextStripper();
			String text =  textStripper.getText(docFirstPage);
	
			//create HTML file from string extracted from first page of pdf
			htmlFile = new File("temp\\"+tid+"_file.html");
			BufferedWriter bw = new BufferedWriter(new FileWriter(htmlFile));
			String lines[] = text.split("\\r?\\n");
			bw.write("<html><head><title>New Page</title></head><body><p>");
			for(int i=0;i<lines.length;i++){
				bw.write(lines[i] + "<br>");
			}
			bw.write("</p></body></html>");
			bw.close();
			
			//create key and upload HTML file
			String key = "html_"+UUID.randomUUID();
			boolean uploaded = uploadToS3("workers-uploads", key, "html");
			
			
			doc.close();
			docFirstPage.close();
			
	//		deleteByPath("fullPdf.pdf");
			deleteByPath("temp\\"+tid+"_firstPage.pdf");
			deleteByPath("temp\\"+tid+"_file.html");
			
			System.out.println("Finsihed creating HTML file from pdf");
			
			if (uploaded)
				return key;
			else 
				return null;
			
			
			
		}	
			
		private void UrlToFile(String url_string) throws IOException{
			URL url = null;
			try {
				url = new URL(url_string);
			} catch (MalformedURLException e) {	
				System.out.println("WARNING: caught MalformedURLException "  + url_string);
			}
			pdfFile = new File("temp\\"+tid+"_fullPdf.pdf");
			if(pdfFile != null && url != null)
				FileUtils.copyURLToFile(url, pdfFile);	
		}
		
		private void deleteByPath(String filePath) throws IOException {
				Path path = Paths.get(filePath);
			    Files.delete(path);
	
		}
	}
}
