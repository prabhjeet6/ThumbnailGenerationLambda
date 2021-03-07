package com.thumbnailgenerator;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.commons.io.FileUtils;
import org.jcodec.api.FrameGrab;
import org.jcodec.api.JCodecException;
import org.jcodec.common.io.NIOUtils;
import org.jcodec.common.model.Picture;
import org.jcodec.scale.AWTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
/**
 * @author Prabhjeet Singh
 *
 * Feb 15, 2021
 */
public class ThumbnailGenerationLambda implements RequestHandler<S3Event, String> {
	
	private static final Logger logger = LoggerFactory.getLogger(ThumbnailGenerationLambda.class);
	
	/*public static void main(String args[]) throws IOException, JCodecException {
		new Test().thumbnailGenerator("E:\\music\\Indian Idol Junior(HD) Ranita Banerjee - Tere Bina Jiya Jaye Na.mp4");

	}*/

	private void thumbnailGenerator(String videoFilePath,String thumbnailName) throws IOException, JCodecException {

		FrameGrab grab = FrameGrab.createFrameGrab(NIOUtils.readableChannel(new File(videoFilePath)));
		int totalFrames=grab.getVideoTrack().getMeta().getTotalFrames();
		int randomFrame=(int) (Math.random()*(totalFrames-1));
		Picture picture = FrameGrab.getFrameFromFile(new File(videoFilePath),
		 randomFrame);
		BufferedImage bufferedImage = AWTUtil.toBufferedImage(picture);
		ImageIO.write(bufferedImage, "png", new File(thumbnailName));

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.lambda.runtime.RequestHandler#handleRequest(java.lang.Object, com.amazonaws.services.lambda.runtime.Context)
	 */
	@Override
	public String handleRequest(S3Event s3event, Context context) {
		
	try {	
		S3EventNotificationRecord record = s3event.getRecords().get(0);
		
		String sourceBucket = record.getS3().getBucket().getName();
		//Key is qualified name of an object in S3 bucket.
		String sourceKey = record.getS3().getObject().getUrlDecodedKey();

		String destinationBucket = sourceBucket;
		//using '.' only for extensions to reduce parsing complexities. 
		//TODO: check if it is required, or will be taken from the generator method
	    String destinationKey = sourceKey.replace(".","_thumbnail.");
	    
	    //AWS SDK API for Java to connect to S3
	    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
	    S3Object s3Object = s3Client.getObject(new GetObjectRequest(
	              sourceBucket, sourceKey));
	    
	    InputStream objectData = s3Object.getObjectContent();
	    
	    File targetFile = new File(".");
	    
	    FileUtils.copyInputStreamToFile(objectData, targetFile);
	
	    thumbnailGenerator(targetFile.getAbsolutePath(),destinationKey);
			
	    s3Client.putObject(destinationBucket, destinationKey, new File(destinationKey));
		} catch (IOException | JCodecException e) {
			e.printStackTrace();
		}
		return null;
	}
}
