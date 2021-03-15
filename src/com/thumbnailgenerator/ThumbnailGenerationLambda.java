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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * @author Prabhjeet Singh
 *
 *         Feb 15, 2021
 */
public class ThumbnailGenerationLambda implements RequestHandler<S3Event, String> {

	private static final Logger logger = LoggerFactory.getLogger(ThumbnailGenerationLambda.class);

	@Override
	public String handleRequest(S3Event s3event, Context context) {
		String destinationKey = null;
		try {
			S3EventNotificationRecord record = s3event.getRecords().get(0);

			String sourceBucket = record.getS3().getBucket().getName();
			String sourceKey = record.getS3().getObject().getUrlDecodedKey();

			String destinationBucket = sourceBucket.replace("bucket", "thumbnails");
			destinationKey = sourceKey.replace(".", "_thumbnail.");

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(new AWSCredentials() {
						@Override
						public String getAWSSecretKey() {
							return "your secret key";
						}

						@Override
						public String getAWSAccessKeyId() {
							return "your access key";
						}
					})).withRegion(Regions.US_EAST_2).build();

			S3Object s3Object = s3Client.getObject(new GetObjectRequest(sourceBucket, sourceKey));
			InputStream objectData = s3Object.getObjectContent();
			String fileName = sourceKey.substring(sourceKey.indexOf("/") + 1);
			String extension = sourceKey.substring(sourceKey.indexOf("."));
			File sourceFile = File.createTempFile(sourceKey, extension);
			FileUtils.copyInputStreamToFile(objectData, sourceFile);

			FrameGrab grab = FrameGrab.createFrameGrab(NIOUtils.readableChannel(sourceFile));
			int totalFrames = grab.getVideoTrack().getMeta().getTotalFrames();
			int randomFrame = (int) (Math.random() * (totalFrames - 1));
			Picture picture = FrameGrab.getFrameFromFile(sourceFile, randomFrame);
			BufferedImage bufferedImage = AWTUtil.toBufferedImage(picture);
            File thumbnailFile=new File("/tmp/"+destinationKey.substring(destinationKey.indexOf("/") + 1, destinationKey.indexOf("."))+".png");
			ImageIO.write(bufferedImage, "png", thumbnailFile);
			s3Client.putObject(new PutObjectRequest(destinationBucket, destinationKey.substring(destinationKey.indexOf("/") + 1, destinationKey.indexOf("."))+".png", thumbnailFile).withCannedAcl(CannedAccessControlList.PublicRead));
			sourceFile.delete();

		} catch (IOException | JCodecException e) {
			e.printStackTrace();
		}
		return destinationKey;
	}
}
