
package org.apache.pdfbox.util;

import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import javax.imageio.ImageIO;
import com.sun.pdfview.PDFFile;
import com.sun.pdfview.PDFPage;

public class PDF2Image 
{ 
	public static void createImage(File pdfFile, File output) throws IOException{
		
		RandomAccessFile raf = new RandomAccessFile(pdfFile, "r");
		FileChannel channel = raf.getChannel();
		ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
		PDFFile pdf = new PDFFile(buf);
		PDFPage page = pdf.getPage(0);

		// create the image
		Rectangle rect = new Rectangle(0, 0, (int) page.getBBox().getWidth(),
		                                 (int) page.getBBox().getHeight());
		BufferedImage bufferedImage = new BufferedImage(rect.width, rect.height,
		                                  BufferedImage.TYPE_INT_RGB);

		Image image = page.getImage(rect.width, rect.height,    // width & height
		                            rect,                       // clip rect
		                            null,                       // null for the ImageObserver
		                            true,                       // fill background with white
		                            true                        // block until drawing is done
		);
		Graphics2D bufImageGraphics = bufferedImage.createGraphics();
		bufImageGraphics.drawImage(image, 0, 0, null);
		ImageIO.write(bufferedImage, "jpg", output);
		raf.close();
		while(!pdfFile.delete()){
			System.gc();
			System.out.println("ERROR");
		}

	}
		
		
	
}
