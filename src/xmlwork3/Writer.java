package xmlwork3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Writes data through a FileChannel to file, by taking elements from provided BlockingQueue
 * and converting its data to byte[].<br>
 * If object, pulled out of blockingQueue cannot be cast to byte[] and writer is created in split mode,
 * the exception is thrown and interpreted as a command to start new file.
 *
 * @author Zmr
 */
public class Writer implements Runnable {

    private final ArrayBlockingQueue arrayBlockQ;
    private FileOutputStream fos;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer = null;
    private byte[] inByteArray;
    private String directory;
    private int fileCount = 1;
    private File file;
    private String filename = "splitFile_";
    private final boolean fileSplit;

    public Writer( ArrayBlockingQueue<Object> arrBQ,            
            File file, boolean fileSplit ) throws FileNotFoundException {
        this.arrayBlockQ = arrBQ;
        this.file = file;
        this.fileSplit = fileSplit;
        if ( this.fileSplit ) {
            directory = this.file.getAbsolutePath()+"\\";
            fos = new FileOutputStream( new File( directory, filename + fileCount + ".xml" ) );
            System.out.println("Filename: " + directory  + filename + fileCount);
        } else {
            fos = new FileOutputStream( file );
        }
        fileChannel = fos.getChannel();
    }

    private void write() {
        //System.out.println( "Write started" );
        try {
            inByteArray = (byte[])arrayBlockQ.take();
        } catch ( InterruptedException ex ) {
            Logger.getLogger( Writer.class.getName() ).log( Level.SEVERE, null, ex );
        }
        catch( ClassCastException cce ){
            if(fileSplit){ //treat ClassCastException as command only if splitting a file
                //create a new file if other class was drained from queue (poison object)
                //create new file part: increment name, create new channel
                //System.out.println("create new file");
                fileCount++;
                file = new File( directory, filename + fileCount + ".xml" );
                try {
                    fos.close();
                    fos = new FileOutputStream( file );
                    fileChannel = fos.getChannel();
                } catch ( FileNotFoundException ex ) {
                    Logger.getLogger( Writer.class.getName() ).log( Level.SEVERE, null, ex );
                } catch ( IOException ex ) {
                    Logger.getLogger( Writer.class.getName() ).log( Level.SEVERE, null, ex );
                }
                return;
            }
            else{
                Logger.getLogger( Writer.class.getName() ).log( Level.SEVERE, null, cce );
            }
        }
        byteBuffer = ByteBuffer.wrap( inByteArray );//wrap for writing
        try {
            fileChannel.write( byteBuffer );
        } catch ( IOException ex ) {
            Logger.getLogger( Writer.class.getName() ).log( Level.SEVERE, null, ex );
        }
        byteBuffer.clear();
    }

    @Override
    public void run() {
        while ( !Thread.currentThread().isInterrupted() ) {
            write();
        }
        try {
            fos.close();
            fileChannel.close();
        } catch ( IOException ex ) {
            Logger.getLogger( Writer.class.getName() ).log( Level.SEVERE, null, ex );
        }
    }
}
