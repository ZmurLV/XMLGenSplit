package xmlwork3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class ChunkWriter {
    final ArrayBlockingQueue<Object> arrayBlockQ;
    final ByteArrayOutputStream out;
    final int cacheSize;
    
    public ChunkWriter( ArrayBlockingQueue<Object> arrayBlockQ, int cacheSize, ByteArrayOutputStream out ){
        this.arrayBlockQ = arrayBlockQ;
        this.out = out;
        this.cacheSize = cacheSize;
    }
    /**
     * Writes the data that is currently inside ByteArrayOutputStream. Clears
     * ByteArray after that.
     * @param isFinished if true, sends all cache to writer even if it is not filled yet
     * @param newFile sends empty String to writer. It will interpret it as a command to create new file
     * @return bytes in cache, before it was sent to write and emptied
     * @throws IOException
     * @throws java.lang.InterruptedException
     */
    public long sendChunkToWrite( boolean isFinished, boolean newFile ) throws IOException, InterruptedException {
        if ( out.size() >= cacheSize | isFinished ) {
            //LOG.log( Level.INFO,"Put to queue.");
            long written = out.size();
            if(newFile){
                arrayBlockQ.put( out.toByteArray() );                
                out.reset();
                arrayBlockQ.put("");
                //System.out.println("Written, New file = " + written);
                return written;
            }
            else{
                arrayBlockQ.put( out.toByteArray() );
                out.reset();
                //System.out.println("Written = " + written);
                return written;
            }
        }
        return 0;
    }
}
