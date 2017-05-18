package xmlwork3;

import auxillary.Percent;
import auxillary.Randoms;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class XMLgenerator implements Runnable {

    public final String encoding;
    public final String ver;
    public final String namespace;
    private static final Logger LOG = Logger.getLogger(XMLgenerator.class.getName() );
    private final long numRecords;
    private final int maxStrLen;
    private final int maxRecRowNr;
    private final ArrayBlockingQueue<Long> statusQueue;
    private int seqNr;//record sequence number;
    private boolean generatorFinished;//flag to indicate that generator has finished. Used for files, smaller than cache size.
    private final ChunkWriter chunkWriter;
    final ByteArrayOutputStream out;
    private long recCount = 0l, recRowCount = 0l;
    
    public XMLgenerator(ArrayBlockingQueue<Object> arrBQ,
            ArrayBlockingQueue<Long> statusQueue,
            XMLGenParam param){
        //this.arrayBlockQ = arrBQ;
        //this.params = param;
        numRecords = param.numRecords;
        maxStrLen = param.maxStrLength;
        maxRecRowNr = param.maxRecRowNum;
        encoding = param.encoding;
        ver = "1.0";
        namespace = param.namespace;
        //cacheSize = ;//the max size of ByteArrayOutputStream. Writing begins when this size is reached.
        seqNr = param.startFrom;//record counter
        out = new ByteArrayOutputStream();
        chunkWriter = new ChunkWriter( arrBQ, param.cacheSize, out);
        this.statusQueue = statusQueue;
    }
    
    /**
     * @throws java.io.FileNotFoundException
     * @throws javax.xml.stream.XMLStreamException
     * @throws java.lang.InterruptedException
     */
    private void makeXML() throws FileNotFoundException, XMLStreamException, IOException, InterruptedException {
        generatorFinished = false;
        //long startTimeMillis = System.currentTimeMillis();

        final XMLStreamWriter writer
                = XMLOutputFactory.newInstance().createXMLStreamWriter( out, "UTF-8" );

        int curMaxStrLen, curRecRowNr;
        String randStr;//string, to be filled with random chars
        //Header of document    
        writer.writeStartDocument( encoding, ver );
        writer.writeStartElement( "record-table" );
        writer.flush();
        //byteBuffer = ByteBuffer.wrap( out.toByteArray() );        
        chunkWriter.sendChunkToWrite( generatorFinished, false );
        //Main part of the document    
        for ( long i = 0l; i < numRecords; i++ ) {

            curRecRowNr = Randoms.getRandInRange( 1, maxRecRowNr );
            //LOG.log( Level.INFO, "MaxStrLen: " + curMaxStrLen +" curRecRowNr: "+ curRecRowNr);

            writer.writeStartElement( "record" );
            writer.writeStartElement( "record_id" );
            writer.writeCharacters( Integer.toString( seqNr ) );
            writer.writeEndElement();//close record-id
            writer.writeStartElement( "record_rows" );
            seqNr++;
            writer.flush();
            chunkWriter.sendChunkToWrite( generatorFinished, false );

            for ( int j = 0; j < curRecRowNr; j++ ) {
                curMaxStrLen = Randoms.getRandInRange( 1, maxStrLen );
                randStr = "";

                for ( int k = 0; k < curMaxStrLen; k++ ) {
                    randStr += Randoms.getRandChar( 48, 122 );
                }
                writer.writeStartElement( "record_row" );
                writer.writeCharacters( randStr );
                writer.writeEndElement();//close record-row
                recRowCount++;
                chunkWriter.sendChunkToWrite( generatorFinished, false );
            }
            writer.writeEndElement();//close record-rows
            writer.writeEndElement();//close record

            chunkWriter.sendChunkToWrite( generatorFinished, false );
            recCount++;
            statusQueue.put( Percent.percent( i, numRecords ) );
        }
        //write footer
        writer.writeStartElement( "footer" );
        writer.writeStartElement( "record_count" );
        writer.writeCharacters( Long.toString( recCount ) );
        writer.writeEndElement();
        writer.writeStartElement( "record_row_count" );
        writer.writeCharacters( Long.toString( recRowCount ) );
        writer.writeEndElement();
        writer.writeEndElement();
        //End of document
        writer.writeEndElement();//close record-table
        writer.writeEndDocument();
        writer.flush();
        chunkWriter.sendChunkToWrite( true, false );              
        statusQueue.put( 100l );
        writer.close();  
        //LOG.log(Level.INFO, "Finished in: {0} milliseconds.", endTimeMillis - startTimeMillis);
    }
    
    @Override
    public void run() {
        try {
            makeXML();
        } catch ( XMLStreamException ex ) {
            Logger.getLogger( XMLgenerator.class.getName() ).log( Level.SEVERE, null, ex );
        } catch ( IOException ex ) {
            Logger.getLogger( XMLgenerator.class.getName() ).log( Level.SEVERE, null, ex );
        } catch ( InterruptedException ex ) {
            Logger.getLogger( XMLgenerator.class.getName() ).log( Level.SEVERE, null, ex );
        }
    }
}
