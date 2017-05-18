package xmlwork3;

import auxillary.Percent;
import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

class DataSaxHandler extends DefaultHandler {
    final static int SIZE_CORRECTION = 225;//correction of size for header and footer
    final String encoding = "UTF-8";
    final String version = "1.0";
    private ChunkWriter chunkWriter;//buffer for writing chunks of data
    public final String DIRECTORY;
    public final long MAX_PART_SIZE;//max split file size
    private final XMLStreamWriter writer;
    private String currentElement = null;
    private long recRowCount;
    private long recCount;
    private StringBuffer tagRowBuffer, tagIDBuffer;
    private boolean skipRecordClose = false;
    private long curPartSize = 0, processedSize = 0; //processed data sizes to control progress and written file length
    private final long sourceFileSize; //unsplit file size for progress control
    private final ArrayBlockingQueue<Long> statusQueue;//for report progress
/**
 * @param file File to split
 * @param filesize Splitted File size in bytes
 * @param cache cache size for writing. Must be less than filesize
 * @param arrayBlockQ contains bytes to be written to file
 * @param statusQueue used by thread in main to read status reports of this thread
 * @throws XMLStreamException 
 */
    public DataSaxHandler( File file, int filesize, int cache,
            ArrayBlockingQueue<Object> arrayBlockQ,
            ArrayBlockingQueue<Long> statusQueue ) throws XMLStreamException {
        this.DIRECTORY = file.getParent();//.getPath();
        this.MAX_PART_SIZE = filesize;
        sourceFileSize = file.length();//get file size in bytes, for progress report
        System.out.println( "sourceFileSize " + file.length() );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer = XMLOutputFactory.newInstance().createXMLStreamWriter( out, "UTF-8" );
        chunkWriter = new ChunkWriter( arrayBlockQ, cache, out );
        this.statusQueue = statusQueue;
    }

    @Override
    public void startDocument() throws SAXException {
        try {
            writer.writeStartDocument( encoding, version );
        writer.writeStartElement( "record-table" );
        } catch ( XMLStreamException ex ) {
            Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
        }
    }

    @Override
    public void startElement( String uri, String localName, String qName,
            Attributes atts ) throws SAXException {
        currentElement = qName;//used for reading tag contents
        if ( qName.equals( "record" ) ) {            
            try {
                writer.writeStartElement( "record" ); 
                recCount++;
            } catch ( XMLStreamException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            }
        }
        if ( qName.equals( "record_id" ) ) {
            tagIDBuffer = new StringBuffer();//reset buffer to get new record ID
        }
        if ( qName.equals( "record_rows" ) ) {
            try {
                writer.writeStartElement( "record_rows" );
            } catch ( XMLStreamException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            }
        }
        if ( qName.equals( "record_row" ) ) {
            recRowCount++;
            tagRowBuffer = new StringBuffer();//reset buffer to get new record_row contents
        }
        if ( qName.equals( "footer" ) ) {
            try {
                //reached footer, close doc, write remains
                writeFooter( recRowCount, recCount );
                chunkWriter.sendChunkToWrite( true, false );//write all there is in buffer even if it is not full
                statusQueue.offer(100l); //finished, report status
            } catch ( XMLStreamException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            } catch ( IOException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            } catch ( InterruptedException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            }
        }

    }

    @Override
    public void endElement( String uri, String localName, String qName ) throws SAXException {
        if ( qName.equals( "record_row" ) ) {
            try {
                writeRecordRow();
            } catch ( XMLStreamException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            } catch ( IOException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            } catch ( InterruptedException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            }
        }
        if ( qName.equals( "record_rows" ) ) {
            if ( !skipRecordClose ) {
                try {
                    writer.writeEndElement();
                } catch ( XMLStreamException ex ) {
                    Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
                }
            }
        }
        if ( qName.equals( "record_id" ) ) {
            try {
                writeID();
            } catch ( XMLStreamException ex ) {
                Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
            }
        }
        if ( qName.equals( "record" ) ) {
            if ( !skipRecordClose ) {
                try {
                    writer.writeEndElement();
                } catch ( XMLStreamException ex ) {
                    Logger.getLogger( DataSaxHandler.class.getName() ).log( Level.SEVERE, null, ex );
                }
            }
        }
    }

    @Override
    public void characters( char[] ch, int start, int length ) throws SAXException {
        if ( currentElement == null ) {
            return;
        }
        if ( currentElement.equals( "record_id" ) ) {            
            tagIDBuffer.append( new String( ch, start, length ) );            
        }
        if ( currentElement.equals( "record_row" ) ) {
            tagRowBuffer.append( new String( ch, start, length ) );
        }
    }
/**
 * Checks each row size after sending to write. Ends document and starts new one after file part was written.
 * Sets up skip flags to skip some closing tags after new file part creation.<br>
 * If max file size was hit in middle of record, just creates new doc and proceeds as usual.<br>
 * If max file size was hit on the last tag of record, sets up skip flags, to skip /record_rows and /record tags.
 * These flags are set up back again after /record_row tag is encountered once more.
 * @throws XMLStreamException
 * @throws IOException
 * @throws InterruptedException 
 */
    private void writeRecordRow() throws XMLStreamException, IOException, InterruptedException {        
        writer.writeStartElement( "record_row" );
        writer.writeCharacters( tagRowBuffer.toString() );
        writer.writeEndElement();
        skipRecordClose = false;
        long prevSize = curPartSize;
        if ( curPartSize >= MAX_PART_SIZE ) {
            skipRecordClose = true;
            /*we reached record_row end tag and file size was exceeded
            close doc, write it, open doc and set skip </record_rows> and </record> tags*/             
            writer.writeEndElement();
            writer.writeEndElement();
            writeFooter( recRowCount, recCount );
            processedSize += chunkWriter.sendChunkToWrite( true, true );
            curPartSize = SIZE_CORRECTION;
            writeHeader();
            recRowCount = 0;
            recCount = 1;
        } else {//we reached record end but file size hasn't been exceeded            
            curPartSize += chunkWriter.sendChunkToWrite( false, false );
            processedSize += (curPartSize - prevSize);
        }        
        //System.out.println("ProcessedSize = "+processedSize +
        //        "; Current pSize= "+ curPartSize+ "; Percent = " +Percent.percent( processedSize, sourceFileSize ));
        statusQueue.offer( Percent.percent( processedSize, sourceFileSize ) );//report status
    }
/**
 * Writes start of the document.
 * @throws XMLStreamException 
 */
    private void writeHeader() throws XMLStreamException {
        writer.writeStartDocument( encoding, version );
        writer.writeStartElement( "record-table" );
        writer.writeStartElement( "record" );
        writeID();
        writer.writeStartElement( "record_rows" );
    }

    private void writeID() throws XMLStreamException {
        writer.writeStartElement( "record_id" );        
        writer.writeCharacters( tagIDBuffer.toString() );
        writer.writeEndElement();
    }

    /**
     * Writes footer with specified record count and closes the document.
     *
     * @param recRowCount
     * @param recCount
     * @throws XMLStreamException
     */
    private void writeFooter( long recRowCount, long recCount ) throws XMLStreamException {
        writer.writeStartElement( "footer" );
        writer.writeStartElement( "record_count" );
        writer.writeCharacters( Long.toString( recCount ) );
        writer.writeEndElement();
        writer.writeStartElement( "record_row_count" );
        writer.writeCharacters( Long.toString( recRowCount ) );
        writer.writeEndElement();
        writer.writeEndElement();
        writer.writeEndDocument();
        writer.flush();
    }
}
