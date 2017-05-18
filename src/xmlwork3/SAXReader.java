package xmlwork3;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.*;
import javax.xml.stream.XMLStreamException;
import org.xml.sax.*;

public class SAXReader implements Runnable{

    private final File file;
    private final int filesize, cache;
    private final ArrayBlockingQueue<Object> arrayBlockQ;
    private final ArrayBlockingQueue<Long> statusQueue;
/**
 * 
 * @param file
 * @param filesize size of resulting files
 * @param cache cache for writing file, must be less than filesize
 * @param writerDataQueue
 * @param statusQueue 
 */
    public SAXReader( File file, int filesize, int cache,
            ArrayBlockingQueue<Object> writerDataQueue,
            ArrayBlockingQueue<Long> statusQueue ) {
        this.file = file;        
        this.arrayBlockQ = writerDataQueue;
        this.statusQueue = statusQueue;
        System.out.println("Filesize=" + filesize + "; Cache = " + cache);
        if ( ( filesize / 2l ) < cache || filesize < 64 ) {
            throw new IllegalArgumentException( "file part must be at least 2x bigger than cache size and must be larger than 64bytes" );
        }
        this.filesize = filesize;
        this.cache = cache;
    }

    @Override
    public void run() {
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            SAXParser sp = spf.newSAXParser();
            XMLReader reader = sp.getXMLReader();
            //reader.setErrorHandler( new MyErrorHandler(System.err) );
            reader.setContentHandler( new DataSaxHandler( file, filesize, cache, arrayBlockQ, statusQueue )); // need to implement this file
            reader.parse(new InputSource(new FileInputStream( file )));
        } catch ( ParserConfigurationException ex ) {
            Logger.getLogger( SAXReader.class.getName() ).log( Level.SEVERE, null, ex );
        } catch ( SAXException ex ) {
            Logger.getLogger( SAXReader.class.getName() ).log( Level.SEVERE, null, ex );
        } catch ( FileNotFoundException ex ) {
            Logger.getLogger( SAXReader.class.getName() ).log( Level.SEVERE, null, ex );
        } catch ( IOException ex ) {
            Logger.getLogger( SAXReader.class.getName() ).log( Level.SEVERE, null, ex );
        } catch ( XMLStreamException ex ) {
            Logger.getLogger( SAXReader.class.getName() ).log( Level.SEVERE, null, ex );
        }
    }
}
