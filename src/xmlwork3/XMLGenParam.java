package xmlwork3;
/**
 * XML parameters for XML generator class.
 * @author Zmr
 */
public class XMLGenParam {
    final long numRecords;
    final int maxStrLength;
    final int maxRecRowNum;
    final int startFrom;
    final boolean isRandom;
    final int cacheSize;
    final int rowWriteInc;
    final String encoding;
    final String namespace;
    final String version;
/**
 * XML parameters for XML generator class.
 * @param numRecords number of record fields.
 * @param maxStrLength maximum length of random data string inside each record row.
 * @param maxRecRowNum max number of record rows inside each record.
 * @param startFrom number to start counting record ID from.
 * @param isRandom if true, all values are random within 1..maxVal interval.
 * @param cacheSize the amount of memory, the document is allowed to occupy, before it will be written to disc
 * @param rowWriteInc number of rows to be generated before sending them to XMLWriter
 */
    public XMLGenParam( long numRecords, int maxStrLength, int maxRecRowNum,
            int startFrom, boolean isRandom, int cacheSize, int rowWriteInc ) {
        this.numRecords = numRecords;
        this.maxStrLength = maxStrLength;
        this.maxRecRowNum = maxRecRowNum;
        this.startFrom = startFrom;
        this.isRandom = isRandom;
        this.cacheSize = cacheSize;
        this.rowWriteInc = rowWriteInc;
        this.encoding = "UTF-8";
        this.namespace = "http://www.w3.org/2001/XMLSchema";
        this.version = "1.0";
    }
 /**
 * XML parameters for XML generator class. Shortened constructor.<br>
 * Max data String length 200.<br>
 * Starts from 1.<br>
 * Uses random.<br>
 * 500 rows generated before write.
 * @param numRecords number of record fields.
 * @param maxRecRowNum max number of record rows inside each record.
 * @param cacheSize the amount of memory, the document is allowed to occupy, before it will be written to disc
 */
    public XMLGenParam( long numRecords, int maxRecRowNum, int cacheSize ) {
        this.numRecords = numRecords;
        this.maxStrLength = 200;
        this.maxRecRowNum = maxRecRowNum;
        this.startFrom = 1;
        this.isRandom = true;
        this.cacheSize = cacheSize;
        this.rowWriteInc = 500;//less seems to be a bit slower, small values are very slow.
        this.encoding = "UTF-8";
        this.namespace = "http://www.w3.org/2001/XMLSchema";
        this.version = "1.0";
    }
    /**
     * Develop time constructor.
     */
    public XMLGenParam(  ) {
        this.numRecords = 5000;
        this.maxStrLength = 200;
        this.maxRecRowNum = 30;
        this.startFrom = 1;
        this.isRandom = true;
        this.cacheSize = 1024*32;
        this.rowWriteInc = 500;//less seems to be a bit slower, small values are very slow.
        this.encoding = "UTF-8";
        this.namespace = "http://www.w3.org/2001/XMLSchema";
        this.version = "1.0";
    }
}
