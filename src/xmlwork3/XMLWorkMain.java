package xmlwork3;

import java.awt.Color;
import static java.awt.Color.WHITE;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.swing.JFileChooser;

public class XMLWorkMain extends javax.swing.JFrame {
    ArrayBlockingQueue<Object> writerDataQueue;
    ArrayBlockingQueue<Long> statusQueue;
    private File file;
    private int filePartSize;//bytes
    int cache = 512*1;// CACHE MUST BE LESS THAN SPLIT FILE SIZE
    private File workDir;
    private static final Pattern sizePatt = Pattern.compile("[0-9]{1,}");
    private static final Color PURPLE = new Color( 1, 0.502f, 0.502f );
    long recordQty;

    /**
     * Creates new form XMLWorkMain
     */
    public XMLWorkMain() {
        initComponents();
        System.out.println("Filesize = " + filePartSize);
        jProgressBar1.setMinimum( 0 );
        jProgressBar1.setMaximum( 100 );
    }
    private void showSourceFileChooser(){
        final JFileChooser fc = new JFileChooser();
        int retVal;

        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
        //FileNameExtensionFilter filter = new FileNameExtensionFilter("xml files " + EXTENSION, EXTENSION);
        //fc.setFileFilter(filter);
        retVal = fc.showSaveDialog( jPanel1 );//.showDialog(null, "Select File");
        if ( retVal == JFileChooser.APPROVE_OPTION )
        {
            addressSource.setText( fc.getSelectedFile().getAbsolutePath() );
            file =  fc.getSelectedFile();
        }
    }
    private void showPartFileChooser() {
        final JFileChooser fc = new JFileChooser();
        int retVal;

        fc.setFileSelectionMode( JFileChooser.DIRECTORIES_ONLY );
        //FileNameExtensionFilter filter = new FileNameExtensionFilter("xml files " + EXTENSION, EXTENSION);
        //fc.setFileFilter(filter);
        retVal = fc.showSaveDialog( jPanel1 );//.showDialog(null, "Select File");
        if ( retVal == JFileChooser.APPROVE_OPTION ) {            
            addressParts.setText( fc.getSelectedFile().getAbsolutePath() );
            workDir = fc.getSelectedFile();//.getAbsolutePath();
        }
    }
    /**
     * Checks splitted file size field and parses it to variable.
     */
    private boolean chkSplitFields()
    {
        boolean isOK = true;
        if( addressParts.getText().isEmpty() ){
            addressParts.setBackground( PURPLE );
            isOK = false;
        }
        if( addressSource.getText().isEmpty() ){
            addressSource.setBackground( PURPLE );
            isOK = false;
        }
        if(!isOK) return false;
        String chSize = chunkSize.getText();
        if( chkString(chSize, sizePatt ) ){//chk if Nchunks filled correctly
            filePartSize = Integer.parseInt( chSize );
            filePartSize *= 1024;//convert from KB to B
            if( filePartSize < 1 || filePartSize>Integer.MAX_VALUE ){                
                chunkSize.setBackground( PURPLE );
                isOK = false;
            }
        }
        else{
            chunkSize.setBackground( PURPLE );
            isOK = false;
        }
        if( !file.exists() ){ //source file does not exist
            addressSource.setBackground( PURPLE );
            isOK = false;
        }
        if ( isOK ) {
            split.setEnabled( true );
            chunkSize.setBackground( WHITE );
            addressParts.setBackground( WHITE );
            addressSource.setBackground( WHITE );            
        }
        return isOK;
    }
    /**
     * Checks numbers of records field and parses it to variable.
     */
    private boolean chkGenerateFields() {        
        boolean isOK = false;
        if( addressSource.getText().isEmpty() ){
            addressSource.setBackground( PURPLE );
            return false;
        }
        String nRec = nRecords.getText();        
        if ( chkString( nRec, sizePatt ) ) {
            recordQty = Long.parseLong( nRec );
            if ( recordQty > Long.MAX_VALUE || recordQty < 1 ) {
                generate.setEnabled( false );
                nRecords.setBackground( PURPLE );                
            } else {
                generate.setEnabled( true );
                nRecords.setBackground( WHITE );
                addressSource.setBackground( WHITE );
                isOK = true;
            }
        } else {
            nRecords.setBackground( PURPLE );
            generate.setEnabled( false );
        }
        return isOK;
    }
    /**checks String(field) against specified regex
     * @param toCheck
     * @param pattern
     * @return 
     */
    private static boolean chkString( String toCheck, Pattern pattern ){
        Matcher matcher = pattern.matcher( toCheck );
        return matcher.matches();
    }
    private void generate() {
        writerDataQueue = new ArrayBlockingQueue<Object>(1,true);
        statusQueue = new ArrayBlockingQueue<Long>(10,true);
        XMLGenParam xmlGenPar = new XMLGenParam(recordQty, 30, cache);
        
        Writer writer;
        try {
            writer = new Writer( writerDataQueue, file, false );//create writer in single file mode
        } catch ( FileNotFoundException ex ) {
            Logger.getLogger( XMLWorkMain.class.getName() ).log( Level.SEVERE, null, ex );
            return;
        }
        Thread writerTh = new Thread( writer );
        XMLgenerator xmlGenerator = new XMLgenerator( writerDataQueue, statusQueue, xmlGenPar );
        Thread generatorTh = new Thread( xmlGenerator );
        statusUpdate();
        writerTh.start();
        generatorTh.start();
    }    
    private void split(){
        final Writer writer;
        final SAXReader saxReader;
        writerDataQueue = new ArrayBlockingQueue<Object>(1,true);
        statusQueue = new ArrayBlockingQueue<Long>(10,true);
        
        try {
            //create SAX, pass parameters into it, launch content handler thread.
            saxReader = new SAXReader(file, filePartSize, cache, writerDataQueue, statusQueue);
            Thread saxTh = new Thread( saxReader );
            statusUpdate();
            saxTh.start();
            
            //start separate thread for writing processed data to file
            writer = new Writer( writerDataQueue, workDir, true );//create writer in multi file mode
            Thread writerTh = new Thread( writer );
            writerTh.start();
            //readSplit.read( fileToSplit, filePartSize, writerDataQueue );
        } catch ( IOException ex ) {
            Logger.getLogger( XMLWorkMain.class.getName() ).log( Level.SEVERE, null, ex );
        }
    }
    /**
     * Check for status updates and put them to the Progressbar value.
     * Polls blocking queue, when 100 is pulled out, exits.
     */
    private void statusUpdate(){
        new Thread()
        {
            @Override
            public void run() {
                int progVal = 0;
                try {
                    while ( true ) {
                        progVal = statusQueue.take().intValue();
                        //System.out.println(progVal);
                        jProgressBar1.setValue( progVal );
                        if ( progVal == 100l ) {
                            return;
                        }
                    }
                } catch ( InterruptedException ex ) {
                    Logger.getLogger( XMLWorkMain.class.getName() ).log( Level.SEVERE, null, ex );
                }
            }
        }.start();
    }
    
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jPanel1 = new javax.swing.JPanel();
        generate = new javax.swing.JButton();
        nRecords = new javax.swing.JTextField();
        addressSource = new javax.swing.JTextField();
        browse = new javax.swing.JButton();
        split = new javax.swing.JButton();
        chunkSize = new javax.swing.JTextField();
        jLabel2 = new javax.swing.JLabel();
        jLabel3 = new javax.swing.JLabel();
        addressParts = new javax.swing.JTextField();
        partBrowse = new javax.swing.JButton();
        jProgressBar1 = new javax.swing.JProgressBar();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        generate.setText("Generate");
        generate.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                generateActionPerformed(evt);
            }
        });

        nRecords.setText("20");

        addressSource.setEditable(false);
        addressSource.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                addressSourceActionPerformed(evt);
            }
        });

        browse.setText("Browse");
        browse.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                browseActionPerformed(evt);
            }
        });

        split.setText("Split");
        split.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                splitActionPerformed(evt);
            }
        });

        chunkSize.setText("10");

        jLabel2.setText("Split file to parts size (KB)");

        jLabel3.setText("No of XML 'record' tags");

        addressParts.setEditable(false);

        partBrowse.setText("Browse");
        partBrowse.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                partBrowseActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(addressSource)
                            .addGroup(jPanel1Layout.createSequentialGroup()
                                .addComponent(jLabel3, javax.swing.GroupLayout.DEFAULT_SIZE, 194, Short.MAX_VALUE)
                                .addGap(20, 20, 20)
                                .addComponent(nRecords, javax.swing.GroupLayout.PREFERRED_SIZE, 45, javax.swing.GroupLayout.PREFERRED_SIZE)))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(generate, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(browse, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(jPanel1Layout.createSequentialGroup()
                                .addComponent(jLabel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(chunkSize, javax.swing.GroupLayout.PREFERRED_SIZE, 42, javax.swing.GroupLayout.PREFERRED_SIZE))
                            .addComponent(addressParts))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(split, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(partBrowse, javax.swing.GroupLayout.DEFAULT_SIZE, 82, Short.MAX_VALUE))))
                .addContainerGap())
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(addressSource, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(browse))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(nRecords, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(generate)
                    .addComponent(jLabel3))
                .addGap(18, 18, 18)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(addressParts, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(partBrowse))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(split)
                    .addComponent(chunkSize, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel2))
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jProgressBar1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(jProgressBar1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void generateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_generateActionPerformed
        if ( chkGenerateFields() ) {
            generate();
        }
    }//GEN-LAST:event_generateActionPerformed

    private void splitActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_splitActionPerformed
        if( chkSplitFields() ){
            split();
        }
    }//GEN-LAST:event_splitActionPerformed

    private void browseActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_browseActionPerformed
       showSourceFileChooser();       
       addressSource.setBackground( WHITE );
    }//GEN-LAST:event_browseActionPerformed

    private void addressSourceActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_addressSourceActionPerformed
    }//GEN-LAST:event_addressSourceActionPerformed

    private void partBrowseActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_partBrowseActionPerformed
        showPartFileChooser();
        addressParts.setBackground( WHITE );
    }//GEN-LAST:event_partBrowseActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main( String args[] ) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for ( javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels() ) {
                if ( "Nimbus".equals( info.getName() ) ) {
                    javax.swing.UIManager.setLookAndFeel( info.getClassName() );
                    break;
                }
            }
        } catch ( ClassNotFoundException ex ) {
            java.util.logging.Logger.getLogger( XMLWorkMain.class.getName() ).log( java.util.logging.Level.SEVERE, null, ex );
        } catch ( InstantiationException ex ) {
            java.util.logging.Logger.getLogger( XMLWorkMain.class.getName() ).log( java.util.logging.Level.SEVERE, null, ex );
        } catch ( IllegalAccessException ex ) {
            java.util.logging.Logger.getLogger( XMLWorkMain.class.getName() ).log( java.util.logging.Level.SEVERE, null, ex );
        } catch ( javax.swing.UnsupportedLookAndFeelException ex ) {
            java.util.logging.Logger.getLogger( XMLWorkMain.class.getName() ).log( java.util.logging.Level.SEVERE, null, ex );
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater( new Runnable() {
            public void run() {
                new XMLWorkMain().setVisible( true );
            }
        } );
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JTextField addressParts;
    private javax.swing.JTextField addressSource;
    private javax.swing.JButton browse;
    private javax.swing.JTextField chunkSize;
    private javax.swing.JButton generate;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JProgressBar jProgressBar1;
    private javax.swing.JTextField nRecords;
    private javax.swing.JButton partBrowse;
    private javax.swing.JButton split;
    // End of variables declaration//GEN-END:variables
}
