package simpledb;

import static org.junit.Assert.fail;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    // Prefixes for the column names that will be given to the new TupleDesc
    public static final String[] ENCRYPTION_PREFIXES = new String[]{"PAILLIER_", "OPE_"};
    // Determines how many new columns the EncryptedFile will have per original column
    public static final int NUM_ENCRYPTIONS = ENCRYPTION_PREFIXES.length;
    // Suffix added to the end of the HeapFile's name when encrypted
    public static final String ENCRYPTION_SUFFIX = "_encrypted";
    // Name of the column with Paillier modulus
    public static final String PAILLIER_MODULUS = ENCRYPTION_PREFIXES[0] + "MODULUS";
    // Name of the column with Paillier G
    public static final String PAILLIER_G = ENCRYPTION_PREFIXES[0] + "G";
    // Bits integer used in creating keys. Should be the same for all files we encrypt
    public static final int BITS_INTEGER = Type.BIGINT_LEN;
    // Number of extra columns beyond encrypted columns needed to recreate keys
    // First one is for N, second one is for G
    public static final int NUM_EXTRA_COLUMNS = 2;
    
    private File file;
    private TupleDesc tupDesc;
    private PrivateKey privateKey = null;

    /**
     * FileTupleIterator implements DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        
    	private HeapFile heapFile;
        private Iterator<Tuple> heapPageIter;
        private TransactionId txnId;
        private boolean isOpen;
        private int pgIdx;
        
        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.heapFile = hf;
            this.heapPageIter = null;
            this.txnId = tid;
            this.isOpen = false;
            this.pgIdx = -1;
        }
        
        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open()
            throws DbException, TransactionAbortedException {
            this.isOpen = true;
        }
        
        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext()
            throws DbException, TransactionAbortedException {
            if (!this.isOpen) {
              return false;
            }
            if (heapPageIter != null) {
              if (heapPageIter.hasNext()) {
                return true;
              }
            }
            do {
              this.pgIdx++;
              if (this.pgIdx == this.heapFile.numPages()) {
                  return false;
              }
              BufferPool bufferPool = Database.getBufferPool();
              PageId pid = new HeapPageId(this.heapFile.getId(), this.pgIdx);
              HeapPage page = (HeapPage) bufferPool.getPage(this.txnId, pid, Permissions.READ_ONLY);
              this.heapPageIter = page.iterator();
            } while (!this.heapPageIter.hasNext());
            return true;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.hasNext()) {
              throw new NoSuchElementException("No more tuples in this file");
            }
            return this.heapPageIter.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
        	this.pgIdx = -1;
        	this.heapPageIter = null;
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.isOpen = false;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupDesc = td;
    }
    
    /**
     * Takes the contents of this file and applies Paillier and Order-Preserving
     * Encryption to all the fields. The resulting EncryptedFile will have two columns for
     * each column in the original file, one for the Paillier Encryption and one for the
     * Order-Preserving Encryption. The last column of the EncryptedFile will be 
     * the modulus n of the Paillier public key.
     * @return An EncryptedFile that contains the encrypted contents of this HeapFile
     */
    public EncryptedFile encrypt() throws IOException, DbException, 
                                          TransactionAbortedException {
        // TODO: Apply the relevant encryption schemes to each tuple in each page
        // essentially iterate through all the tuples and create a new encrypted table
        // that has twice the number of columns
        
        // Create a new TupleDescriptor that includes the new columns
        // +1 for the paillier modulus column and +1 for the Paillier g column
        int totalNumFields = tupDesc.numFields() * NUM_ENCRYPTIONS + NUM_EXTRA_COLUMNS;
        
        Type[] newTypes = new Type[totalNumFields];
        String[] newNames = new String[totalNumFields];
        int i = 0;
        
        // Create a new column for each encryption scheme
        for (TDItem td : tupDesc.getItems()) {
            for (int j=0; j < NUM_ENCRYPTIONS; j++) {
                newTypes[i] = td.getFieldType();
                newNames[i] = ENCRYPTION_PREFIXES[j] + td.getFieldName(); // TODO: Check for NPE
                i++;
            }
        }
        // Add one more column that has the public key values for paillier encryption
        // First extra column is N, second extra column is G
        int nColumn = totalNumFields - 2;
        int gColumn = totalNumFields - 1; // last column
        newTypes[nColumn] = Type.INT_TYPE;
        newNames[nColumn] = PAILLIER_MODULUS;
        newTypes[gColumn] = Type.INT_TYPE;
        newNames[gColumn] = PAILLIER_G;
        
        TupleDesc newTD = new TupleDesc(newTypes, newNames);

        // Create a new file that we're going to write to
        File newF = new File(this.file.getAbsolutePath() + ENCRYPTION_SUFFIX);

        // touch the file
        FileOutputStream fos = new FileOutputStream(newF);
        fos.write(new byte[0]);
        fos.close();

        EncryptedFile encF = new EncryptedFile(newF, newTD);
        Database.getCatalog().addTable(encF, UUID.randomUUID().toString());
        
        // write an empty page to the new file
        HeapPageId pid = new HeapPageId(encF.getId(), 0);
        HeapPage page = null;
        try {
            page = new HeapPage(pid, HeapPage.createEmptyPageData());
        } catch (IOException e) {
            // this should never happen for an empty page; bail;
            throw new RuntimeException("failed to create empty page in HeapFile");
        }
        
        encF.writePage(page);
        // Now that we have the new file to write to, iterate through each tuple in
        // the original heap file, apply the encryptions, and write to encFile
        HeapFileIterator hfi = (HeapFileIterator) this.iterator(null);
        hfi.open();
        
        // Create new KeyPair for the whole table
        // TODO: PUT encryption keys somehwere
        KeyPair keyPair;
        PublicKey publicKey;
        KeyPairBuilder keygen = new KeyPairBuilder();
        keygen.upperBound(BigInteger.valueOf(Integer.MAX_VALUE));
        keygen.bits(BITS_INTEGER);
        keyPair = keygen.generateKeyPair();
        publicKey = keyPair.getPublicKey();
        
        // TODO: This line is for testing only
        this.privateKey = keyPair.getPrivateKey();
        System.out.println("Public key: " + publicKey.toString());
        System.out.println("Private key: " + privateKey.toString());
        
        
        // TODO: Come up with convention for saving all the different private keys. Do we
        // want a file for each key, or one file with all the keys for the file
        saveKeyPair(keyPair, String.valueOf(getId()) + ".paillier");
        
        while (hfi.hasNext()) {
            Tuple originalTuple = hfi.next();
            Tuple encTuple = new Tuple(newTD); // Tuple to save encrypted data, has 2n+2 columns
            int originalNumFields = originalTuple.getTupleDesc().numFields();
            
            for(int j=0; j < originalNumFields; j++) {
                // Paillier Encryption
                Integer fieldValue = ((IntField) originalTuple.getField(j)).getValue();
                BigInteger plainData = BigInteger.valueOf((long) fieldValue);
                BigInteger encryptedData = publicKey.encrypt(plainData);
                IntField encryptedField = new IntField(encryptedData.intValueExact()); // TODO: Change
                encTuple.setField(j, encryptedField);
                System.out.println("Encrypted " + plainData + " to " + encryptedData);
            }

            for (int j = originalNumFields; j < originalNumFields*2; j++) {
                // TODO: OPE of tuple field   
                // For now, to test, just store the unencrypted values
                encTuple.setField(j, originalTuple.getField(j-originalNumFields));
            }
            
            // Save public key values
            IntField N = new IntField(publicKey.getN().intValueExact());
            encTuple.setField(nColumn, N);
            IntField G = new IntField(publicKey.getG().intValueExact());
            encTuple.setField(gColumn, G);
            
            // write tuple to file
            encF.insertTuple(null, encTuple);
        }
        
        return encF;
    }

    /**
     * Saves the private key used to encrypt this file
     * @param privateKey the PrivateKey that was used to encrypt this file
     * @param the name of the file to save the private key to
     */
    public static void saveKeyPair(KeyPair keypair, String filename) {
        try {
            ObjectOutputStream objectOutputStream = 
                    new ObjectOutputStream(new FileOutputStream(filename));
            objectOutputStream.writeObject(keypair);
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * FOR TESTING ONLY: Gets the private key associated with this file
     * @return
     */
    public PrivateKey getPrivateKey() {
        return this.privateKey;
    }
        
    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile pageRaf;
        try {
          pageRaf = new RandomAccessFile(this.file, "r");
        } catch (FileNotFoundException fnfExn) {
          throw new IllegalArgumentException("File not found");
        }
        int pageNum = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageNum * pageSize;
        byte pageData[] = new byte[pageSize];
        if (pageNum < 0 || pageNum > this.numPages()) {
          try {
            pageRaf.close();
          } catch (IOException ioExn) {
          }
          throw new IllegalArgumentException("This page does not exist");
        }
        try {
          pageRaf.seek(offset);
          pageRaf.read(pageData, 0, pageSize);
          pageRaf.close();
          return new HeapPage((HeapPageId)pid, pageData);
        } catch (IOException ioExn) {
          throw new IllegalArgumentException("Page does not exist in this file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int pageNum = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageNum * pageSize;
        byte[] pageData = page.getPageData();
        RandomAccessFile pageRaf = new RandomAccessFile(this.file, "rw");
        pageRaf.seek(offset);
        pageRaf.write(pageData);
        pageRaf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        return (int) Math.ceil(this.file.length() / (double) pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pagesAffected = new ArrayList<Page>();
        BufferPool bufferPool = Database.getBufferPool();
        HeapPage page;
        HeapPageId pid;
        boolean foundPage = false;
        int numPages = this.numPages();
        for (int i = 0; i < numPages; i++) {
          pid = new HeapPageId(this.getId(), i);
          try {
            page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            pagesAffected.add(page);
            foundPage = true;
            break;
          } catch (DbException dbExn) {
            bufferPool.releasePage(tid, pid);
          }
        }
        if (!foundPage) {
          pid = new HeapPageId(this.getId(), numPages);
          int pageSize = BufferPool.getPageSize();
          int offset = numPages * pageSize;
          byte[] pageData = new byte[pageSize];
          Arrays.fill(pageData, (byte) 0);
          RandomAccessFile pageRaf = new RandomAccessFile(this.file, "rw");
          pageRaf.seek(offset);
          pageRaf.write(pageData);
          pageRaf.close();
          page = (HeapPage) this.readPage(pid);
          page.insertTuple(t);
          page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
          page.insertTuple(t);
          pagesAffected.add(page);
        }
        return pagesAffected;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pagesAffected = new ArrayList<Page>();
        BufferPool bufferPool = Database.getBufferPool();
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        pagesAffected.add(page);
        return pagesAffected;
    }

    /**
     * @return the tuples in this file
     */
    public List<Tuple> getValidTuples(TransactionId tid) {
        ArrayList<Tuple> newTuples = new ArrayList<Tuple>();
        for (int i = 0; i < this.numPages(); i++) {
          PageId pid = new HeapPageId(this.getId(), i);
          BufferPool bufferPool = Database.getBufferPool();
          HeapPage page;
          try {
            page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
          } catch (TransactionAbortedException txnAbExn) {
            throw new NoSuchElementException("Invalid page access");
          } catch (DbException dbExn) {
            throw new NoSuchElementException("Invalid page access");
          }
          List<Tuple> tuples = page.getValidTuples();
          newTuples.addAll(tuples);
        }
        return newTuples;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}

