package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

//CS4432 Project 1
//Robert Mullins
//Alfred Scott

/**
 * An individual buffer.
 * A buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log record.
 * @author Edward Sciore
 */
/* CS4432 Project 1:
 * The Buffer class has been given new variables and functions. 
 * - timeLastAccessed is a long that is to record the last time the buffer has been accessed and is used in for our LRU policy.
 * timeLastAccessed is set to the current time in every function call, other than "getLastAccessed", "getRefBit", 
 * "decrementRedBit", and "toString". getLastAccessed returns timeLastAccessed.
 * - refBit is a reference number used to function as the reference bit in the the clock replacement policy. It is returned via the getRefBit
 * function. It is set to 0 via the decrementRefBit function.
 * toString is a function that returns a string containing the state of the block (its number, if it is pinned or not, and its current
 * reference bit)
 */
public class Buffer {
   private Page contents = new Page();
   private Block blk = null;
   private int pins = 0;
   private int modifiedBy = -1;  // negative means not modified
   private int logSequenceNumber = -1; // negative means no corresponding log record
   private long timeLastAccessed = 0;
   private short refBit = 1; // set to 1 by default by clock replacment policy

   /**
    * Creates a new buffer, wrapping a new 
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link BasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    */
   public Buffer() {}
   
   /**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   public int getInt(int offset) {
	   timeLastAccessed = System.nanoTime();
      return contents.getInt(offset);
   }

   /**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   public String getString(int offset) {
	   timeLastAccessed = System.nanoTime();
      return contents.getString(offset);
   }

   /**
    * Writes an integer to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setInt(offset, val);
      timeLastAccessed = System.nanoTime();
   }

   /**
    * Writes a string to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setString(offset, val);
      timeLastAccessed = System.nanoTime();
   }

   /**
    * Returns a reference to the disk block
    * that the buffer is pinned to.
    * @return a reference to a disk block
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   public Block block() {
	   timeLastAccessed = System.nanoTime();
      return blk;
   }

   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   void flush() {
      if (modifiedBy >= 0) {
         SimpleDB.logMgr().flush(logSequenceNumber);
         contents.write(blk);
         modifiedBy = -1;
      }
      timeLastAccessed = System.nanoTime();
   }

   /**
    * Increases the buffer's pin count.
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    * refBit set to 1 because the block has been pinned.
    */
   void pin() {
	   timeLastAccessed = System.nanoTime();
	   refBit = 1;
      pins++;
   }

   /**
    * Decreases the buffer's pin count.
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   void unpin() {
	   timeLastAccessed = System.nanoTime();
      pins--;
   }

   /**
    * Returns true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
 //CS4432 Project 1
   boolean isPinned() {
	   timeLastAccessed = System.nanoTime();
      return pins > 0;
      
   }

   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    * Return the total number of pins instead of if the pin count is 0 or not.
    */
   int getPinCount() {
	   timeLastAccessed = System.nanoTime();
      return pins;
   }

   /**
    * Returns true if the buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the buffer
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   boolean isModifiedBy(int txnum) {
	   timeLastAccessed = System.nanoTime();
      return txnum == modifiedBy;
   }

   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;
      timeLastAccessed = System.nanoTime();
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
   /*CS4432 Project 1:
    * timeLastAccessed set to current time due to block being accessed.
    */
   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;
      timeLastAccessed = System.nanoTime();
   }

   /*CS4432 Project 1:
    * timeLastAccessed is returned in order to do comparisons for the LRU policy.
    */
   long getAccessTime()
   {
	   return timeLastAccessed;
   }

   /*CS4432 Project 1:
    * refBit is returned for in order to perform the clock replacement policy.
    */
   short getRefBit()
   {
	   return refBit;
   }

   /*CS4432 Project 1:
    * The redBit is set to 0.
    * Used for clock replacement. 
    */
   void decrementRefBit()
   {
	   refBit = 0;
   }
   
   /*CS4432 Project 1:
    * Returns a string that contains information regarding the block number, pin status, and refBit.
    */
   public String toString()
   {//fuck with thus
	   String retString;
	   
	   if(blk != null)
	   {
		   String ID = null;
		   String block = "" + blk.number() + "";
		   String isPinned = "" + this.getPinCount() + "";
		   String refBitString = "" + refBit + "";
		   retString = "Block Number: " + block + " " + "Number Pinned: " + isPinned + " " + 
				   		"Reference bit: " + refBitString + " Time Last Accessed: " + timeLastAccessed + " ";
		   return retString;
	   }
	  
	   else
	   {
		   retString = "No info on block\n";
		   return retString;
	   }
   }
}