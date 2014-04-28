package simpledb.buffer;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.file.*;

// CS4432 Project 1
// Robert Mullins
// Alfred Scott

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
/*CS4432 Project 1:
 * BasicBufferMgr has has variables and functions added.
 * - unpinned is an ArrayList used to store all buffers that are not currently pinned. It is used so that the entire buffer pool does not
 * have to be searched to find an un-pinned buffer. It is used within the LRU and clock replacement policies as well. This list is maintained
 * by the functions the involved the pinned and un-pinning of buffers.
 * - pinned is an ArrayList that stores all buffers that are currently pinned. This is maintained the same way as unpinned.
 * - BufferHash is a hashtable of used to located specific buffers in a set time instead of having to search the entire buffer pool.
 * - REPLACEMENT_POLICY is a variable that determines which policy (LRU or clock) is used to pick pin new buffers. Setting it to 1 calls
 * for LRU to be used and setting it to 2 calls for clock replacement to be used. It defaults to 1.
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private ArrayList<Buffer> unpinned = new ArrayList<Buffer>();
   private ArrayList<Buffer> pinned = new ArrayList<Buffer>();
   private HashMap<Block, Buffer> BufferHash = new HashMap<Block, Buffer>();
   private int REPLACEMENT_POLICY = 1;
   private int clockPolicyCounter = 0;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   /*CS4432 Project 1:
    * BasicBufferMgr now by default creates a Hashmap full of every buffer with their keys being their blocks.
    * Every new buffer created is initially not pinned, so unpinned is populated by all buffers at start
    */
   public BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      //pinned = null;
      
      for (int i=0; i<numbuffs; i++)
      {
         bufferpool[i] = new Buffer();
         BufferHash.put(bufferpool[i].block(), bufferpool[i]); // For every buffer added to the pool, it is also stored in the Hashmap
      	 unpinned.add(bufferpool[i]); // Every buffer added is also added to unpinned
      }
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   /* CS4432 Project 1:
    * This function now maintains the unpinned and pinned lists once an un-pinned buffer has been selected to for pinning.
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (buff.getPinCount() == 0) // If the chosen buffer was previously unpinned...
      {
    	  numAvailable--;
    	  unpinned.remove(buff); // ... it has to be removed from the unpinned list
    	  pinned.add(buff); // ... and then also added to the pinned list
      }
      buff.pin();
      printBufferPool();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   /* CS4432 Project 1:
    * This function now maintains the unpinned and pinned lists once a new buffer has been selected to for pinning.
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      unpinned.remove(buff); // It is a new buffer being pinned, so remove from unpinned
      pinned.add(buff); // and also add it to pinned
      buff.pin();
      System.out.print(this.toString());
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   /* CS4432 Project 1:
    * This function now maintains the unpinned list.
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (buff.getPinCount() == 0){
    	  numAvailable++;
    	  unpinned.add(buff);
    	  pinned.remove(buff);
      }
      
   }
   
   /* CS4432 Project 1:
    * The function now uses a hashtable to find a block, which is much more reliable time-wise than the previous method of searching
    * the entire buffer pool
    */
   private Buffer findExistingBuffer(Block blk) {
      return BufferHash.get(blk); // The hashtable allows us to avoid a for loop search through the entire buffer pool
      
      // Old code that has been replaced:
      /* 
       for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;*/
   }
  
   /* CS4432 Project 1:
    * chooseUpinnedBuffer has been modified to have the code for both LRU and clock policy replacement.
    * The policy to run is chosen by the current value of REPLACEMENT_POLICY.
    */
   private Buffer chooseUnpinnedBuffer(){
	   if (unpinned.isEmpty()) // Checks to see if unpinned is populated by any buffers
	   {
		   return null; // If it is empty, then there are no available buffers so return null
	   }
	   
	   Buffer retBuffer = null; // Creates a buffer that will eventually be returned
	   
	   if (REPLACEMENT_POLICY != 1 && REPLACEMENT_POLICY != 2) // REPLACEMENT_POLICY should only ever be 1 or 2 in this implementation
	   {
		   REPLACEMENT_POLICY = 1; // If neither LRU or clock replacement was selected, default back to LRU
	   }
	   
	   if (REPLACEMENT_POLICY == 2) // Checks to see if clock policy was chosen
	   {
		   Buffer buff = null; // Initializes a buffer so that comparisons can be made
		   int size = unpinned.size() - 1; // The size of unpinned is stored (the -1 is because lists start at 0) 
		   
		   if (clockPolicyCounter > size) // If the current counter is set larger than the size, reset it back to 0
		   {
			   clockPolicyCounter = 0; 
		   }
		   
		   retBuffer = unpinned.get(clockPolicyCounter); // retBuffer defaults to the last buffer pointed at by the counter
		  
		   while (clockPolicyCounter <= size) // Iterate the length of unpinned
		   {
			   buff = unpinned.get(clockPolicyCounter); // Store the current buffer in unpinned
			   if(buff.getRefBit() == 1) // Check if its reference bit is 1
			   {
				   buff.decrementRefBit(); // If so, decrement it to 0
			   }
			   
			   else // If it was no 1, then it was 0, so it can be replaced
			   {
				   retBuffer = buff; // The buffer to be returned is set to the current buffer being looked at
				   retBuffer.flush(); // Check to see if the dirtybit needs to be written
				   return retBuffer;
			   }
			   
			   clockPolicyCounter++; // If no buffers were found then increment counter and keep going
				   
			   if (clockPolicyCounter >= size) // If the counter is greater than/equal to the size, then wrap it around back to 0 
			   {
				   clockPolicyCounter = 0;
			   }
		   }
	   }
	   
	   if (REPLACEMENT_POLICY == 1) //Checks to see if the LRU policy was chosen
	   {
		   long currentLowTime = unpinned.get(0).getAccessTime(); // The lastAccessTime of the first element of unpinned is saved
		   retBuffer = unpinned.get(0); // The buffer to be returned defaults to the first element of unpinned
			   
		   for (Buffer buff : unpinned) // Iterate through the unpinned list
		   {
			   if(buff.getAccessTime() < currentLowTime) // If the lastAccessTime of the current buffer is lower than the current lowest time...
			   {
				   currentLowTime = buff.getAccessTime(); // ... then set the current lowest time and to that of the current buffer
				   retBuffer = buff; // and set retBuffer to the current buffer
			   }
		   }
		   retBuffer.flush(); // Check to see if the dirtybit needs to be written	   
		   return retBuffer; // When all of unpinned has been checked, return the buffer with lowest time
	   }
	   
	   return null;
	   
	   // Old code that has been replaced
	   /*
	   for (Buffer buff : bufferpool)
			if (!buff.isPinned())
			return buff;
			return null;
		*/
   }
   
   /* CS4432 Project 1:
    * This function returns the numAvailable field
    */
   public int available() {
	   return numAvailable;
   }
   

   /* CS4432 Project 1:
    * This function produces a string of information about the buffer pool including the number of available buffers, the size of the
    * unpinned ArrayList, the size of the pinned ArrayList, and the size of the buffer pool.
    */
   public String toString()
   {
	   String numberAvailable = "" + numAvailable + "";
	   String unPinnedSize = "" + unpinned.size() + "";
	   String pinnedSize = "" + pinned.size() + "";
	   String poolSize = "" + bufferpool.length + "";
	   String retString = "Bufferpool Size: " + poolSize + "\n" + "Number pinned: " + pinnedSize + "\n" +
			   			  "Number unpinned: " + unPinnedSize + "\n" + "Number Available: " + numberAvailable + "\n";
	   return retString;
   }
   
   public void printBufferPool()
   {
	   System.out.print(toString());
	   
	   for (int i = 0; i < bufferpool.length - 1; i++)
	   {   
		   System.out.print(bufferpool[i].toString() + "Block ID: " + i + "\n");
	   }
   }
}