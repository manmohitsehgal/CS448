/*
Team :
	- Manmohit Sehgal
	- Justin Jaworski
*/

package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */

/*
 * TODO :
 * restart()
 * isOpen ()
 * close()
 * hasNext()
 * getNext()
 */



public class FileScan extends Iterator {

  /**
   * Constructs a file scan, given the schema and heap file.
   */
	private HeapFile	theGivenHeapFile; // heap_file
	private HeapScan	heapToScan; // heap_scan
	private RID			currentRIDToUse; // currRID

  public FileScan(Schema schema, HeapFile file) {
	    //throw new UnsupportedOperationException("Not implemented");
		setSchema(schema);
		theGivenHeapFile = file; // the heap file in use
		heapToScan		 = theGivenHeapFile.openScan(); // open the scanner to scan the heap file
		currentRIDToUse  = new RID();
				  
	}

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    //throw new UnsupportedOperationException("Not implemented");
		heapToScan.close();
		heapToScan = theGivenHeapFile.openScan();  
	}

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
		if(heapToScan != null){
			return false;	
		}
		else{
			return true;
		}
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    //throw new UnsupportedOperationException("Not implemented");
		heapToScan.close(); // closes the heap file and sets the RID to null 
	}

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
		if(heapToScan.hasNext()){
			return true;		
		}
		else{
			return false;		
		}
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    //throw new UnsupportedOperationException("Not implemented");
		byte[] heapData; 
		heapData = heapToScan.getNext(currentRIDToUse);
		Tuple newTuple = new Tuple(getSchema(), heapData);
		return newTuple;		
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    //throw new UnsupportedOperationException("Not implemented");
		
		return currentRIDToUse;
  }

} // public class FileScan extends Iterator
