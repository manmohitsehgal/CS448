/*
Team :
	- Manmohit Sehgal
	- Justin Jaworski
*/

package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */

/*
 * TODO :
 * restart()
 * isOpen ()
 * close()
 * hasNext()
 * getNext()
 */




public class KeyScan extends Iterator {

  /**
   * Constructs an index scan, given the hash index and schema.
   */

	private HashIndex	indexToHash;
	private SearchKey	keyToSearch;
	private HeapFile	theGivenHeapFile;
	private HashScan	hashToScan;	

  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
    //throw new UnsupportedOperationException("Not implemented");
	
	setSchema(schema);	
	indexToHash 	 = index;
	keyToSearch 	 = key;
	theGivenHeapFile = file;
	hashToScan		 = indexToHash.openScan(keyToSearch); 
	
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
		hashToScan.close(); // close the existing 
		hashToScan = indexToHash.openScan(keyToSearch);   
	}

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
 	//return hashToScan != null; 

		if(hashToScan != null){
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
	hashToScan.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
		if(hashToScan.hasNext()){
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
	
	byte [] keyScanData;
	keyScanData = theGivenHeapFile.selectRecord(hashToScan.getNext());
	Tuple newTuple = new Tuple(getSchema(), keyScanData);
	return newTuple;

  }

} // public class KeyScan extends Iterator
