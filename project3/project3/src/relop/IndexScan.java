/*
Team :
	- Manmohit Sehgal
	- Justin Jaworski
*/

package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.BucketScan;
import global.RID;

/**
 * Wrapper for bucket scan, an index access method.
 */

/*
 * TODO :
 * restart()
 * isOpen ()
 * close()
 * hasNext()
 * getNext()
 */




public class IndexScan extends Iterator {

  /**
   * Constructs an index scan, given the hash index and schema.
   */

	private HashIndex	indexToHash;
	private HeapFile	theGivenHeapFile;
	private BucketScan	bucketToScan;
	

  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
    //throw new UnsupportedOperationException("Not implemented");
	setSchema(schema);
	indexToHash = index;
	theGivenHeapFile = file;
	bucketToScan = indexToHash.openScan();
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
	bucketToScan.close();
	bucketToScan = indexToHash.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
	
		if(bucketToScan != null){
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
	bucketToScan.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");

		if(bucketToScan.hasNext()){
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
	byte[] data = theGivenHeapFile.selectRecord(bucketToScan.getNext());
	Tuple newTuple = new Tuple(getSchema(), data);
	return newTuple;

  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {

	return bucketToScan.getLastKey();
 
}

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
   // throw new UnsupportedOperationException("Not implemented");
	return bucketToScan.getNextHash();

  }

} // public class IndexScan extends Iterator
