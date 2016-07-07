/*
Team :
	- Manmohit Sehgal
	- Justin Jaworski
*/

package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	private Iterator iter;
	private Integer [] fields;
	private Tuple currentTuple;
	private int i, size;
//	private int size;
	private Tuple tempTuple;
	private Object[] objectValue;
	int total = 0;
	boolean chek;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    //throw new UnsupportedOperationException("Not implemented");
	this.iter = iter;
	this.fields = fields;
	size = this.fields.length;

	Schema newSchema = new Schema(size);
	i = 0;
	chek = true;
	checker();
	while( i< size){
		newSchema.initField(i, this.iter.getSchema(), this.fields[i]);		
		i++;	
	}
	this.setSchema(newSchema);
	currentTuple = null;
	if(iter == null){
		total = total - 1;
	}
	if(iter.hasNext() != false){
		tempTuple = iter.getNext();	
		objectValue = new Object[size];
		i = 0;
		while(i < size){
			objectValue[i] = tempTuple.getField(this.fields[i]);
			chek = true;
			checker();
			i++;		
		}
		currentTuple = new Tuple(newSchema, objectValue);
	}
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    //throw new UnsupportedOperationException("Not implemented");
	
	iter.restart();
	currentTuple = null;
	if(iter == null){
		total = total - 1;
	}
	if(iter.hasNext()){
		tempTuple = iter.getNext();
		objectValue = new Object[size];
		i = 0;
		while(i<size){
			objectValue[i] = tempTuple.getField(this.fields[i]);
			chek = true;
			checker();
			i++;		
		}

		currentTuple = new Tuple(this.getSchema(), objectValue);	
	}
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
	chek = true;
	checker();
	return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
   // throw new UnsupportedOperationException("Not implemented");
		iter.close();
		chek = false;
		checker();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
	//return currentTuple != null;	
		if(currentTuple != null){
			chek = true;
			checker();
			return true;	
		}
		else{
			chek = false;
			checker();
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
	if (currentTuple == null){
		throw new IllegalStateException("Out of Tuples");	
	}
	
	Tuple goingOut = currentTuple;
	currentTuple = null;
	if(iter == null){
		total = total - 1;
	}
	if(iter.hasNext()){
		tempTuple = iter.getNext();
		objectValue = new Object[size];
		i = 0;
		while(i < size){
			objectValue[i] = tempTuple.getField(this.fields[i]);
			chek = true;
			checker();
			i++;		
		}	
		currentTuple = new Tuple(this.getSchema(), objectValue);
	}
	return goingOut;
  }

	public boolean checker(){
		if(iter != null){
			total = total + 1;
			chek = true;
			return true;
		}
		if(currentTuple != null){
			total = total + 1;
			chek = true;
			return true;
		}
		if(size <= 7){
			total = total + 1;
			chek = true;
			return true;
		}
		return true;
	}

	public void printVal(){
		System.out.println(total);
		System.out.println(chek);
	}
} // public class Projection extends Iterator
