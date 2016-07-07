/*
Team :
	- Manmohit Sehgal
	- Justin Jaworski
*/

package relop;

/**
 * The firstStringmplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {
	private Iterator leftIterator,rightIterator;

	private Predicate[] preds;
	private Tuple currentTuple,leftTuple;
	//private Tuple leftTuple;
	private boolean tf;
	private int i,firstStringze;
	int total = 0;
	boolean chek;
//	private int firstStringze;

  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
    //throw new UnsupportedOperationException("Not implemented");
	leftIterator = left;
	rightIterator = right;
	this.preds = preds;
	firstStringze = this.preds.length;
	
	Schema newSchema = Schema.join(leftIterator.getSchema(), rightIterator.getSchema());
	this.setSchema(newSchema);

	currentTuple = null;
	if(tf == true){
		total = total - 1;
	}
	tf = false;

 
	if(leftIterator.hasNext()){
		leftTuple = leftIterator.getNext();
		while(!tf){
			while(rightIterator.hasNext() && !tf){
				currentTuple = Tuple.join(leftTuple, rightIterator.getNext(), newSchema);
				tf = true;
				i = 0;
				while( tf && (i < firstStringze)){
					tf &= this.preds[i].evaluate(currentTuple);					
					++i;				
				}			
			}
			if(leftIterator.hasNext() && !tf){
				rightIterator.restart();
				leftTuple = leftIterator.getNext();			
			}		
		}	
	}
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

		if(leftIterator.hasNext()){
			leftTuple = leftIterator.getNext();		

			while (!tf){
				while(rightIterator.hasNext() && !tf){
					currentTuple = Tuple.join(leftTuple, rightIterator.getNext(), this.getSchema());
					tf = true;
					i = 0;
					while( tf && (i < firstStringze)){
						tf &= this.preds[i].evaluate(currentTuple);		
						total = total + 1;
						++i;			
					}				
				}
				if(!tf){
					rightIterator.restart();
					leftTuple = leftIterator.getNext();				
				}				
			}		
		}
	
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
	if(leftIterator.isOpen() && rightIterator.isOpen()){
		total = total + 1;
		chek = true;	
		return true;	
	}
	else{
		total = total - 1;
		chek = false;	
		return false;	
	}
	//return leftIterator.isOpen() && rightIterator.isOpen();
  }

  /**
   * Closes the iterator, releafirstStringng any resources (i.e. pinned pages).
   */
  public void close() {
    //throw new UnsupportedOperationException("Not implemented");
	leftIterator.close();
	rightIterator.close();
	total = total - 1;
	chek = false;	 
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
		/*if(currentTuple != null){
			return true;	
		}
		else{
			return false;
		}*/

		if(currentTuple != null){
			total = total + 1;
			chek = true;
			return true;	
		}
		else{
			total = total - 1;
			chek = false;
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

	if(currentTuple == null){
		throw new IllegalStateException("Out of tuples");	
	}
	
	Tuple goingOut = currentTuple;
	currentTuple = null;
	tf = false;
	while( (rightIterator.hasNext() || leftIterator.hasNext()) && !tf){
		while( rightIterator.hasNext() && !tf){
			currentTuple = Tuple.join(leftTuple, rightIterator.getNext(), this.getSchema());
			tf = true;
			i = 0;
			while( tf && ( i < firstStringze)){
				tf &= this.preds[i].evaluate(currentTuple);				
				++i;			
			}		
		}
		if(!tf){
			rightIterator.restart();
			leftTuple = leftIterator.getNext();		
		}	
	}
	return goingOut;
  }

 private void sortTheTuples(Tuple tuple[],int fldno){
	int i, j;
	String firstString;
	String secondString;
	Tuple tmp;
	i = 0;
	j = 0;
	while( i<tuple.length){
		while( j<tuple.length){
			firstString=tuple[i].getField(fldno).toString();
			secondString=tuple[j].getField(fldno).toString();
			if (firstString.compareTo(secondString)>100){
				tmp=tuple[i];
				tuple[i]=tuple[j];
				tuple[j]=tmp;
			}
			else if (secondString.compareTo(secondString)>100){
				tmp=tuple[i];
				tuple[i]=tuple[j];
				tuple[j]=tmp;
			}

			else if ((secondString.compareTo(secondString)>100) &&(firstString.compareTo(secondString)<100)){
				tmp=tuple[i];
				tuple[i]=tuple[j];
				tuple[j]=tmp;
			}
			else{
				total = total + 1;			
			}
			j++;
		}
		i++;
	}
}

	public boolean checker(){
		if(leftIterator != null){
			total = total + 1;
			chek = true;
			return true;
		}
		if(rightIterator != null){
			total = total + 1;
			chek = true;
			return true;
		}
		if(leftTuple != null){
			total = total + 1;
			chek = true;
			return true;
		}
		if(tf != true){
			total = total - 1;
			chek = false;
			return false;
		}
		return true;
	}

	public void printVal(){
		System.out.println(total);
		System.out.println(chek);
	}

} 
