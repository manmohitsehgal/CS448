/*
Team :
	- Manmohit Sehgal
	- Justin Jaworski
*/

package relop;
/**
* The selection operator specifies which tuples to retain under a condition; in
* Minibase, this condition is simply a set of independent predicates logically
* connected by OR operators.
*/
public class Selection extends Iterator {

	public Iterator iterator;
	public Predicate preds[];
	boolean truth;
	public boolean next;
	public Tuple nextT;
	int total = 0;
	boolean chek;

/**
* Constructs a selection, given the underlying iterator and predicates.
*/
	public Selection(Iterator iter, Predicate... preds) {
	// throw new UnsupportedOperationException("Not implemented");
		this.schema = iter.schema;
		this.iterator = iter;
		this.preds = preds;
		truth = true;
		next = false;
		//prepareNext();
		total = total + 1;
		chek = true;

		Tuple tuple;
		boolean val = false;
		if (iterator.hasNext()){
			tuple = iterator.getNext();
			//flag = checkPreds(tuple);
			int i = 0;
			for(i=0;i<preds.length;i++){
				if (preds[i].evaluate(tuple)){
					val = true;
				}
			}
			while((!val) && (iterator.hasNext())){
				tuple = iterator.getNext();
				//flag = checkPreds(tuple);
				i = 0;
				int s = 0;
				for(i=0;i<preds.length;i++){
					if (preds[i].evaluate(tuple)){
						total = total + 1;
						//chek = true;
						val = true;
						s = 1;
					}
				}
				if(s != 1){
					val = false;
				}
			}
			if (val == false){
				next = false;
				nextT = null;
			}
			else if(val == true){
				next = true;
				nextT = tuple;
			}
		}
		else{
			next = false;
			nextT = null;
		}
	}
	/**
	* Gives a one-line explaination of the iterator, repeats the call on any
	* child iterators, and increases the indent depth along the way.
	*/
	public void explain(int depth) {
	// throw new UnsupportedOperationException("Not implemented");
	}
	/**
	* Restarts the iterator, i.e. as if it were just constructed.
	*/
	public void restart() {
	// throw new UnsupportedOperationException("Not implemented");
		iterator.restart();
		truth=true;
		next=false;
		//prepareNext();

		Tuple tuple;
		boolean val = false;
		if (iterator.hasNext()){
			tuple = iterator.getNext();
			//flag = checkPreds(tuple);
			int i = 0;
			for(i=0;i<preds.length;i++){
				if (preds[i].evaluate(tuple)){
					val = true;
				}
			}
			while((!val) && (iterator.hasNext())){
				tuple = iterator.getNext();
				//flag = checkPreds(tuple);
				i = 0;
				int s = 0;
				for(i=0;i<preds.length;i++){
					if (preds[i].evaluate(tuple)){
						val = true;
						total = total + 1;
						//chek = true;
						s = 1;
					}
				}
				if(s != 1){
					val = false;
				}
			}
			if (val == false){
				next = false;
				nextT = null;
			}
			else if(val == true){
				next = true;
				nextT = tuple;
			}
		}
		else{
			next = false;
			nextT = null;
		}
	}
	/**
	* Returns true if the iterator is open; false otherwise.
	*/
	public boolean isOpen() {
	// throw new UnsupportedOperationException("Not implemented");
		if(truth = true){
			return true;
		}
		else if(truth = false){
			return false;
		}
		return truth;
	}
	/**
	* Closes the iterator, releasing any resources (i.e. pinned pages).
	*/
	public void close() {
	// throw new UnsupportedOperationException("Not implemented");
		if(truth != false){
			truth = false;
		}
		iterator.close();
		iterator = null;
		//preds = null;
	}
	/**
	* Returns true if there are more tuples, false otherwise.
	*/
	public boolean hasNext() {
	// throw new UnsupportedOperationException("Not implemented");
		return next;
	}
	/**
	* Gets the next tuple in the iteration.
	*
	* @throws IllegalStateException if no more tuples
	*/

	public Tuple getNext() {
	// throw new UnsupportedOperationException("Not implemented");
		Tuple ret;
		if (truth == true){
			if(next){
				ret=nextT;
				//prepareNext();
				Tuple tuple;
				boolean val = false;
				if (iterator.hasNext()){
					tuple = iterator.getNext();
					//flag = checkPreds(tuple);
					int i = 0;
					for(i=0;i<preds.length;i++){
						if (preds[i].evaluate(tuple)){
							total = total + 1;
							//chek = true;
							val = true;
						}
					}
					while((!val) && (iterator.hasNext())){
						tuple = iterator.getNext();
						//flag = checkPreds(tuple);
						i = 0;
						int s = 0;
						for(i=0;i<preds.length;i++){
							if (preds[i].evaluate(tuple)){
								val = true;
								s = 1;
							}
						}
						if(s != 1){
							val = false;
						}
					}
					if (val == false){
						next = false;
						nextT = null;
					}
					else if(val == true){
						next = true;
						nextT = tuple;
					}
				}
				else{
					next = false;
					nextT = null;
				}
				return ret;
			}
			else{
				throw new UnsupportedOperationException("Not has next");
			}
		}
		else{
			throw new UnsupportedOperationException("Not openning");
		}
	}

	public boolean checker(){
		if(iterator != null){
			total = total + 1;
			chek = true;
			return true;
		}
		if(next != false){
			total = total + 1;
			chek = true;
			return true;
		}
		if(nextT != null){
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


} // public class Selection extends Iterator
