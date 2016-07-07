package relop;
import global.*;
import heap.HeapFile;
import index.HashIndex;
import relop.*;


public class HashJoin extends Iterator {
	public static int heapFileId=0;
	public Iterator leftIterator, rightIterator;
	int o;
	public boolean isHasNext = false;
	public Tuple nextTuple, leftTuple, rightTuple;
	public boolean isFirstCall = true;
	public HeapFile leftHeapFile[];
	public HeapFile rightHeapFile[];
	public FileScan leftFileScan[];
	public FileScan rightFileScan[];
	public FileScan currentFileScan;
	public HashTableDup hashTableDup;
	public int rightColm, leftColm, ind, leftind, rightind;
	public Tuple numberOfTuplesLeft[];
	int total = 0;
	boolean chek;
	/**
	* Constructs a hash join, given the left and right iterators and which
	* columns to match (relative to their individual schemas).
	*/
	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
		// throw new UnsupportedOperationException("Not implemented");


		leftHeapFile=whichHeapFileToGet(left,lcol);
		rightHeapFile=whichHeapFileToGet(right,rcol);

		int i = 0;
		FileScan newFileScan1[]=new FileScan[2];
		FileScan newFileScan[]=new FileScan[2];

		while( i < 2){
			newFileScan[i]= new FileScan(left.schema,leftHeapFile[i]);
			chek = false;
			checker();
			i++;
		}

		i = 0;
		while( i < 2){
			newFileScan1[i]=new FileScan(right.schema,rightHeapFile[i]);
			chek = false;
			checker();
			i++;
		}
		//return newFileScan;
		leftFileScan = newFileScan;
		rightFileScan = newFileScan1;

		//return newFileScan;
		this.leftIterator=left;
		this.rightIterator=right;
		this.leftColm=lcol;
		this.rightColm=rcol;
		this.schema=Schema.join(leftIterator.schema,rightIterator.schema);

		o = 1;

		isHasNext=false;
		nextTuple=null;
		leftTuple=null;
		rightTuple=null;
		isFirstCall=true;
		nextInLine();
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

		leftIterator.restart();
		rightIterator.restart();
		int i = 0;
		while( i < 2){
			leftFileScan[i].close();
			rightFileScan[i].close();
			leftHeapFile[i].deleteFile();
			rightHeapFile[i].deleteFile();
			chek = false;
			checker();
			i++;
		}
		leftHeapFile=whichHeapFileToGet(leftIterator,leftColm);

		rightHeapFile=whichHeapFileToGet(rightIterator,rightColm);

		int j = 0;
		FileScan newFileScan1[]=new FileScan[2];
		FileScan newFileScan[]=new FileScan[2];

		while( j < 2){
			newFileScan[j]= new FileScan(leftIterator.schema,leftHeapFile[j]);
			chek = false;
			checker();
			j++;
		}

		j = 0;
		while( j < 2){
			newFileScan1[j]=new FileScan(rightIterator.schema,rightHeapFile[j]);
			chek = false;
			checker();
			j++;
		}
		//return newFileScan;
		leftFileScan = newFileScan;
		rightFileScan = newFileScan1;

		leftTuple=null;
		rightTuple=null;
		isFirstCall=true;
		isHasNext=false;
		nextTuple=null;

		nextInLine();
	}
	/**
	* Returns true if the iterator is open; false otherwise.
	*/
	public boolean isOpen() {
	// throw new UnsupportedOperationException("Not implemented");
		if(o == 1){
			chek = true;
			checker();
			return true;
		}
		else{
			chek = false;
			checker();
			return false;
		}
		//return true;
	}
	/**
	* Closes the iterator, releasing any resources (i.e. pinned pages).
	*/
	public void close() {
		// throw new UnsupportedOperationException("Not implemented");
		//openning=false;
		o = 0;
		int i = 0;
		while( i < 2){
			leftFileScan[i].close();
			rightFileScan[i].close();
			leftHeapFile[i].deleteFile();
			rightHeapFile[i].deleteFile();
			i++;
		}
		chek = false;
		checker();
		this.leftIterator.close();
		this.rightIterator.close();
		this.leftIterator=null;
		this.rightIterator=null;
	}
	/**
	* Returns true if there are more tuples, false otherwise.
	*/
	public boolean hasNext() {
	// throw new UnsupportedOperationException("Not implemented");
		if(isHasNext == true){
			chek = true;
			checker();
			isHasNext = true;
		}
		else if(isHasNext == false){
			chek = false;
			checker();
			isHasNext = false;
		}
		return isHasNext;
	}
	/**
	* Gets the next tuple in the iteration.
	*
	* @throws IllegalStateException if no more tuples
	*/
	public Tuple getNext() {

		Tuple toReturn;
		if(!(isOpen())){
			throw new UnsupportedOperationException("Not openning");
		}
		else{
			if(!(isHasNext)){
				throw new UnsupportedOperationException("Not has next");
			}
			else{
				toReturn=nextTuple;
				nextInLine();
				return toReturn;
			}		
		}
	}
	
	private void nextInLine(){
		boolean flag=false;
		Tuple tuple=null;
		Tuple tempTuple = null;
		SearchKey keyToSearchFor;

		if (isFirstCall){
			isFirstCall=false;
			hashTableDup=new HashTableDup();
			leftind=0;
			ind=0;
			numberOfTuplesLeft=new Tuple[0];
			while(leftFileScan[leftind].hasNext()){
				tempTuple=leftFileScan[leftind].getNext();
				int type = leftIterator.schema.fieldType(leftColm);

				if (type == AttrType.INTEGER){
					Integer IntegerField = tempTuple.getIntFld(leftColm);
					keyToSearchFor=new SearchKey(IntegerField);
				}
				else if(type == AttrType.FLOAT){
					Float floatField = tempTuple.getFloatFld(leftColm);
					keyToSearchFor=new SearchKey(floatField);
				}
				else if(type == AttrType.STRING){
					String stringField = tempTuple.getStringFld(leftColm);
					keyToSearchFor=new SearchKey(stringField);
				}
				else{
					throw new UnsupportedOperationException("Search key not defined");
				}
				

				hashTableDup.add(keyToSearchFor,tempTuple);
			}
			currentFileScan=rightFileScan[leftind];
			leftind++;
		}
		while((!flag) && ((leftind<2) || (currentFileScan.hasNext()) || (ind<numberOfTuplesLeft.length))){
			if (ind<numberOfTuplesLeft.length){
				leftTuple=numberOfTuplesLeft[ind];
				ind++;
				tuple=Tuple.join(leftTuple,rightTuple,this.schema);
				flag=true; 
			}
			while((!flag)&&(currentFileScan.hasNext())){
			rightTuple=currentFileScan.getNext();

			keyToSearchFor=retreiveSearchKey(rightTuple,rightIterator.schema,rightColm);

			numberOfTuplesLeft=hashTableDup.getAll(keyToSearchFor);
			if (numberOfTuplesLeft!=null){ 

					flag=true;
					ind=0;
					leftTuple=numberOfTuplesLeft[ind];

					ind++;
					tuple=Tuple.join(leftTuple,rightTuple,this.schema);
					flag=true; 
				}
			}
			if ((!currentFileScan.hasNext()) && (leftind<2)){
				hashTableDup=new HashTableDup();
				while(leftFileScan[leftind].hasNext()){
					tempTuple=leftFileScan[leftind].getNext();
					int type = leftIterator.schema.fieldType(leftColm);


				if (type == AttrType.INTEGER){
					Integer IntegerField = tempTuple.getIntFld(leftColm);
					keyToSearchFor=new SearchKey(IntegerField);
				}
				else if(type == AttrType.FLOAT){
					Float floatField = tempTuple.getFloatFld(leftColm);
					keyToSearchFor=new SearchKey(floatField);
				}
				else if(type == AttrType.STRING){
					String stringField = tempTuple.getStringFld(leftColm);
					keyToSearchFor=new SearchKey(stringField);
				}
				else{
					throw new UnsupportedOperationException("Search key not defined");
				}
				


					hashTableDup.add(keyToSearchFor,tempTuple);
				}
				currentFileScan=rightFileScan[leftind];
				leftind++;
				ind=0;
				numberOfTuplesLeft=new Tuple[0];
			}
		}
		if (flag){
			isHasNext=true;
			nextTuple=tuple;
		}
		else{
			isHasNext=false;
			nextTuple=null;
		}
	}


	private HeapFile[] whichHeapFileToGet(Iterator iter,int filedNumber){
		HeapFile theGivenHeapFile[]=new HeapFile[2];
		for(int i=0; i<2; i++){
			heapFileId=heapFileId+1;
			String tmpName=((Integer)heapFileId).toString();
			theGivenHeapFile[i]=new HeapFile(tmpName);
		}
		Tuple tuple=null;
		int key;
		int ind;
		chek = true;
		checker();
		while(iter.hasNext()){
			tuple = iter.getNext();
			key = tuple.getIntFld(filedNumber);
			ind = key % 2;
			theGivenHeapFile[ind].insertRecord(tuple.getData());
		}
		iter.restart();
		return theGivenHeapFile;
	}


	private SearchKey retreiveSearchKey(Tuple tuple, Schema schema, int filedNumber){
			int type = schema.fieldType(filedNumber);
			SearchKey keyToSearchFor=null;
		
			chek = true;
			checker();
			if (type == AttrType.INTEGER){
				Integer IntegerField = tuple.getIntFld(filedNumber);
				keyToSearchFor=new SearchKey(IntegerField);
			}
			else if(type == AttrType.FLOAT){
				Float floatField = tuple.getFloatFld(filedNumber);
				keyToSearchFor=new SearchKey(floatField);
			}
			else if(type == AttrType.STRING){
				String stringField = tuple.getStringFld(filedNumber);
				keyToSearchFor=new SearchKey(stringField);
			}
			else{
				throw new UnsupportedOperationException("Search key not defined");
			}
			return keyToSearchFor;
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
		if(currentFileScan != null){
			total = total + 1;
			chek = true;
			return true;
		}
		if(hashTableDup != null){
			total = total + 1;
			chek = true;
			return true;
		}
		return false;
	}
	
	

	public void printVal(){
		System.out.println(total);
		System.out.println(chek);
	}

}
