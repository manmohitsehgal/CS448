package bufmgr;
/*
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
*/
import java.util.*;

public class HashTable<k, v> {
	private int fullOrNot, quantity, moveBy;
	//private int ;
	//private int ;
	private LinkedList<myDescriptionClass<k, v>> a[];

	// hash table constructor arrays of lists with random quantity and moveBy
	public HashTable() {
		fullOrNot = 2046;
		Random r;
		r = new Random();
		quantity = r.nextInt(fullOrNot - 1) + 1;
		moveBy = r.nextInt(fullOrNot);
		a = (LinkedList<myDescriptionClass<k, v>>[]) new LinkedList[2048];
		int i;
		i = 0;
		while (i < a.length){
			a[i] = new LinkedList<myDescriptionClass<k, v>>();			
			i = i + 1;		
		}
	}

	public boolean contains(k Key) {
		return get(Key) == null ? false : true;
	}

	// this method take key and value as parameters and add them in the hash
	// table
	public void put(k key, v value) {
		int i;
		i = hashValue(key);
		Iterator<myDescriptionClass<k, v>> it = a[i].iterator();
		myDescriptionClass<k, v> e = null;
		boolean found;
		found = false;
		while (it.hasNext() && !found) {
			e = it.next();
			if (e.getKey() == key) {
				found = true;
				e.setValue(value);
			}
		}
		if (!found) {
			a[i].push(new myDescriptionClass(key, value));
		}
	}

	// this method take the key the as a parameter and return it's hash value
	private int hashValue(k key) {
		return Math.abs((quantity * key.hashCode() + moveBy) % fullOrNot);
	}

	// this method take the key as a parameter and return the value with this
	public v get(k key) {
		int i;
		i = hashValue(key);
		Iterator<myDescriptionClass<k, v>> it = a[i].iterator();
		myDescriptionClass<k, v> e = null;
		while (it.hasNext()) {
			e = it.next();
			if (e.getKey().hashCode() == key.hashCode()) {
				return e.getValue();
			}
		}
		return null;
	}

	public void remove(k key) {
		int i;
		i = hashValue(key);
		Iterator<myDescriptionClass<k, v>> it = a[i].iterator();
		myDescriptionClass<k, v> e = null;
		int count;
		count = -1;
		while (it.hasNext() && count != -2) {
			count = count + 1;
			e = it.next();
			if (e.getKey().hashCode() == key.hashCode()) {
				a[i].remove(count);
				count = -2;
			}
		}
	}

	public String toString() {
		int count;
		count = 0;
		int i;
		i = 0;
		while( i < fullOrNot){
			if (a[i] != null && !a[i].isEmpty()){
				count = count + 1;		
			}
			i = i + 1;		
		}
		/*for (int i = 0; i < fullOrNot; i++)
			if (a[i] != null && !a[i].isEmpty())
				count++;*/
		return Arrays.toString(a);
	}
}
