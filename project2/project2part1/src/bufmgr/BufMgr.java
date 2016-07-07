/* ... */
/* Author : Manmohit Sehgal
   CS448 Project 2
*/

package bufmgr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import global.PageId;
import global.SystemDefs;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.FreePageException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.Page;
import diskmgr.PageUnpinnedException;



public class BufMgr{

	private Page[] bufferPool;
	private descriptions[] bufferDescription;
	private Queue<Integer> bufferQueue;
	private int numbufs;
	private HashTable<Integer, Integer> tableEntry;
  /**
   * Create the BufMgr object.
   * Allocate pages (frames) for the buffer pool in main memory and
   * make the buffer manage aware that the replacement policy is
   * specified by replacerArg (i.e. HL, Clock, LRU, MRU etc.).
   *
   * @param numbufs number of buffers in the buffer pool.
   * @param replacerArg name of the buffer replacement policy.
   */
  public BufMgr(int numbufs, String replacerArg) {

	this.numbufs = numbufs;
	bufferPool = new Page[numbufs];
	bufferDescription = new descriptions[numbufs];
	tableEntry = new HashTable<Integer, Integer>();

	bufferQueue = new LinkedList<Integer>();
	int i = 0;
		while( i< numbufs){
			bufferQueue.add(i);
			i = i + 1;
		}	
	}

	// method gets the empty frame
	public int getEmptyFrame() throws BufferPoolExceededException{
			try{
				if(bufferQueue.size() != 0){
					return bufferQueue.poll();
				}
			}
			catch(Exception e){
				throw new BufferPoolExceededException(null, "POOL_EXCEEDED");
			}
			return 0;
		}	

	// Check if the buffer is full 
	public boolean isFull(){
		return (bufferQueue.size() == 0);
	}

	// gets the frame number
	private int whatIsTheFrameNumber(PageId pgId) throws HashEntryNotFoundException{
		try{
			if(tableEntry.contains(pgId.pid)){
				return tableEntry.get(pgId.pid);		
			}
		}
		catch(Exception e){
			throw new HashEntryNotFoundException(null,"HASH_NOT_FOUND");		
		}
		return 0;
	}



  /**
   * Pin a page.
   * First check if this page is already in the buffer pool.
   * If it is, increment the pin_count and return a pointer to this
   * page.  If the pin_count was 0 before the call, the page was a
   * replacement candidate, but is no longer a candidate.
   * If the page is not in the pool, choose a frame (from the
   * set of replacement candidates) to hold this page, read the
   * page (using the appropriate method from {\em diskmgr} package) and pin it.
   * Also, must write out the old page in chosen frame if it is dirty
   * before reading new page.  (You can assume that emptyPage==false for
   * this assignment.)
   *
   * @param Page_Id_in_a_DB page number in the minibase.
   * @param page the pointer poit to the page.
   * @param emptyPage true (empty page); false (non-empty page)
   */
	// throws some exceptions make sure to catch that
  public void pinPage(PageId pin_pgid, Page page, boolean emptyPage) 	throws DiskMgrException,	BufferPoolExceededException, PagePinnedException, InvalidPageNumberException,FileIOException, IOException, HashEntryNotFoundException {

		boolean pageFound;
		pageFound = tableEntry.contains(pin_pgid.pid);
	
		if(!pageFound){
			int pIndex = -1;
			if(bufferQueue.size() != 0){
				pIndex = getEmptyFrame();
				if((bufferDescription[pIndex] != null) && (bufferDescription[pIndex].get_dirty_bit())){
					flushPage(bufferDescription[pIndex].get_page_number());
					tableEntry.remove(bufferDescription[pIndex].get_page_number().pid);
				}		
			}
			else{
				throw new BufferPoolExceededException(null, "THE_PIN_FAILED");	
			}
	
			Page pageTemp;
			pageTemp = new Page();

			try{
				SystemDefs.JavabaseDB.read_page(new PageId(pin_pgid.pid), pageTemp);
			}
			catch (Exception e){
				throw new DiskMgrException(e, "PINNING_A_PAGE_FAILED");
			}
	
			bufferPool[pIndex] = pageTemp;			
		
		
			page.setpage(bufferPool[pIndex].getpage());
			bufferDescription[pIndex] = new descriptions(1, new PageId(pin_pgid.pid), false);
			tableEntry.put(pin_pgid.pid, pIndex);		
		}

		else{
			int pageIndex = tableEntry.get(pin_pgid.pid);
			if(bufferDescription[pageIndex].get_pin_count()==0){
				bufferQueue.remove(pageIndex);
			}

			bufferDescription[pageIndex].set_pin_count(bufferDescription[pageIndex].get_pin_count() + 1);
			page.setpage(bufferPool[pageIndex].getpage());

		}

	}


  /**
   * Unpin a page specified by a pageId.
   * This method should be called with dirty==true if the client has
   * modified the page.  If so, this call should set the dirty bit
   * for this frame.  Further, if pin_count&gt;0, this method should
   * decrement it. If pin_count=0 before this call, throw an exception
   * to report error.  (For testing purposes, we ask you to throw
   * an exception named PageUnpinnedException in case of error.)
   *
   * @param globalPageId_in_a_DB page number in the minibase.
   * @param dirty the dirty bit of the frame
   */

// throws exceptions , need to put in the exceptions
  public void unpinPage(PageId PageId_in_a_DB, boolean dirty) throws PageUnpinnedException, HashEntryNotFoundException,
InvalidPageNumberException, FileIOException, IOException,DiskMgrException{


	if(!tableEntry.contains(PageId_in_a_DB.pid)){
		throw new HashEntryNotFoundException(null,"PAGE_UNPIN_FAILED_HASH_TABLE_ENTRY_NOT_FOUND");

	}
	else{
		int pIndex;
		pIndex = tableEntry.get(PageId_in_a_DB.pid);
		try{
			if(bufferDescription[pIndex].get_pin_count()!=0){
				bufferDescription[pIndex].set_dirty_bit(dirty);
				bufferDescription[pIndex].set_pin_count(bufferDescription[pIndex].get_pin_count()-1);
				if(bufferDescription[pIndex].get_pin_count()==0){
					bufferQueue.add(pIndex);			
				}		
			}
		}
		catch(Exception e){
			throw new PageUnpinnedException(null, "UNPID_FAILED");	
		}
	}
}


  /**
   * Allocate new pages.
   * Call DB object to allocate a run of new pages and
   * find a frame in the buffer pool for the first page
   * and pin it. (This call allows a client of the Buffer Manager
   * to allocate pages on disk.) If buffer is full, i.e., you
   * can't find a frame for the first page, ask DB to deallocate
   * all these pages, and return null.
   *
   * @param firstpage the address of the first page.
   * @param howmany total number of allocated new pages.
   *
   * @return the first page id of the new pages.  null, if error.
   */
	// throws exceptions, make sure to put it in there
  public PageId newPage(Page firstpage, int howmany) throws DiskMgrException,
		 FreePageException, BufferPoolExceededException,
		 PagePinnedException, InvalidPageNumberException, FileIOException,
		 HashEntryNotFoundException, IOException, InvalidRunSizeException {

	// check if the page is full

	PageId pageId;
	pageId = new PageId();
	if(isFull()){
		return null;
	}
	
	// try to allocate new pages
	try{
		SystemDefs.JavabaseDB.allocate_page(pageId, howmany);	
	}
	catch(Exception e){
		throw new DiskMgrException(e, "NEW_PAGE_FAILED");	
	}
	
	pinPage(pageId, firstpage, false);
	return pageId; 
}


  /**
   * This method should be called to delete a page that is on disk.
   * This routine must call the method in diskmgr package to
   * deallocate the page.
   *
   * @param globalPageId the page number in the data base.
   */
	// throws expection , to put in here
  public void freePage(PageId globalPageId) throws PagePinnedException,
InvalidRunSizeException, InvalidPageNumberException,
FileIOException, DiskMgrException, IOException  {

	if(tableEntry.contains(globalPageId.pid)){
		int frame;
		try{
			frame = whatIsTheFrameNumber(globalPageId);
			if(bufferDescription[frame].get_pin_count() > 1){
				throw new PagePinnedException(null, "FREEING_A_PAGE_FAILED");
			}
			
			if(bufferDescription[frame].get_pin_count() == 1){
				unpinPage(bufferDescription[frame].get_page_number(), bufferDescription[frame].get_dirty_bit());			
			}
			if(bufferDescription[frame].get_dirty_bit()){
				try{
					flushPage(globalPageId);
				}
				catch(Exception e){
					throw new FreePageException(null, "PAGE_FREE_FAIL");
				}			
			}
			tableEntry.remove(globalPageId.pid);
			bufferPool[frame] = null; bufferDescription[frame] = null;
			SystemDefs.JavabaseDB.deallocate_page(new PageId(globalPageId.pid));
		}
		catch(Exception e){
			throw new PagePinnedException(null, "FREE_PAGE_FAIL");	
		}	
	}
	else{
		SystemDefs.JavabaseDB.deallocate_page(new PageId(globalPageId.pid));	
	}
	
}


  /**
   * Used to flush a particular page of the buffer pool to disk.
   * This method calls the write_page method of the diskmgr package.
   *
   * @param pageid the page number in the database.
   */
//catch the exceptions thrown
  public void flushPage(PageId pageid) throws HashEntryNotFoundException,DiskMgrException {
	Page pageToFlush;
	pageToFlush = null;
	
	int frameNumber;
	frameNumber = whatIsTheFrameNumber(pageid);
	
	if(bufferPool[frameNumber] != null){
		pageToFlush = new Page(bufferPool[frameNumber].getpage().clone());	
	}
	try{
		if(pageToFlush != null){
			SystemDefs.JavabaseDB.write_page(pageid,pageToFlush);
			bufferDescription[frameNumber].set_dirty_bit(false);		
		}
		else{
			throw new HashEntryNotFoundException(null, "PAGE_NOT_FLUSHED_EXCEPTION");		
		}	
	}
	catch(Exception e){
		throw new DiskMgrException(e, "FLUSHING_PAGE_FAILED");	
	}
}

  
  /** Flushes all pages of the buffer pool to disk
   */
  public void flushAllPages() throws HashEntryNotFoundException, DiskMgrException {
	int i;
	i = 0;
		while(i < numbufs){
			if(bufferDescription[i] != null){
				flushPage(bufferDescription[i].get_page_number());		
			}
			i = i + 1;	
		}
	}


  /** Gets the total number of buffers.
   *
   * @return total number of buffer frames.
   */
  public int getNumBuffers() {
	return numbufs;
}


  /** Gets the total number of unpinned buffer frames.
   *
   * @return total number of unpinned buffer frames.
   */
  public int getNumUnpinnedBuffers() {
		return bufferQueue.size();
	}

}

