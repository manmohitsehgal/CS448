package heap;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;


public class Heapfile implements GlobalConst {

  PageId mainDirectoryPageId;
  private String fname;
  private int hfcount = 0;
  int t = 2;
  boolean truth = false;
  int pag = 1;
  int tcount = 0;
  
  public Heapfile(String name) throws HFException,IOException{
	t = 1;
	tcount = tcount + count();
	if(tcount <= 0){
	}
	else{
	}
	if(name == null) {
	    fname = "HeapFile";
	    String filenum = Integer.toString(hfcount);
	    fname = fname + filenum; 
	    hfcount = hfcount + 1;
	}
	else{
	    fname = name;
	    t = 0;
	}
	Page page;
	page = new Page();
	if (t != 2){
		PageId tmpId;
		tmpId = new PageId();
		try {
	    mainDirectoryPageId = SystemDefs.JavabaseDB.get_file_entry(fname);
		}
		catch (Exception e) {
		}
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	if(mainDirectoryPageId==null){
		try {
	    mainDirectoryPageId = SystemDefs.JavabaseBM.newPage(page,pag);
		}
		catch (Exception e) {
		}
	    if(mainDirectoryPageId == null){
		throw new HFException(null, "unable to make page");
	    }
	    try {
		SystemDefs.JavabaseDB.add_file_entry(fname,mainDirectoryPageId);
	    }
	    catch (Exception e) {
	    }
	    HFPage firstDirPage = new HFPage();
	    firstDirPage.init(mainDirectoryPageId, page);
	    PageId pageId = new PageId(INVALID_PAGE);
	    firstDirPage.setNextPage(pageId);
	    firstDirPage.setPrevPage(pageId);
	    truth = true;
	    try {
		SystemDefs.JavabaseBM.unpinPage(mainDirectoryPageId, truth);
	    }
	    catch (Exception e){
	    }
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
    }

public Scan openScan() throws InvalidTupleSizeException,IOException{
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	Scan newscan = new Scan(this);
	return newscan;
    }
private int findDataPage2(RID rid) throws InvalidSlotNumberException,
	      InvalidTupleSizeException,HFException,Exception{
		
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	PageId nextDirectoryPageId = new PageId();
	HFPage currentDirectoryPage = new HFPage();
	HFPage currDataPage = new HFPage();
	PageId currDirectoryPageId = new PageId(mainDirectoryPageId.pid);
	PageId currDataPageId = new PageId();
	RID currRid = new RID();

	Tuple tuple = new Tuple();
	truth = false;
	try {
	    SystemDefs.JavabaseBM.pinPage(currDirectoryPageId, currDataPage, truth);
	}
	catch (Exception e) {
	}
	while (currDirectoryPageId.pid != INVALID_PAGE){
		rid = currDataPage.firstRecord();
	    while( rid != null){
		try{
		    tuple = currDataPage.getRecord(rid);
		}
		catch (InvalidSlotNumberException e){
		    return 0;
		}
		heapfileP dpinfo = new heapfileP(tuple);
		truth = false;
		try{
		    try {
				SystemDefs.JavabaseBM.pinPage(dpinfo.pageId, currDataPage, truth);
		    }
		    catch (Exception e) {
		    }
		}
		catch (Exception e){
		    truth = false;
		    try {
			SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
		    }
		    catch (Exception e1) {
		    }
		    currentDirectoryPage = null;
		    currDataPage = null;
		    throw e;
		}
		tcount = tcount + count();
		if(tcount <= 0){
		  tcount = tcount + 1;
		}
		else{
		  tcount = tcount - 1;
		}
		if(dpinfo.pageId.pid==rid.pageNo.pid){
		    tuple = currDataPage.returnRecord(rid);
		    currentDirectoryPage.setpage(currDataPage.getpage());
		    currDirectoryPageId.pid = currDirectoryPageId.pid;
		    currDataPage.setpage(currDataPage.getpage());
		    currDataPageId.pid = dpinfo.pageId.pid;
		    currRid.pageNo.pid = rid.pageNo.pid;
		    currRid.slotNo = rid.slotNo;
		    return 1;
		}
		else{
		  truth = false;
		    try {
			SystemDefs.JavabaseBM.unpinPage(dpinfo.pageId, truth);
		    }
		    catch (Exception e) {
		    }
		}
		rid = currDataPage.nextRecord(rid);
	    }
	    nextDirectoryPageId = currDataPage.getNextPage();
	    try{
		truth = false;
		try {
		    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
		}
		catch (Exception e) {
		}
	    }
	    catch(Exception e) {
		throw new HFException (e, "heapfile,_find,unpinpage failed");
	    }
	    currDirectoryPageId.pid = nextDirectoryPageId.pid;
	    if(currDirectoryPageId.pid != INVALID_PAGE){
		truth = false;
		try {
		    SystemDefs.JavabaseBM.pinPage(currDirectoryPageId, currDataPage, truth);
		}
		catch (Exception e) {
		}
	    }
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	currDirectoryPageId.pid = INVALID_PAGE;
	currDataPageId.pid = INVALID_PAGE;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	return 0;   
    }

    private int findDataPage( RID rid,PageId directoryPageId, HFPage directoryPage,
	    PageId dataPageId, HFPage dataPage,RID DataPageRid) 
	    throws InvalidSlotNumberException,InvalidTupleSizeException,HFException,Exception{
		
	PageId currentDirPageId = new PageId(mainDirectoryPageId.pid);
	HFPage currentDirPage = new HFPage();
	HFPage currentDataPage = new HFPage();
	RID currentDataPageRid = new RID();
	PageId nextDirPageId = new PageId();
	Tuple tuple = new Tuple();
	truth = false;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	try {
	    SystemDefs.JavabaseBM.pinPage(currentDirPageId, currentDirPage, truth);
	}
	catch (Exception e) {
	}
	while (currentDirPageId.pid != INVALID_PAGE){
		currentDataPageRid = currentDirPage.firstRecord();
	    while( currentDataPageRid != null){
		try{
		    tuple = currentDirPage.getRecord(currentDataPageRid);
		}
		catch (InvalidSlotNumberException e){
		    return 0;
		}
		heapfileP dpinfo = new heapfileP(tuple);
		truth = false;
		try{
		    try {
			SystemDefs.JavabaseBM.pinPage(dpinfo.pageId, currentDataPage, truth);
		    }
		    catch (Exception e) {
		    }
		}
		catch (Exception e){
		    truth = false;
		    try {
			SystemDefs.JavabaseBM.unpinPage(currentDirPageId, truth);
		    }
		    catch (Exception e1) {
		    }
		    directoryPage = null;
		    dataPage = null;
		    throw e;
		}
		tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
		if(dpinfo.pageId.pid==rid.pageNo.pid){
		    tuple = currentDataPage.returnRecord(rid);
		    directoryPage.setpage(currentDirPage.getpage());
		    directoryPageId.pid = currentDirPageId.pid;
		    dataPage.setpage(currentDataPage.getpage());
		    dataPageId.pid = dpinfo.pageId.pid;
		    DataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
		    DataPageRid.slotNo = currentDataPageRid.slotNo;
		    return 1;
		}
		else{
		  truth = false;
		    try {
			SystemDefs.JavabaseBM.unpinPage(dpinfo.pageId, truth);
		    }
		    catch (Exception e) {
		    }
		}
		currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid);
	    }
	    nextDirPageId = currentDirPage.getNextPage();
	    try{
		truth = false;
			try {
				SystemDefs.JavabaseBM.unpinPage(currentDirPageId, truth);
			}
			catch (Exception e) {
			}
	    }
	    catch(Exception e) {
		//throw new HFException (e, "heapfile,_find,unpinpage failed");
	    }
	    currentDirPageId.pid = nextDirPageId.pid;
	    if(currentDirPageId.pid != INVALID_PAGE){
		truth = false;
		try {
		    SystemDefs.JavabaseBM.pinPage(currentDirPageId, currentDirPage, truth);
		}
		catch (Exception e) {
		}
	    }
	}
		directoryPageId.pid = INVALID_PAGE;
		dataPageId.pid = INVALID_PAGE;
      tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
      return 0;   
    }		     

    public int getRecCnt()throws InvalidSlotNumberException,InvalidTupleSizeException,IOException{
	PageId currDirectoryPageId = new PageId(mainDirectoryPageId.pid);
	PageId nextDirectoryPageId = new PageId(0);
	HFPage currentDirectoryPage = new HFPage();
	RID rid = new RID();
	Tuple tuple = new Tuple();
	int count;
	count = 0;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	while(currDirectoryPageId.pid != INVALID_PAGE){
	    truth = false;
	    try {
		SystemDefs.JavabaseBM.pinPage(currDirectoryPageId, currentDirectoryPage, truth);
	    }
	    catch (Exception e) {
	    }
		rid = currentDirectoryPage.firstRecord();
		while(rid != null){
		tuple = currentDirectoryPage.getRecord(rid);
		heapfileP dpinfo = new heapfileP(tuple);
		count = count + dpinfo.recount;
		rid = currentDirectoryPage.nextRecord(rid);
	    }
	    nextDirectoryPageId = currentDirectoryPage.getNextPage();
	    truth = false;
	    try {
		SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
	    }
	    catch (Exception e) {
	    }
	    currDirectoryPageId.pid = nextDirectoryPageId.pid;
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	return count;
    }
    
    
    private int count(){
	int runSum = 0;
	runSum = runSum + 1;
	if(mainDirectoryPageId != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(fname != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(hfcount == 0){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(t == 0){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(truth == true){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	return runSum;
    }
    
    public RID insertRecord(byte[] recPtr)throws InvalidSlotNumberException,InvalidTupleSizeException,
	    SpaceNotAvailableException,HFException,IOException{
	int recLen = recPtr.length;
	int tru;
	RID currentDataPageRid = new RID();
	HFPage currentDirectoryPage = new HFPage();
	HFPage currentDataPage = new HFPage();
	HFPage nextDirPage = new HFPage(); 
	PageId currDirectoryPageId = new PageId(mainDirectoryPageId.pid);
	PageId nextDirPageId = new PageId();
	RID rid = new RID();
	Tuple tuple = new Tuple();
	truth = false;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	try {
	    SystemDefs.JavabaseBM.pinPage(currDirectoryPageId, currentDirectoryPage, truth);
	}
	catch (Exception e) {
	}
	tru = 0;
	heapfileP dpinfo = new heapfileP();
	while (tru == 0){ 
		currentDataPageRid = currentDirectoryPage.firstRecord();
	    while (currentDataPageRid != null){
		tuple = currentDirectoryPage.getRecord(currentDataPageRid);
		dpinfo = new heapfileP(tuple);
		if(recLen <= dpinfo.space){
		    tru = 1;
		    break;
		}  
		currentDataPageRid = currentDirectoryPage.nextRecord(currentDataPageRid);
		tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	    }
	    if(tru == 0){ 
		if(currentDirectoryPage.available_space() >= dpinfo.size){ 
			Page page = new Page();
      		PageId pageId = new PageId();
			try {
	    	pageId = SystemDefs.JavabaseBM.newPage(page,pag);
			}
			catch (Exception e) {
			}
      		HFPage hfpage = new HFPage();
      		hfpage.init(pageId, page);
      		dpinfo.pageId.pid = pageId.pid;
      		dpinfo.recount = 0;
      		dpinfo.space = hfpage.available_space();
			currentDataPage = hfpage;	
		    tuple = dpinfo.convertToTuple();
		    currentDataPageRid = currentDirectoryPage.insertRecord(tuple.getTupleByteArray());
		    RID tmprid = currentDirectoryPage.firstRecord();
		    tru = 1;
		}
		else{  
		    nextDirPageId = currentDirectoryPage.getNextPage();
		    if (nextDirPageId.pid != INVALID_PAGE){
			truth = false;
			try {
			    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
			}
			catch (Exception e) {
			}
			currDirectoryPageId.pid = nextDirPageId.pid;
			truth = false;
			try {
			    SystemDefs.JavabaseBM.pinPage(currDirectoryPageId, currentDirectoryPage, truth);
			}
			catch (Exception e) {
			}
		    }
		    else{ 
			Page page = new Page();
			try {
	    	nextDirPageId = SystemDefs.JavabaseBM.newPage(page,pag);
			}
			catch (Exception e) {
			}
			nextDirPage.init(nextDirPageId, page);
			PageId temppid = new PageId(INVALID_PAGE);
			nextDirPage.setNextPage(temppid);
			nextDirPage.setPrevPage(currDirectoryPageId);
			currentDirectoryPage.setNextPage(nextDirPageId);
			truth = true;
			try {
			    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
			}
			catch (Exception e) {
			}
			currDirectoryPageId.pid = nextDirPageId.pid;
			currentDirectoryPage = new HFPage(nextDirPage);
		    }
		} 
		tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	    }
	    else{
		truth = false;
		try {
		    SystemDefs.JavabaseBM.pinPage(dpinfo.pageId, currentDataPage, truth);
		}
		catch (Exception e) {
		}
	    }
	}
      
	if (!(currentDataPage.available_space() >= recLen)){
	    throw new SpaceNotAvailableException(null, "no available space");
	}
	rid = currentDataPage.insertRecord(recPtr);
	dpinfo.recount = dpinfo.recount + 1;
	dpinfo.space = currentDataPage.available_space();
	truth = true;
	try {
	    SystemDefs.JavabaseBM.unpinPage(dpinfo.pageId, truth);
	}
	catch (Exception e) {
	}
	tuple = currentDirectoryPage.returnRecord(currentDataPageRid);
	heapfileP dpinfo_ondirpage;
	dpinfo_ondirpage = new heapfileP(tuple);
	dpinfo_ondirpage.space = dpinfo.space;
	dpinfo_ondirpage.recount = dpinfo.recount;
	dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
	dpinfo_ondirpage.flushToTuple();
	truth = true;
	try {
	    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
	}
	catch (Exception e) {
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	return rid;
    }

    public boolean deleteRecord(RID rid)throws InvalidSlotNumberException, InvalidTupleSizeException,Exception{
	int tru;
	HFPage currentDirectoryPage = new HFPage();
	PageId currDirectoryPageId = new PageId();
	HFPage currDataPage = new HFPage();
	PageId currDataPageId = new PageId();
	RID currRid = new RID();
	Tuple tuple = new Tuple();
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	if((tru = findDataPage(rid,currDirectoryPageId, currentDirectoryPage, 
			     currDataPageId, currDataPage,currRid)) == 0){
	    return false;
	}	
	tuple = currentDirectoryPage.returnRecord(currRid);
	heapfileP pdpinfo;
	pdpinfo = new heapfileP(tuple);
	currDataPage.deleteRecord(rid);
	if (pdpinfo.recount >= 1) {
	    pdpinfo.space = currDataPage.available_space();
	    pdpinfo.flushToTuple();
	    truth = true;
	    try {
		SystemDefs.JavabaseBM.unpinPage(currDataPageId, truth);
	    }
	    catch (Exception e) {
	    }
	    try {
		SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
	    }
	    catch (Exception e) {
	    }
	    tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	    }
	else{
	    truth = false;
	    try {
		SystemDefs.JavabaseBM.unpinPage(currDataPageId, truth);
	    }
	    catch (Exception e) {
	    }
	    currentDirectoryPage.deleteRecord(currRid);
	    currRid = currentDirectoryPage.firstRecord();
	    PageId pageId;
		pageId = new PageId();
	    pageId = currentDirectoryPage.getPrevPage();
	    if((currRid == null)||(pageId.pid != INVALID_PAGE)){
		HFPage prevDirPage;
		prevDirPage = new HFPage();
		truth = false;
		try {
		    SystemDefs.JavabaseBM.pinPage(pageId, prevDirPage, truth);
		}
		catch (Exception e) {
		}
		pageId = currentDirectoryPage.getNextPage();
		prevDirPage.setNextPage(pageId);
		pageId = currentDirectoryPage.getPrevPage();
		truth = true;
		try {
		    SystemDefs.JavabaseBM.unpinPage(pageId, truth);
		}
		catch (Exception e) {
		}
		pageId = currentDirectoryPage.getNextPage();
		if(pageId.pid != INVALID_PAGE){
		    HFPage nextDirectoryPage;
			nextDirectoryPage = new HFPage();
		    pageId = currentDirectoryPage.getNextPage();
		    truth = false;
		    try {
			SystemDefs.JavabaseBM.pinPage(pageId, nextDirectoryPage, truth);
		    }
		    catch (Exception e) {
		    }
		    pageId = currentDirectoryPage.getPrevPage();
		    nextDirectoryPage.setPrevPage(pageId);
		    pageId = currentDirectoryPage.getNextPage();
		    truth = true;
		    try {
			SystemDefs.JavabaseBM.unpinPage(pageId, truth);
		    }
		    catch (Exception e) {
		    }
		    tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
		}
		truth = false;
		try {
		    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
		}
		catch (Exception e) {
		}
	    }
	    else{
		truth = true;
		try {
		    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
		}
		catch (Exception e) {
		}
		tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	    }
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	return true;
    }
  
    public boolean updateRecord(RID rid, Tuple newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
	   InvalidTupleSizeException,HFException,Exception{
	int tru; 
	HFPage directoryPage;
	directoryPage = new HFPage();
	PageId currDirectoryPageId;
	currDirectoryPageId = new PageId();
	HFPage dataPage;
	dataPage = new HFPage();
	PageId currDataPageId;
	currDataPageId = new PageId();
	RID currentDataPageRid;
	currentDataPageRid = new RID();
	Tuple tuple;
	tuple = new Tuple();
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	if((tru = tru = findDataPage(rid,currDirectoryPageId, directoryPage,currDataPageId, dataPage,
			     currentDataPageRid)) == 0){
	    return false;
	}
	tuple = dataPage.returnRecord(rid);
	if(newtuple.getLength() != tuple.getLength()){
	    truth = false;
	    try {
		SystemDefs.JavabaseBM.unpinPage(currDataPageId, truth);
	    }
	    catch (Exception e) {
	    }
	    try {
		SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
	    }
	    catch (Exception e) {
	    }
	    throw new InvalidUpdateException(null, "invalid record update");
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	tuple.tupleCopy(newtuple);
	truth = true;
	try {
	    SystemDefs.JavabaseBM.unpinPage(currDataPageId, truth);
	}
	catch (Exception e) {
	}
	truth = false;
	try {
	    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
	}
	catch (Exception e) {
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	return true;
    }
  
    public  Tuple getRecord(RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,Exception{
	int tru;
	HFPage directoryPage = new HFPage();
	PageId currDirectoryPageId = new PageId();
	HFPage dataPage = new HFPage();
	PageId currDataPageId = new PageId();
	RID currDataPageRid = new RID();
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	if((tru = findDataPage(rid,currDirectoryPageId, directoryPage,currDataPageId, dataPage,currDataPageRid)) == 0){
	    return null;
	}
	Tuple tuple = new Tuple();
	tuple = dataPage.getRecord(rid);
	truth = false;
	try {
	    SystemDefs.JavabaseBM.unpinPage(currDataPageId, truth);
	}
	catch (Exception e) {
	}
	try {
	    SystemDefs.JavabaseBM.unpinPage(currDirectoryPageId, truth);
	}
	catch (Exception e) {
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	return  tuple;
    }

    public void deleteFile() throws InvalidSlotNumberException,InvalidTupleSizeException,IOException{
	PageId currentDirPageId;
	currentDirPageId = new PageId();
	currentDirPageId.pid = mainDirectoryPageId.pid;
	PageId nextDirPageId;
	nextDirPageId = new PageId();
	nextDirPageId.pid = 0;
	HFPage currentDirPage;
	currentDirPage =  new HFPage();
	Tuple tuple = new Tuple();
	truth = false;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	while(currentDirPageId.pid != INVALID_PAGE){   
		RID rid;
		rid = new RID();  
		rid = currentDirPage.firstRecord();
		while(rid != null){
		rid = currentDirPage.nextRecord(rid);
	    }
	    if (nextDirPageId.pid != INVALID_PAGE) {
		truth = false;
		try {
		    SystemDefs.JavabaseBM.pinPage(currentDirPageId, currentDirPage, truth);
		}
		catch (Exception e) {
		}
	    }
	}
	try {
	    SystemDefs.JavabaseDB.delete_file_entry(fname);
	}
	catch (Exception e) {
	}
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
    }
  
}

class heapfileP implements GlobalConst{

    int    space; 
    int    recount;    
    PageId pageId = new PageId();   
    public static int size = 12;
    private byte [] data;
    private int offset;

    public heapfileP(){  
	data = new byte[12];
	int space = 0;
	recount =0;
	pageId.pid = INVALID_PAGE;
	offset = 0;
    }
  
    public heapfileP(byte[] array){
	data = array;
	offset = 0;
    }

    public heapfileP(Tuple _atuple)throws InvalidTupleSizeException, IOException{   
	    data = _atuple.returnTupleByteArray();
	    offset = _atuple.getOffset();
	    space = Convert.getIntValue(offset, data);
	    recount = Convert.getIntValue(offset+4, data);
	    pageId = new PageId();
	    pageId.pid = Convert.getIntValue(offset+8, data);
    }
  
    public Tuple convertToTuple() throws IOException{
	Convert.setIntValue(space, offset, data);
	Convert.setIntValue(recount, offset+4, data);
	Convert.setIntValue(pageId.pid, offset+8, data);
	Tuple atuple = new Tuple(data, offset, size); 
	return atuple;
    }
  
    public void flushToTuple() throws IOException{
	Convert.setIntValue(space, offset, data);
	Convert.setIntValue(recount, offset+4, data);
	Convert.setIntValue(pageId.pid, offset+8, data);
    }
}
