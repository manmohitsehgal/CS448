package heap;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;

public class Scan implements GlobalConst{
 

    private Heapfile heapfile;
    private PageId directoryId = new PageId();
    private PageId dataPageId = new PageId();
    private HFPage directoryPage = new HFPage();
    private HFPage dataPage = new HFPage();
    private RID dataPageRid = new RID();
    private RID currId = new RID();
    private boolean ns;
    private int tcount = 0;

    public Tuple getNext(RID rid) throws InvalidTupleSizeException,IOException{
		Tuple tuple;
		tuple = new Tuple();        
		if (ns == false) {
			int truth;
            truth = next();
        }
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
        if (dataPage == null){
            return null;
        }
        
        rid.pageNo.pid = currId.pageNo.pid;    
        rid.slotNo = currId.slotNo;
        try {
            tuple = dataPage.getRecord(rid);
        }
        catch (Exception e) {
        }   
    
        currId = dataPage.nextRecord(rid);
        if(currId != null){
            ns = true;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
			return tuple;
        }
        else{
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
            ns = false;
			return tuple;
        }
    }

    public boolean position(RID rid) throws InvalidTupleSizeException,IOException{ 
        RID nextrid;
		nextrid = new RID();
        boolean flag = true;
		rid.pageNo.pid = currId.pageNo.pid;
        rid.slotNo = currId.slotNo;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
        if (nextrid.equals(rid)!= false){
            return true;
        }

        PageId pgid;
		pgid = new PageId();
        pgid.pid = rid.pageNo.pid;
        if (!dataPageId.equals(pgid)) {
			if (dataPage != null) {
            	try{
					try {
		    			SystemDefs.JavabaseBM.unpinPage(dataPageId, false);
					}
					catch (Exception e) {
					}
            	}
            	catch (Exception e){
            	}  
        	}
        	dataPageId.pid = 0;
        	dataPage = null;
        	if (directoryPage == null) {
            	directoryPage = null;
        		ns = true;
            	flag =  first();
            	if (flag != true){
                	return flag;
            	}
        	}
			else{
				try{
					try {
		    			SystemDefs.JavabaseBM.unpinPage(directoryId, false);
					}
					catch (Exception e) {
					}
            	}
            	catch (Exception e){
            	}
				directoryPage = null;
        		ns = true;
            	flag =  first();
            	if (flag != true){
                return flag;
            	}
			}

            while (!dataPageId.equals(pgid)) {
				int truth;
                truth = next();
                if (truth == 0){
                    return false;
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
        try{
            currId = dataPage.firstRecord();
		}
        catch (Exception e) {
        }
        if (currId == null){
            flag = false;
            return flag;
        }
    
		rid.pageNo.pid = currId.pageNo.pid;
        rid.slotNo = currId.slotNo;
        flag = true;
        while ((flag == true) || (nextrid != rid)){
        	if (dataPage == null){
             	flag = false;
        	}

    		nextrid = dataPage.nextRecord(rid);
			if( nextrid != null ){
            	currId.pageNo.pid = nextrid.pageNo.pid;
            	currId.slotNo = nextrid.slotNo;
            	flag = true;
			}
        	else {
				int truth;
            	truth = next();
            	if (truth == 1){
                	rid.pageNo.pid = currId.pageNo.pid;
                	rid.slotNo = currId.slotNo;
            	}
				else{
					flag = true;
				}
			}
			return true;
        }
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
        return flag;
    }

    public void closescan(){
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
    	if (dataPage == null) {
			dataPageId.pid = 0;
        dataPage = null;
        }
		else{
			try {
		    		SystemDefs.JavabaseBM.unpinPage(dataPageId, false);
				}
				catch (Exception e) {
			}
			dataPageId.pid = 0;
        	dataPage = null;
		}
        if (directoryPage != null) {
            try{
		try {
		    SystemDefs.JavabaseBM.unpinPage(directoryId, false);
		}
		catch (Exception e) {
		    throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
		}
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        directoryPage = null;
        ns = true;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
    }
 
    private boolean first() throws InvalidTupleSizeException,IOException{
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
        ScanP pi;
        Tuple tuple;
		tuple = new Tuple();
        directoryId.pid = heapfile.mainDirectoryPageId.pid;
    	try {
		directoryPage = new HFPage();	   
	    try {
		SystemDefs.JavabaseBM.pinPage(directoryId, directoryPage, false);
	    }
	    catch (Exception e) {
	    } 
	}
    	catch (Exception e) {
	}
    
	dataPageRid = directoryPage.firstRecord();
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
    	if (dataPageRid == null) {
			PageId nextDirPageId;
			nextDirPageId = new PageId();
            nextDirPageId = directoryPage.getNextPage();
            if (nextDirPageId.pid == INVALID_PAGE) {
		dataPageId.pid = INVALID_PAGE;
            }
            else {
                try {
					try {
		    			SystemDefs.JavabaseBM.unpinPage(directoryId, false);
					}
					catch (Exception e) {
					}
                }
                catch (Exception e) {
                }
                try {
                    directoryPage = new HFPage();
		    		try {
						SystemDefs.JavabaseBM.pinPage(nextDirPageId, directoryPage, false);
		    		}
		    		catch (Exception e) {
		    		}
                }
                catch (Exception e) {
                }
                try {
                    dataPageRid = directoryPage.firstRecord();
                }
                catch (Exception e) {
                }
                if(dataPageRid != null) {
                    try {
                        tuple = directoryPage.getRecord(dataPageRid);
                    }
                    catch (Exception e) {
                    }
                    if (tuple.getLength() == ScanP.size){
						pi = new ScanP(tuple);
						dataPageId.pid = pi.pageId.pid;
                    }
             		else{
					ns = true;
					return false;
					}
                }
                else {
                    dataPageId.pid = INVALID_PAGE;
                }
				dataPageId.pid = INVALID_PAGE;
            }
        }
        else {
			try {
                tuple = directoryPage.getRecord(dataPageRid);
            }  
				
            catch (Exception e) {
            }			    
            pi = new ScanP(tuple);
            dataPageId.pid = pi.pageId.pid;
        }
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	try{
			int truth;
            truth = next();
	  }
	catch (Exception e) {
	}
		ns = true;
      return true;
    }
    
    private int count(){
	int runSum = 0;
	runSum = runSum + 1;
	if(directoryId != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(dataPageId != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(directoryPage != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(dataPage != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(dataPageRid != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(currId != null){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	if(ns == true){
	    runSum = runSum + 1;
	}
	else{
	    runSum = runSum - 1;
	}
	return runSum;
    }
    
    
    
    private int next() throws InvalidTupleSizeException,IOException{
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
        ScanP pi;
        if ((directoryPage == null) || (dataPageId.pid == INVALID_PAGE)){
            return 0;
        }
        if (dataPage == null) {
            if (dataPageId.pid != INVALID_PAGE) {
                try {
                    dataPage  = new HFPage();
		    try {
			SystemDefs.JavabaseBM.pinPage(dataPageId, dataPage, false);
		    }
		    catch (Exception e) {
		    }
                }
                catch (Exception e){
                }
                try {
                    currId = dataPage.firstRecord();
                }
                catch (Exception e) {
                }
                return 1;
            }
            else {
		try{
		    try {
			SystemDefs.JavabaseBM.unpinPage(directoryId, false);
		    }
		    catch (Exception e) {
		    }
                    directoryPage = null;
                }
                catch (Exception e){
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
        try{
	    try {
		SystemDefs.JavabaseBM.unpinPage(dataPageId, false);
	    }
	    catch (Exception e) {
	    }
            dataPage = null;
        }
        catch (Exception e){
        } 
        if (directoryPage == null) {
            return 0;
        }
    
        dataPageRid = directoryPage.nextRecord(dataPageRid);
	PageId nextDirectoryPageId;
	nextDirectoryPageId = new PageId();
        if (dataPageRid == null) {
            nextDirectoryPageId = directoryPage.getNextPage();
            try {
		try {
		    SystemDefs.JavabaseBM.unpinPage(directoryId, false);
		}
		catch (Exception e) {
		}
                directoryPage = null;
                dataPageId.pid = INVALID_PAGE;
            }
            catch (Exception e) {
            }	    
            if (nextDirectoryPageId.pid != INVALID_PAGE){
                directoryId = nextDirectoryPageId;
                try { 
                    directoryPage = new HFPage();
		    try {
			SystemDefs.JavabaseBM.pinPage(directoryId, directoryPage, false);
		    }
		    catch (Exception e) {
		    }
                }
                catch (Exception e){
              
                }
                if (directoryPage == null){
                    return 0;
                }
                try {
                    dataPageRid = directoryPage.firstRecord();
                }
                catch (Exception e){
                    return 0;
                } 
            }
            else {
                return 0;
            }
        }
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	Tuple tuple;
	tuple = null;
        try {
            tuple = directoryPage.getRecord(dataPageRid);
        }
	catch (Exception e) {
	}
	if (tuple.getLength() != ScanP.size){
            return 0;
	}
                       
	pi = new ScanP(tuple);
	dataPageId.pid = pi.pageId.pid;
 	try {
            dataPage = new HFPage();
	    try {
		SystemDefs.JavabaseBM.pinPage(pi.pageId, dataPage, false);
	    }
	    catch (Exception e) {
	    }
        }
	catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
	}
        currId = dataPage.firstRecord();
        if(currId == null){
            ns = false;
            return 0;
        }
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
        return 1;
    }
    
    public Scan(Heapfile hf) throws InvalidTupleSizeException, IOException{
        heapfile = hf;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	ScanP pi;
        Tuple tuple;
		tuple = new Tuple();
        directoryId.pid = heapfile.mainDirectoryPageId.pid;
    	try {
		directoryPage = new HFPage();	   
	    try {
		SystemDefs.JavabaseBM.pinPage(directoryId, directoryPage, false);
	    }
	    catch (Exception e) {
	    } 
	}
    	catch (Exception e) {
	}
    
	dataPageRid = directoryPage.firstRecord();
    	if (dataPageRid == null) {
	    PageId nextDirPageId;
	    nextDirPageId = new PageId();
            nextDirPageId = directoryPage.getNextPage();
            if (nextDirPageId.pid != INVALID_PAGE) {
				try {
		    		SystemDefs.JavabaseBM.unpinPage(directoryId, false);
				}
				catch (Exception e) {
				}
                directoryPage = new HFPage();
		    	try {
					SystemDefs.JavabaseBM.pinPage(nextDirPageId, directoryPage, false);
		    	}
		    	catch (Exception e) {
		    	}
                try {
                    dataPageRid = directoryPage.firstRecord();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                if(dataPageRid == null) {
                    dataPageId.pid = INVALID_PAGE;
                }
                else {
					try {
                        tuple = directoryPage.getRecord(dataPageRid);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (tuple.getLength() == ScanP.size){
						pi = new ScanP(tuple);
						dataPageId.pid = pi.pageId.pid;
                    }
                }
            }
            else {
                dataPageId.pid = INVALID_PAGE;
            }
        }
        else {
			 try {
                tuple = directoryPage.getRecord(dataPageRid);
            }  		
            catch (Exception e) {
            }			    
            pi = new ScanP(tuple);
            dataPageId.pid = pi.pageId.pid;
        }
		dataPage = null;
			int truth;
            truth = next();
		ns = true;
	tcount = tcount + count();
	if(tcount <= 0){
	  tcount = tcount + 1;
	}
	else{
	  tcount = tcount - 1;
	}
	}
}

class ScanP implements GlobalConst{

    int    space; 
    int    recount; //recount    
    PageId pageId = new PageId();   
    public static int size = 12;
    private byte [] data;
    private int offset;

    public ScanP(){  
	data = new byte[12];
	int space = 0;
	recount =0;
	pageId.pid = INVALID_PAGE;
	offset = 0;
    }
 
    public ScanP(Tuple _atuple)throws InvalidTupleSizeException, IOException{   
	    data = _atuple.returnTupleByteArray();
	    offset = _atuple.getOffset();
	    space = Convert.getIntValue(offset, data);
	    recount = Convert.getIntValue(offset+4, data);
	    pageId = new PageId();
	    pageId.pid = Convert.getIntValue(offset+8, data);
    }
}
