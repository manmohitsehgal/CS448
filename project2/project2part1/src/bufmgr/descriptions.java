package bufmgr;

import global.PageId;

public class descriptions{
	private int 	pinCount;
	private PageId	pageNumber;
	private boolean	dirtyBit;
	
	public descriptions(){
		this.pinCount = 0;
		this.pageNumber= null;
		this.dirtyBit  = false;
	} 

	public descriptions(int pinCount, PageId pageNumber, boolean dirtyBit){
		this.pinCount 	= pinCount;
		this.pageNumber	= pageNumber;	
		this.dirtyBit	= dirtyBit;
	} 
	
	public void set_pin_count(int pinCount){
		this.pinCount = pinCount;	
	}
	
	public void set_page_number(PageId pageNumber){
		this.pageNumber = pageNumber;
	}

	public void set_dirty_bit(boolean dirtyBit){
		this.dirtyBit = dirtyBit;
	}
	
	public int get_pin_count(){
		return pinCount;	
	}

	public PageId get_page_number(){
		return pageNumber;	
	}

	public boolean get_dirty_bit(){
		return dirtyBit;	
	}

	public String toString(){
		return "pinCount" + get_pin_count() + "pageId "+get_page_number().pid+"dirtyBit "+get_dirty_bit();	
	}
		
}














