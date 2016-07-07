package diskmgr;
import chainexception.*;


public class HashEntryFoundException extends ChainException {

  public HashEntryFoundException(Exception e, String name)
  { 
    super(e, name); 
  }
}




