package diskmgr;
import chainexception.*;


public class BufferPoolExceedException extends ChainException {

  public BufferPoolExceedException(Exception e, String name)
  { 
    super(e, name); 
  }

  


}




