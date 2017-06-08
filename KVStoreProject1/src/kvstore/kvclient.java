package kvstore;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class kvclient {
	
	public static final String EMPTY_STRING = "";
	public static final String GET = "get";
	public static final String SET = "set";
	public static final String DEL = "del";
	
	
	/**
	 *  Function to print generalized error.
	 */
	static void printError(){
		System.out.println("Bad command!");
		System.exit(2);
	}
	
	/**
	 *  Method to print insufficient number of arguments exception.
	 */
	static void printInsuffNoOfArgs(){
		System.out.println("Insufficient Number of Arguments!");
		System.exit(2);
	}
	
	/**
	 * @param result
	 * 
	 * Function to process result object.
	 */
	static void processResult(Result result,String method){
		String errorText = EMPTY_STRING;
		if(null != result){
			ErrorCode errorCode =  result.getError();
			switch(errorCode){
				case kSuccess:
					String value = result.getValue();
					errorText = result.getErrortext();
					if(method == GET){
						System.out.println(value);
					}
					if(null != errorText) {
						System.err.println(errorText);
					}
					System.exit(errorCode.getValue());
					break;
					
				case kKeyNotFound:
					errorText = result.getErrortext();
					if(null != errorText) {
						System.err.println(errorText);
					}
					System.exit(errorCode.getValue());
					break;
					
				case kError:
					errorText = result.getErrortext();
					if(null != errorText) {
						System.err.println(errorText);
					}
					System.exit(errorCode.getValue());
					break;
			}
		}
		else {
			System.out.println("Empty Result");
			System.exit(2);
		}
	}
	
	 /**
	 * @param args
	 */
	public static void main(String[] args) {
	
	  try {
	   TTransport transport;
	   
	   // Do something
	   
	   //Server Name
	   String server = args[0];
	   
	   //Get the server address
	   String[] address = args[1].split(":");
	   
	   String ip = address[0];
	   String port = address[1];
	   
	   //Open a socket to server
	   transport = new TSocket(ip, Integer.parseInt(port));
	   transport.open();
	
	   TProtocol protocol = new TBinaryProtocol(transport);
	   KVStore.Client client = new KVStore.Client(protocol);
	   
	   String operation = args[2];
	   String key = EMPTY_STRING;
	   Result res = null;	
	   
	   operation = (operation.substring(operation.indexOf("-") + 1, operation.length()));
	   switch (operation) {	   
		   case GET:
			   try{
				   if(null != args[3]){
					   //To Do 
					   //String filename = args[5];
					   key = args[3];
					   res = client.kvget(key);
					   
					   processResult(res,GET);
				   }
			   }
			   catch(Exception e){
				   printError();
			   }
			   break;
		   case SET:
			   try{
				   if(null != args[3] && null != args[4]){
					   key = args[3];
					   String value = args[4];
					   res = client.kvset(key, value);

					   processResult(res,SET);
				   }
			   }
			   catch(Exception e){
				   printError();
			   }			   
			   break;
		   case DEL:
			   try{
				   if(null != args[3]){
					   key = args[3];
					   res = client.kvdelete(key);
					   processResult(res,DEL);
				   }
			   }
			   catch(Exception e){
				   printError();
			   }
			   
			   break;
			   
			 default:
				 printError();
				 break;
	   }
	   
	
	   transport.close();
	  } 
	
	  catch (TTransportException e) {
	   //e.printStackTrace();
	   System.out.println("Server not Running");
	   System.exit(2);
	  } 
	  catch(ArrayIndexOutOfBoundsException ae){
		  printInsuffNoOfArgs();
	  }
	  catch (Exception e) {
		  printError();
	  }
	 }

}