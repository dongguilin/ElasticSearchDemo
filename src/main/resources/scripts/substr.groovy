package scripts

def result="";
//try{
    result = doc[field];
    if(result!=null){
    	temp = doc[field].value.toString();
    	if(start.abs()>=temp.length()){
    		return "";
    	}
    	if(end==null) result = temp[start..-1];
    	else {
    		if(end.abs()>=temp.length()){
    			return "";
    		}
    		result =temp[start..end];
    		
    	}
    }
//}catch(Throwable t) {
//}

return result;
