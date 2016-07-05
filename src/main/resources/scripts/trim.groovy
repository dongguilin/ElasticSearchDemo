package scripts

def result="";
try{
    result = doc[field];
    if(result!=null){
		result =doc[field].value.toString().trim();
    }
}catch(Throwable t) {
}

return result;