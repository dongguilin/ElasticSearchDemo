package scripts

def result="";
try{
    result = doc[field];
    if(result!=null){
		result =doc[field].value.toString().toUpperCase();
    }
}catch(Throwable t) {
}

return result;