package scripts

def result="";
try{
    result = doc[field];
    if(result!=null){
		result =doc[field].value.toString().toLowerCase();
    }
}catch(Throwable t) {
}

return result;