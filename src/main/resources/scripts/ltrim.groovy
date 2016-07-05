package scripts

def result="";
try{
    result = doc[field];
    if(result!=null){
		result =doc[field].value.toString().trim();
		result ="trim"
    }
}catch(Throwable t) {
}

return result;