package scripts

def num=0;
try{
    num = doc[field]?doc[field].value.toDouble():0;
}catch(Throwable t) {
    num = 0;
}

return num;