package scripts

def res = null
try {
    res =  doc[field].value.toString()
}finally{
    return res;
}