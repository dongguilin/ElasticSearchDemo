package scripts

def num = null;
try {
    num = doc[field] ? doc[field].value.toDouble() : 0;
}  finally {
    return num;
}