package scripts

def result = null;
try {
    result = doc[field].value.toString().toUpperCase();
} finally {
    return result;
}
