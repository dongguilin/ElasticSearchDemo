package scripts

def result = null;
try {
    result = doc[field].value.toString().toLowerCase();
} finally {
    return result;
}