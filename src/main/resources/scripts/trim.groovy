package scripts

def result = null;
try {
    result = doc[field].value.toString().trim();
} finally {
    return result;
}