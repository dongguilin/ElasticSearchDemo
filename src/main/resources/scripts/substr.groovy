package scripts

def result = null;
try {
    str = doc[field].value.toString()
    if (end != null) {
        if (start < end) {
            if (end >= str.length()) {
                result = str[start..-1];
            } else {
                result = str[start..end - 1];
            }
        }
    } else {
        if (start < str.length()) {
            result = str[start..-1];
        }
    }
} finally {
    return result;
}
