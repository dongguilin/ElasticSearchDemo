package scripts

def result = null
try {
    def field1 = doc[field1].value
    field1 = field1 == null ? '' : field1.toString()
    def field2 = doc[field2].value
    field2 = field2 == null ? '' : field2.toString()
    result = field1.concat(field2)
} finally {
    return result;
}