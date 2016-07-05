package scripts

import org.joda.time.DateTime

import java.text.SimpleDateFormat

def res = null
try {
    def str = doc[field].value.toString()
    if (format != null) {
        SimpleDateFormat df = new SimpleDateFormat(format)
        res = new DateTime(df.parse(str).getTime()).getMillis()
    } else {
        res = new DateTime(str).getMillis()
    }
} finally {
    return res;
}