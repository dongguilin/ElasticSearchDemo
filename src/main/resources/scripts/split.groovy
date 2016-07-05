package scripts

import com.google.common.base.Splitter
def res = null
try {
    def str = doc[field].value.toString()
    def lists = Splitter.onPattern(separator).splitToList(str);
    if (index != null && index < lists.size()) {
        res = lists.get(index)
    } else {
        res = str
    }
} finally {
    return res;
}