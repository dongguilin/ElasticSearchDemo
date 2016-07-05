package scripts

def res = null
try {
    def str = doc[field].value.toDouble()

    def flag = false
    if (inCludeFrom) {
        flag = str >= from
    } else {
        flag = str > from
    }

    if (flag) {
        if (inCludeTo) {
            flag = str <= to
        } else {
            flag = str < to
        }
    }

    if (flag) {
        res = str
    }
} finally {
    return res;
}