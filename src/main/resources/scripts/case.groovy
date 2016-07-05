package scripts

import java.util.regex.Pattern

def result = null;
def find = false

try {
    for (i = 0; i < cons.size(); i++) {
        def con = cons[i].tokenize(',')
        def fieldv = doc[con[0]].value
        def cv = con[2]

        switch (con[1]) {
            case '>':
                if (fieldv.toDouble() > Double.parseDouble(cv)) {
                    result = con[3];
                    find = true
                }
                break;
            case '>=':
                if (fieldv.toDouble() >= Double.parseDouble(cv)) {
                    result = con[3];
                    find = true
                }
                break;
            case '<':
                if (fieldv.toDouble() < Double.parseDouble(cv)) {
                    result = con[3];
                    find = true
                };
                break;
            case '<=':
                if (fieldv.toDouble() <= Double.parseDouble(cv)) {
                    result = con[3];
                    find = true
                };
                break;
            case '==':
                if (fieldv.toDouble() == Double.parseDouble(cv)) {
                    result = con[3];
                    find = true
                };
                break;
            case '!=':
                if (fieldv.toDouble() != Double.parseDouble(cv)) {
                    result = con[3];
                    find = true
                };
                break;
            case 'contains':
                if (fieldv.contains(cv)) {
                    result = con[3];
                    find = true
                };
                break;
            case 'match':
                if (Pattern.compile(cv).matcher(fieldv).find()) {
                    result = con[3];
                    find = true
                }
                break;
        }

        if (find) {
            break;
        }
    }

    if (!find && els != null) {
        result = els;
    }

} finally {
    return result
}