package scripts

import java.util.regex.Matcher
import java.util.regex.Pattern

def result = null
try {
    def field = doc[field].value.toString();
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(field);
    while(matcher.find()){
        if(group != null){
            result = matcher.group(group)
        }else{
            result = field
        }
        break
    }
} finally {
    return result;
}