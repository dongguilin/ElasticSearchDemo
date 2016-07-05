package scripts

import org.joda.time.DateTime
import org.joda.time.Days

def fromw = new DateTime(doc[from].value)
def tow 
if(doc[to]){
	tow = new DateTime(doc[to].value)
	}else{
		if(d2){
			tow = new DateTime(d2)
		}else{
			tow = new DateTime()
		}
	}
	Days.daysBetween(fromw, tow).getDays();
