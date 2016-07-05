package scripts

import org.joda.time.DateTime
import org.joda.time.Days
import org.joda.time.Hours
import org.joda.time.Minutes
import org.joda.time.Months
import org.joda.time.Seconds
import org.joda.time.Weeks
import org.joda.time.Years

import java.text.SimpleDateFormat

def res = null

try {
    def from = doc[fromv].value
    def to = doc[tov].value

    def fd = null
    def td = null

    if (from == null) {
        if(format1 == null){
            fd = d1;
        }else{
            fd = new DateTime(new SimpleDateFormat(format1).parse(d1).getTime())
        }
    } else {
        if(format1 == null){
            fd = new DateTime(from.toLong())
        }else{
            fd = new DateTime(new SimpleDateFormat(format1).parse(from.toString()).getTime())
        }
    }


    if (to == null) {
        if(format2 == null){
            td = d2;
        }else{
            td = new DateTime(new SimpleDateFormat(format2).parse(d2).getTime())
        }
    } else {
        if (format2 == null) {
            td = new DateTime(to.toLong())
        } else {
            td = new DateTime(new SimpleDateFormat(format2).parse(to.toString()).getTime())
        }
    }

    if (fd != null && td != null) {
        if (unit != null) {
            switch (unit) {
                case 'y': res = Years.yearsBetween(fd, td).years; break;
                case 'M': res = Months.monthsBetween(fd, td).months; break;
                case 'w': res = Weeks.weeksBetween(fd, td).weeks; break;
                case 'd': res = Days.daysBetween(fd, td).days; break;
                case 'h': res = Hours.hoursBetween(fd, td).hours; break;
                case 'm': res = Minutes.minutesBetween(fd, td).minutes; break;
                case 's': res = Seconds.secondsBetween(fd, td).seconds; break;
                default: res = 0; break;
            }
        } else {
            res = Days.daysBetween(fd, td).getDays()
        }
    }
} finally {
    return res
}
