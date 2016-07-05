package scripts

result=els;
cons.any { elem ->
  con=elem.tokenize(',');
  temkey = doc[con[0]].value.toDouble();
  tem = con[2].toDouble();
  //temkey=5;
  switch (con[1]) {  
        case '>':  
        	if(temkey>tem){
				result = con[3]; 
				return true;
        	}
            break;
        case '>=':  
            if(temkey>=tem){
            	result = con[3]; 
				return true;
            } 
            break;
        case '<':  
            if(temkey<tem){
            	result = con[3]; 
				return true;
            };
            break;
        case '<=':  
            if(temkey<=tem){
            	result = con[3]; 
				return true;
            };
            break;
        case '==':  
            if(temkey==tem){
            	result = con[3]; 
				return true;
            };
            break;
        case '!=':   
            if(temkey!=tem){
            	result = con[3]; 
				return true;
            };
            break; 
        case 'contains':  
            if(temkey==tem){
            	result = con[3]; 
				return true;
            };
            break;
        case 'match':  
            if(temkey==tem){
            	result = con[3]; 
				return true;
            };
            break;  
    }      
}
result;