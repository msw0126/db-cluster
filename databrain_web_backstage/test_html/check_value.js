function transform_check(v, min, max, parse_method,label){
    try{
        v = parse_method(v);
    }catch(error) {
        return "can not transform to "+label
    }
    if(min!==null&&v<min){
        return "should between "+min+" and "+max;
    }
    if(max!==null&&v>max){
        return "should between "+min+" and "+max;
    }
    return null
}

function checkInt(v){
    v1 = parseInt(v);
    v2 = parseFloat(v);

    if(v2>=v1){
        throw Error()
    }
}

function check_value(value, vtype, multiple, check){
    value = value.trim();
    value = value.split(",");

    if(value.length>0&&(!multiple)){
        return "can not accept multiple value"
    }

    min = null;
    max = null;
    decimal = check.attr('decimal');
    if(vtype==='double'&&decimal===undefined){
        decimal = 3;
    }

    for(var i=0;i<value.length;i++){
        v = value[i].trim();
        if(vtype==='int'){
            trans_res = transform_check(v, min, max, checkInt, 'int')
            if(trans_res!==null) return trans_res;
        }else if(vtype==='double'){
            trans_res = transform_check(v, min, max, parseFloat, 'int')
            if(trans_res!==null) return trans_res;

            v = parseFloat(v);
            v1 = v.toFixed(decimal);
            if(v1!==v){
                return "remain "+decimal;
            }
        }else{
            return
        }
    }

}