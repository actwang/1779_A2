import axios from "axios";

import API from "../apiEndPoints";

function debugLog(data) {
    console.log(data);
}


async function getMemcacheStat() {    
    let res = await axios.get(URL=API.GETCACHESTATS);

    return res.data.memcache_stat;
}


export default { getMemcacheStat }

