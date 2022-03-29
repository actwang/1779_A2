import axios from "axios";

import API from "../apiEndPoints";

function debugLog(data) {
    console.log(data);
}

async function postImage(key, file) {
    let formdata = new FormData();
    formdata.append('key', key);
    formdata.append('file', file);

    let res = await axios.post(URL=API.UPLOADIMAGE, formdata);
    debugLog(res.data);
    return res.data;
}

async function postListAllKeys() {
    let res = await axios.post(URL=API.GETALLKEYS);
    debugLog(res.data);
    return res.data.keys;
}


async function postGetImage(key) {    
    let res = await axios.post(URL=API.GETIMAGE + '?key=' + key);

    debugLog(res.data);
    return res.data.content;
}

async function postCacheSetting(capacity, policy) {    
    let formdata = new FormData();
    formdata.append('capacity', capacity);
    formdata.append('rep_policy', policy)
    let res = await axios.post(URL=API.POSTCACHESETTING, formdata);

    debugLog(res.data);
    return res.data.content;
}


export default { postImage, postListAllKeys, postGetImage, postCacheSetting }

