const userActions = {
    LOGIN: "LOGIN",
    UPLOAD_IMAGE: "UPLOAD_IMAGE",
    DELETE_IMAGE: "DELETE_IMAGE",
    GET_IMAGE: "GET_IMAGE",
    GET_ALL_KEYS: "GET_ALL_KEYS",
    GET_CACHE_STATS: "GET_CACHE_STATS",
};
  
const actions = {
    login: login,
    uploadImage: uploadImage,
    deleteImage: deleteImage,
    getImage: getImage,
    getAllImageKeys: getAllImageKeys,

    getCacheStat: getCacheStat
};

function success(type, data) {
    return { type: type, data : data};
}

function login(data){
    return (dispatch) => {
        dispatch(success(userActions.LOGIN, data));
    };
}

function getAllImageKeys(data) {
    return (dispatch) => {
        dispatch(success(userActions.GET_ALL_KEYS, data));
    };    
}

function getCacheStat(data) {
    return (dispatch) => {
        dispatch(success(userActions.GET_CACHE_STATS, data));
    };      
}

// TO BE IMPLEMENTED
function uploadImage(data) {
}

// TO BE IMPLEMENTED
function deleteImage(){

}

function getImage(data) {
    return (dispatch) => {
        dispatch(success(userActions.GET_IMAGE, data));
    };   
}

export { actions, userActions};