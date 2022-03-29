import { combineReducers } from "redux";
import { userActions } from "./action";

import user from "../models/user";

const initState = user;

// Sample reducer function. Will update the fields in state.
function userLogin(state, data) {
    state.name = data.name;
    state.authenticationToken = data.authenticationToken;
    state.keys = data.images;

    return state;
}

function getAllImageKeys(state, data) {
    state.keys = data;
    return state;
}

function getImageOfBase64(state, data) {
    state.curImage = data;
    return state;
}

function getCacheStat(stat, data) {
    stat.cacheStats = data;
    return stat;
}

function actionReducer(state = initState, action) {
    switch (action.type) {
        case userActions.LOGIN:
            console.log("Action: login");
            return userLogin(state, action.data);

        case userActions.UPLOAD_IMAGE:
            console.log("Action: add image");
            return state;

        case userActions.DELETE_IMAGE:
            console.log("Action: delete image");
            return state;

        case userActions.GET_IMAGE:
            console.log("Action: get image");
            return Object.assign({}, getImageOfBase64(state, action.data));
        
        case userActions.GET_ALL_KEYS:
            console.log("Action: get all keys");
            return Object.assign({}, getAllImageKeys(state, action.data));

        case userActions.GET_CACHE_STATS:
            console.log("Action: get cache stats");
            return Object.assign({}, getCacheStat(state, action.data));
           
        default:
            return Object.assign({}, state);
    }
}

const rootReducer = combineReducers({
    actionReducer,});
    
export default rootReducer;