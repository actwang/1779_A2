import { createStore, applyMiddleware } from "redux";
import thunkMiddleware from "redux-thunk";
import rootReducer from "./reducer";
import { composeWithDevTools } from "redux-devtools-extension";

// Sample code copied from other projects, not need to modify this file
const middlewareEnhancer = applyMiddleware(thunkMiddleware);

const store = createStore(rootReducer, composeWithDevTools(middlewareEnhancer));

export default store;