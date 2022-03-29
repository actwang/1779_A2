import {  useEffect } from 'react';
import { connect } from "react-redux";


import getRequest from '../rpcCalls/getRequest';
import { actions } from '../redux/action';
import StatsTable from './StatsTable';

const MemcacheStats = (props) => {
    useEffect(() => {
        getRequest.
            getMemcacheStat().
            then((datas) => {
                props.afterGetCacheStat(datas);
            }).catch(err => {
                console.error(err);
            })
    });

    return (
       <div>
           <StatsTable/>
       </div>
    )
}

const actionsCreactor = {
    afterGetCacheStat : actions.getCacheStat
};

const mapState = (state) => {
    return {};
};


export default connect(mapState, actionsCreactor)(MemcacheStats);;