import { connect } from "react-redux";
import { Table } from 'antd';

const columns = [
  {
    title: 'Number of items',
    dataIndex: 'num_items',
    key: 'num_items',
  },
  {
    title: 'Total size',
    dataIndex: 'total_size',
    key: 'total_size',
  },
  {
    title: 'Num requests',
    dataIndex: 'num_requests',
    key: 'num_requests',
  },
  {
    title: 'Miss rate',
    key: 'miss_rate',
    dataIndex: 'miss_rate',
  },

  {
    title: 'Hit rate',
    key: 'hit_rate',
    dataIndex: 'hit_rate',
  },
];

const StatsTable = (props) => {
    const dataSource = props.stats.reduce((reducer, curValue) => {
        reducer = reducer.concat({
            key: reducer.length,
            num_items: curValue[0],
            total_size: curValue[1],
            num_requests: curValue[2],
            miss_rate : curValue[3],
            hit_rate: curValue[4],
        });
        return reducer;
    }, []);

    return (
        <Table columns={columns} dataSource={dataSource} />
    );
}

const actionsCreactor = {
};

const mapState = (state) => {
    return {stats: state.actionReducer.cacheStats};
    // return {stats: []};
};


export default connect(mapState, actionsCreactor)(StatsTable);;