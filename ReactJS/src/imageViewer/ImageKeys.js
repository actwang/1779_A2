import { List, Divider, Card, Input } from 'antd';
import { connect } from "react-redux";
import { actions } from '../redux/action';
import postRequest from '../rpcCalls/postRequest';

const { Search } = Input; 

const ImageKeys = (props) => {

    const onSearch = value => {
        postRequest.postGetImage(value).then((content) => {
            props.getImage(content);
        });
    }

    return (
        <div>
            <Divider>Type in a key</Divider>
                <Search 
                    placeholder="input key of image file"
                    onSearch={onSearch}
                    enterButton
                />        
            <Divider style={{overflow:"auto"}}>or select one</Divider>
            <List
                size="small"
                header={null}
                footer={null}
                bordered
                dataSource={props.keys}
                renderItem={item => 
                <Card
                    hoverable
                    size="small"
                    onClick={
                        (e) => {
                            onSearch(item)
                        }
                    }
                >
                    {item}
                </Card>}
            />
        </div>
    );
}


const actionsCreactor = {
    getAllKeys: actions.getAllImageKeys,
    getImage: actions.getImage
};

const mapState = (state) => {
    return { keys: state.actionReducer.keys};
};


export default connect(mapState, actionsCreactor)(ImageKeys);