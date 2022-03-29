import { useState } from 'react';
import { Form, Input, Select, Button, message } from 'antd';
import postRequest from '../rpcCalls/postRequest';

const { Option } = Select;


const MemcacheSetting = (props) => {
    const options = [
        {
            value: "LRU",
            label: 'LRU',
        },
        {
            value: "Random",
            label: 'Random',

        }
    ];

    const onFinish = (values) => {
        postRequest.postCacheSetting(values.capacity, values.policy)
        .then(res => {
            message.info("Update success");
        }).catch(err => {
            message.error(err);
        });
    };
    
    const onFinishFailed = (errorInfo) => {
        console.log('Failed:', errorInfo);
    };

    const [replacePolicy , setPolicy] = useState("Random");

    return (
        <Form
            name="basic"
            labelCol={{ span: 8 }}
            wrapperCol={{ span: 16 }}
            initialValues={{ remember: true }}
            onFinish={onFinish}
            onFinishFailed={onFinishFailed}
            autoComplete="off"
        >
        <Form.Item
            name="capacity"
            label="Memcache Capacity"
            rules={[{  message: 'Please input ideal memcache capacity', required: true, pattern: new RegExp('^[0-9]+$')}]}
            help="Must be numerical value"
        >
            <Input />
        </Form.Item>
        <Form.Item label="Memcache Policy" name="policy" rules={[{required: true}]}>
          <Select
            placeholder="Select a replacement policy"
            onChange={(value) => setPolicy(value)}
            allowClear
          >
            {options.map((option) => {
                return <Option value={option.value} key={option.value}>{option.label}</Option>
            })}
          </Select>
        </Form.Item>        
        <Form.Item wrapperCol={{ offset: 8, span: 16 }}>
            <Button type="primary" htmlType="submit">
            Submit
            </Button>
        </Form.Item>
        </Form>
    );
}


export default MemcacheSetting;
