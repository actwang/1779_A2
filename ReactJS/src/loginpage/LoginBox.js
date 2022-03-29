import { actions } from "../redux/action";
import API from "../apiEndPoints";

import { connect } from "react-redux";


import axios from "axios";

import { Form, Input, Button, Checkbox } from 'antd';

const LoginBox = (props) => {
  const onFinish = (values) => {
        axios
            .post(API.TESTLOGIN, {
                username: values.username,
                password: values.password
            })
            .then((res) => {
                props.login(res.data);
            }).catch((error) => {
                console.error(error);
            });
  };

  const onFinishFailed = (errorInfo) => {
    console.log('Failed:', errorInfo);
  };

  return (
    <Form
      name="basic"
      labelCol={{
        span: 8,
      }}
      wrapperCol={{
        span: 16,
      }}
      initialValues={{
        remember: true,
      }}
      onFinish={onFinish}
      onFinishFailed={onFinishFailed}
      autoComplete="off"
    >
      <Form.Item
        label="Username"
        name="username"
        rules={[
          {
            required: true,
            message: 'Please input your username!',
          },
        ]}
      >
        <Input />
      </Form.Item>

      <Form.Item
        label="Password"
        name="password"
        rules={[
          {
            required: true,
            message: 'Please input your password!',
          },
        ]}
      >
        <Input.Password />
      </Form.Item>

      <Form.Item
        name="remember"
        valuePropName="checked"
        wrapperCol={{
          offset: 8,
          span: 16,
        }}
      >
        <Checkbox>Remember me</Checkbox>
      </Form.Item>

      <Form.Item
        wrapperCol={{
          offset: 8,
          span: 16,
        }}
      >
        <Button type="primary" htmlType="submit">
          Submit
        </Button>
      </Form.Item>
    </Form>
  );
};

const actionsCreactor = {
    login : actions.login
};

const mapState = (state) => {
    return {};
};

export default connect(mapState, actionsCreactor)(LoginBox);