import { useState } from 'react';
import { Upload, Button, message } from 'antd';
import { UploadOutlined } from '@ant-design/icons';

const Uploader = (props) => {
    const [files, setFiles] = useState([]);
    const [uploading, setUploading] = useState(false);

    console.log(props);
    // handleUpload = () => {
    //     setUploading(false);
    //     props.
    // }
  
    const uploadPropos = {
      onRemove: file => {
        setFiles([]);
      },
      beforeUpload: file => {
        setFiles([file]);
        return false;
      },
      fileList: files,
      maxCount:1,
    };

    return (
      <>
        <Upload {...uploadPropos}>
          <Button icon={<UploadOutlined />}>Select File</Button>
        </Upload>
        <Button
          type="primary"
          onClick={() => {
            console.log(files);
            props.handleUpload(files[0])
          }}
          disabled={(files.length) == 0}
          loading={uploading}
          style={{ marginTop: 16 }}
        >
          {uploading ? 'Uploading' : 'Start Upload'}
        </Button>
      </>
    );
}

export default Uploader;