import { useState } from "react";
// import { connect } from "react-redux";
import postRequest from "../rpcCalls/postRequest";
import Uploader from './Uploader';

const ImageUploader = () => {
    const [errorMessage, setMessage] = useState("");
    const [imageKey, setImageKey] = useState("");

    const myProps = {
        onChange(file) {
            if (file.status !== 'uploading') {
                if (! file.type.includes('image')) {
                    setMessage("You should only uplode images");
                } else {
                    let response = postRequest.postImage(imageKey, file);
                    if (response.success !== "true") {
                        setMessage(response.error.message);
                    }
                }
            }
        },
    };
      
    return (    
        <div style={{textAlign: 'center'}}>
            Please type in the key of your uploaded image.
            <br/>
            <input type="text"
                placeholder="Key of the image"
                onChange={(e) => {
                    let imageKey = e.target.value;
                    setImageKey(imageKey);
                }}
            />
            <br/>
            <Uploader handleUpload={myProps.onChange}/>
            {errorMessage}
        </div>
    );
}

// const actionsCreactor = {
//     // uploadImage: actions.ImageUploader;
// };

// const mapState = (state) => {
//   return { ...state.actionReducer};
// };

// export default connect(mapState, actionsCreactor)(ImageUploader);

export default ImageUploader;