import React, { useState } from 'react'
import { Layout, Menu, Breadcrumb } from 'antd';
import './Mainpage.css';

// import MemcacheStats from "../memcachePages/MemcacheStats";
// import MemcacheSetting from '../memcachePages/MemcacheSetting';

import ImageUploader from '../imageUploader/ImageUploader';
import ImageViewer from '../imageViewer/ImageViewer';

const { Header, Content, Footer } = Layout;

const NUM_PAGES = 4;

const PAGES_TITLE = {
  0: "Upload Image",
  1: "View Image",
  // 2: "Memcache performance",
  // 3: "Memcache setting"
}

const PAGES = {
  0: <ImageUploader/>,
  1: <ImageViewer/>,
  // 2: <MemcacheStats/>,
  // 3: <MemcacheSetting/>,
}

const Mainpage = (props) => {
    const [curPage, setCurpage] = useState(0);

    const curPageComponent = PAGES[curPage];

    return (
        <Layout className="layout">
        <Header>
          <div className="logo" />
          <Menu theme="dark" mode="horizontal" defaultSelectedKeys={['0']}>
            {new Array(NUM_PAGES).fill(null).map((_, index) => {
              const key = index;
              return <Menu.Item key={key} onClick={(action) => {
                setCurpage(key)
              }}>{PAGES_TITLE[key]}</Menu.Item>;
            })}
          </Menu>
        </Header>
        <Content style={{ padding: '0 50px'}}>
          <div className="site-layout-content" style={{minHeight:700}}>
            {
              curPageComponent
            }

          </div>
        </Content>
        <Footer style={{ textAlign: 'center' }}>This is footer.</Footer>
      </Layout>
    );
}

export default Mainpage;