const proxy = require('http-proxy-middleware');

module.exports = function(app) {
    app.use(
        proxy.createProxyMiddleware('/api/*', {
            target: 'https://127.0.0.1:5003',
        })
    );
    app.listen(5003, () =>{
        console.log("Listen on 5003");
    })
}