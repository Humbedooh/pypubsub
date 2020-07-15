const http = require("http");
const https = require("https");

class PyPubSub {
    constructor(url) {
        this.url = url;
        this.getter = url.match(/^https/i) ? https : http;
    }

    attach(func) {
        this.getter.get(this.url, res => {
            res.setEncoding("utf8");
            let body = '';
            res.on("data", data => {
                body += data;
                if (data.endsWith("\n")) {
                    let payload = JSON.parse(body);
                    body = '';
                    func(payload);
                }
              });
        });
    }
}


// Test
function process(payload) {
    // ping-back?
    if (payload.stillalive) {
        console.log("Got a ping-back");
    // Actual payload? process it!
    } else {
        console.log("Got a payload from PyPubSub!");
        console.log(payload);
    }
}

const pps = new PyPubSub('http://localhost:2069/');
pps.attach(process);

