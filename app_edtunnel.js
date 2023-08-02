const WebSocket = require('ws');
const { Readable } = require('stream');
const net = require('net');

const wss = new WebSocket.Server({ port: 443 });
let userID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
let proxyIP = "64.68.192." + Math.floor(Math.random() * 255);

let address = '';
let portWithRandomLog = '';
const log = (/** @type {string} */ info, /** @type {string | undefined} */ event) => {
    console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
};


wss.on('connection', (ws) => {
    console.log("ws connection")
  
    ws.once('message', (chunk) => {
        console.log("on message")
        let webSocket = ws;
        let remoteSocketWapper = {
            value: null,
        };
        let isDns = false;

        const {
            hasError,
            message,
            portRemote = 443,
            addressRemote = '',
            rawDataIndex,
            vlessVersion = new Uint8Array([0, 0]),
            isUDP,
        } = processVlessHeader(chunk, userID);

        address = addressRemote;
        portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '
        } `;
        if (hasError) {
            console.log('hasError,Connection closed:'+message);
            ws.close();
            return;
        }
        if (isUDP) {
            if (portRemote === 53) {
                isDns = true;
            } else {
    
                throw new Error('UDP proxy only enable for DNS which is port 53');
                return;
            }
        }
        const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
        const rawClientData = chunk.slice(rawDataIndex);

        if (isDns) {
            return handleDNSQuery(rawClientData, webSocket, vlessResponseHeader, log);
        }
        handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
    });
    ws.on('close', () => {
        console.log('Connection closed');
        ws.close();
    });
});
async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log,) {
    async function connectAndWrite(address, port) {
        const options = {
            host: address,
            port: port      
        };

        const tcpSocket = net.createConnection(options, () => {
            console.log('LEE LOGS');
        });

        remoteSocket.value = tcpSocket;
        log(`handleTCPOutBound connected to ${address}:${port}`);
       
        console.log("rawClientData:"+rawClientData)
        tcpSocket.write(rawClientData); 
   
        return tcpSocket;
    }


    async function retry() {
        console.log("retry")
        const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote)
        // no matter retry success or not, close websocket
        tcpSocket.closed.catch(error => {
            console.log('retry tcpSocket closed error', error);
        }).finally(() => {
            safeCloseWebSocket(webSocket);
        })
        remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
    }

    const tcpSocket = await connectAndWrite(addressRemote, portRemote);

    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry, log);
}


async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
    
    let remoteChunkCount = 0;
    let chunks = [];
  
    let vlessHeader = vlessResponseHeader;


   
    remoteSocket.on('data', (data) => {
        if (vlessHeader) {
            new Blob([vlessHeader, data]).arrayBuffer()
                .then(arrayBuffer => {
                    webSocket.send(arrayBuffer);
                })
                .catch(error => {
                    console.error('处理 ArrayBuffer 出错：', error);
                });

            vlessHeader = null;
        } else {
            webSocket.send(data);
        }
    });

    
    remoteSocket.on('end', () => {
        console.log("remoteSocket on end end end")
        webSocket.send(null);
    });
    let is_error = false;

   
    remoteSocket.on('error', () => {
        is_error = true;
    });

    if (is_error && retry) {
        log(`retry`)
        retry();
    }
}

function processVlessHeader(
    vlessBuffer,
    userID
) {
    if (vlessBuffer.byteLength < 24) {
        return {
            hasError: true,
            message: 'invalid data',
        };
    }
    const version = new Uint8Array(vlessBuffer.slice(0, 1));
    let isValidUser = false;
    let isUDP = false;
    if (stringify(new Uint8Array(vlessBuffer.slice(1, 17))) === userID) {
        console.log(stringify(new Uint8Array(vlessBuffer.slice(1, 17))))
        console.log(userID)
        isValidUser = true;
    }
    if (!isValidUser) {
        console.log(stringify(new Uint8Array(vlessBuffer.slice(1, 17))))
        console.log(userID)
        return {
            hasError: true,
            message: 'invalid user',
        };
    }

    const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
    //skip opt for now

    const command = new Uint8Array(
        vlessBuffer.slice(18 + optLength, 18 + optLength + 1)
    )[0];

    if (command === 1) {
    } else if (command === 2) {
        isUDP = true;
    } else {
        return {
            hasError: true,
            message: `command ${command} is not support, command 01-tcp,02-udp,03-mux`,
        };
    }
    const portIndex = 18 + optLength + 1;
    const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  
    const portRemote = portBuffer.readUInt16BE();

    let addressIndex = portIndex + 2;
    const addressBuffer = new Uint8Array(
        vlessBuffer.slice(addressIndex, addressIndex + 1)
    );

  
    const addressType = addressBuffer[0];
    let addressLength = 0;
    let addressValueIndex = addressIndex + 1;
    let addressValue = '';
    switch (addressType) {
        case 1:
            addressLength = 4;
            addressValue = new Uint8Array(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
            ).join('.');
            break;
        case 2:
            addressLength = new Uint8Array(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + 1)
            )[0];
            addressValueIndex += 1;
            addressValue = new TextDecoder().decode(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
            );
            break;
        case 3:
            addressLength = 16;
            const dataView = new DataView(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
            );
          
            const ipv6 = [];
            for (let i = 0; i < 8; i++) {
                ipv6.push(dataView.getUint16(i * 2).toString(16));
            }
            addressValue = ipv6.join(':');
           
            break;
        default:
            return {
                hasError: true,
                message: `invild  addressType is ${addressType}`,
            };
    }
    if (!addressValue) {
        return {
            hasError: true,
            message: `addressValue is empty, addressType is ${addressType}`,
        };
    }

    return {
        hasError: false,
        addressRemote: addressValue,
        addressType,
        portRemote,
        rawDataIndex: addressValueIndex + addressLength,
        vlessVersion: version,
        isUDP,
    };
}

const byteToHex = [];
for (let i = 0; i < 256; ++i) {
    byteToHex.push((i + 256).toString(16).slice(1));
}

function unsafeStringify(arr, offset = 0) {
    return (byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] + byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + "-" + byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + "-" + byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + "-" + byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + "-" + byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] + byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] + byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]).toLowerCase();
}
function stringify(arr, offset = 0) {
    const uuid = unsafeStringify(arr, offset);
    return uuid;
}


async function handleDNSQuery(udpChunk, webSocket, vlessResponseHeader, log) {
    
    try {
        const dnsServer = '8.8.4.4'; 
        const dnsPort = 53;
        let vlessHeader = vlessResponseHeader;

        const options = {
            host: dnsServer,
            port: dnsPort       
        };
        const tcpSocket = net.createConnection(options, () => {
            console.log('handleDNSQuery 已连接到服务器');
        });

        log(`connected to ${dnsServer}:${dnsPort}`);
        tcpSocket.write(udpChunk)

     
        tcpSocket.on('data', (data) => {
            if (webSocket.readyState === WS_READY_STATE_OPEN) {
                if (vlessHeader) {
                    new Blob([vlessHeader, data]).arrayBuffer()
                        .then(arrayBuffer => {
                            webSocket.send(arrayBuffer);
                        })
                        .catch(error => {
                            console.error('ArrayBuffer：', error);
                        });
                    vlessHeader = null;
                } else {
                    webSocket.send(data);
                }
            }
        });
    } catch (error) {
        console.error(
            `handleDNSQuery have exception, error: ${error.message}`
        );
    }
}

function safeCloseWebSocket(socket) {
    try {
        if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
            socket.close();
        }
    } catch (error) {
        console.error('safeCloseWebSocket error', error);
    }
}
