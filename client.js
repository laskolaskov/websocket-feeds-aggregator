const Rx = require('rxjs')
//const socket = require('socket.io-client')//('http://localhost:3000')

//uses /socket.io/socket.io.js exposed by Socket.io
//no host:port config needed 
const socket = io() 

socket.on('connect', () => {
    console.log('Connected to server !')
    socket.emit('interface', null)
})

socket.on('aggregator', (data) => {
    console.log(data)
});

socket.on('disconnect', () => {
    console.error('Disconnected from server !')
})
