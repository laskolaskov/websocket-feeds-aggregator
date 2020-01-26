const Rx = require('rxjs')

//uses /socket.io/socket.io.js exposed by Socket.io
//no host:port config needed 
const socket = io()

const aggregatorFeed = {
    source: null,
    sub: null
}

const updateEvent = {
    source: null,
    sub: null
}

socket.on('connect', () => {
    console.log('Connected to server !')
    //create event observables
    aggregatorFeed.source = Rx.fromEvent(socket, 'aggregator')
    aggregatorFeed.sub = aggregatorFeed.source.subscribe(aggregatorObserver)
    updateEvent.source = Rx.fromEvent(socket, 'update')
    updateEvent.sub = updateEvent.source.subscribe(updateEventObserver)
})

socket.on('disconnect', () => {
    console.error('Disconnected from server !')
    //unsub and clear the aggregated feed
    if (aggregatorFeed.source) {
        aggregatorFeed.sub.unsubscribe()
        console.log('Unsubscribed feed.')
        aggregatorFeed.source = null
    }
    //unsub and clear the update event
    if (updateEvent.source) {
        updateEvent.sub.unsubscribe()
        console.log('Unsubscribed update event.')
        updateEvent.source = null
    }
})

const aggregatorObserver = {
    next: (data) => console.log(data),
    error: (err) => console.log(err),
    complete: () => console.log('Aggregator Feed Completed !')
}

const updateEventObserver = {
    next: (data) => console.log(data),
    error: (err) => console.log(err),
    complete: () => console.log('Update Event Feed Completed !')
}