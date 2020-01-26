const express = require('express')
const socket = require('socket.io')
const Rx = require('rxjs')
const rxOps = require('rxjs/operators')


const port = 3000
//express app
const app = express()
//create web server
const server = app.listen(port, () => {
    console.log(`Aggregator Service Listening on port : ${port}`)
})
//set public folder to serve static files
app.use(express.static('public'))
//create websocket on the web server
const io = socket(server)

const tick = 2
const feeds = {}
const webFeed = {
    source: null,
    connection: null,
    sub: null
}

io.sockets.on('connection', (sock) => {
    console.log('Connected ::', sock.id)

    source = Rx.fromEvent(sock, 'feed')
    sub = source.subscribe(data => {
        //console.log(`IN OBSERVABLE ${sock.id} : ${JSON.stringify(data)}`
    })
    feeds[sock.id] = { source, sub }
    //update the web feed
    updateWebFeed()

    //listen for client disconnect
    sock.on('disconnect', (reason) => {
        console.log(`Socket Disconnected :: ${sock.id} :: ${reason}`)
        //unsubscribe
        feeds[sock.id].sub.unsubscribe()
        console.log(`Unsubscribed feed !`)
        //remove socket
        delete feeds[sock.id]
        //update web feed with remaining feeds
        updateWebFeed()
    })
})

const getFeedForTimeFrame = (timeFrame) => {
    //timer ticking every 'timeFrame' seconds,
    //starting after 1 sec delay to buffer some initial data

    const timer = Rx.timer(1000, timeFrame * 1000)
    //create the aggregated feed
    return Rx.merge(
        //augment all feeds output to include their extended PriceFeed props
        ...Object.values(feeds).map(pf => pf.source))
        .pipe(
            //buffer output between timer ticks
            rxOps.buffer(timer),
            //aggregate the buffered output
            //uses custom aggregator function
            rxOps.map(x => aggregator(x)),
            //publish as ConnectableObservable
            rxOps.publish()
        )
}

const aggregator = (input) => {
    //reduce the input to the latest entry for each 'providerName-symblol' combination
    const reduced = input.reduce((result, pf) => {
        if (!result[`${pf.providerName}-${pf.symbol}`]) {
            result[`${pf.providerName}-${pf.symbol}`] = pf
        } else if (
            result[`${pf.providerName}-${pf.symbol}`].timestamp < pf.timestamp
        ) {
            result[`${pf.providerName}-${pf.symbol}`] = pf
        }
        return result
    }, {})
    //create and return the aggregated summary object
    return Object.values(
        Object.values(reduced).reduce((summary, pf) => {
            //does symbol exists as key ?
            if (!summary[pf.symbol]) {
                summary[pf.symbol] = initPriceSummary(pf)
            } else {
                if (pf.value.buyPrice > summary[pf.symbol].bestBuyPrice.value) {
                    summary[pf.symbol].bestBuyPrice = {
                        value: pf.value.buyPrice,
                        spread: pf.value.buyPrice - pf.value.sellPrice,
                        provider: pf.providerName,
                    }
                }
                if (pf.value.sellPrice < summary[pf.symbol].bestSellPrice.value) {
                    summary[pf.symbol].bestSellPrice = {
                        value: pf.value.sellPrice,
                        spread: pf.value.sellPrice - pf.value.buyPrice,
                        provider: pf.providerName
                    }
                }
            }
            return summary
        }, {})
    )
}

//helper to init the summary object
const initPriceSummary = (pf) => {
    return {
        symbol: pf.symbol,
        timestamp: Date.now(),
        bestBuyPrice: {
            value: pf.value.buyPrice,
            spread: pf.value.buyPrice - pf.value.sellPrice,
            provider: pf.providerName,
        },
        bestSellPrice: {
            value: pf.value.sellPrice,
            spread: pf.value.sellPrice - pf.value.buyPrice,
            provider: pf.providerName
        }
    }
}

const observer = {
    next: (data) => {
        io.sockets.emit('aggregator', data)
    },
    error: (err) => {
        console.error('ERROR :: ', err)
    },
    complete: () => console.log('Aggregated Feed Completed !')
}

const updateWebFeed = () => {
    //disconnect and unsubscribe existing web feed
    if (webFeed.source) {
        webFeed.connection.unsubscribe()
        webFeed.sub.unsubscribe()
    }
    //get new feed, connect and subscribe
    webFeed.source = getFeedForTimeFrame(tick)
    webFeed.connection = webFeed.source.connect()
    webFeed.sub = webFeed.source.subscribe(observer)
    console.log('Inputs updated !')
    io.sockets.emit('update', null)
}