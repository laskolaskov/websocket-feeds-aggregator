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

const feeds = []
let interfaceFeed = []

io.sockets.on('connection', (sock) => {
    console.log('Connected ::', sock.id)

    sock.on('feed', (data) => {
        console.log(`Received < ${data} > from :: ${sock.id}`)
    })

    sock.on('interface', () => {
        console.log('Web interface connected !!!')
        aggr = getFeedForTimeFrame(5)
        console.log('connecting aggregator ...')
        aggrConnection = aggr.connect()
        console.log('aggregator connected !')
        aggrSub = aggr.subscribe(data => { 
            console.log(`Aggregated FEED :: ${JSON.stringify(data)}`)
            sock.emit('aggregator', data)
        })
        interfaceFeed = [aggr, aggrConnection, aggrSub, sock.id]
    })
    //listen for client disconnect
    sock.on('disconnect', (reason) => {
        console.log(`Socket Disconnected :: ${sock.id} :: ${reason}`)
        let toDelete
        feeds.forEach((feed, i) => {
            if (feed[2] == sock.id) {
                feed[1].unsubscribe()
                console.log(`Unsubscribed feed ${feed[2]}`)
                toDelete = i
            }
        })
        delete feeds[toDelete]
        //also clear web intercace feed if needed
        if(interfaceFeed[3] && sock.id == interfaceFeed[3]) {
            interfaceFeed[1].unsubscribe()
            interfaceFeed[2].unsubscribe()
            interfaceFeed = []
            console.log('Aggregated FEED disconnected and unsubscribed !!!')
        }
    })
    source = Rx.fromEvent(sock, 'feed')
    sub = source.subscribe(data => console.log(`IN OBSERVABLE ${sock.id} : ${JSON.stringify(data)}`))
    feeds.push([source, sub, sock.id])
})


const getFeedForTimeFrame = (timeFrame) => {
    //timer ticking every 'timeFrame' seconds
    const timer = Rx.interval(timeFrame * 1000)
    //create the aggregated feed
    const feed = Rx.merge(
        //augment all feeds output to include their extended PriceFeed props
        ...feeds.map(pf => pf[0]))
        .pipe(
            //buffer output between timer ticks
            rxOps.buffer(timer),
            //aggregate the buffered output
            //uses custom aggregator function
            rxOps.map(x => aggregator(x)),
            //publish as ConnectableObservable
            rxOps.publish()
        )
    console.log('FEED :: ', feed)
    return feed
}

/* addPriceFeed(priceFeed) {
    this.priceFeeds.push(priceFeed)
}

removePriceFeed(providerName) {

} */

const aggregator = (input) => {
    console.log('input :: ', input);
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
    console.log('reduced :: ', reduced)
    console.log('count :: ', Object.values(reduced).length)
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
