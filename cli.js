#!/usr/bin/env node
const eventStore = require('eventstore-rx')
const alco = require('alco')
const fifo = require('fifo')

const main = () => {
  var errorBuffer = []
  const stdin = alco.observable.fromStdinLines()
    .map(x => {
      try {
        const ret = JSON.parse(x)
        errorBuffer = []
        return ret
      } catch (ex) {
        console.warn(x)
        errorBuffer.push(x)
      }
    })
    .filter(x => x)
    .do(null, null, () => {
      if (errorBuffer.length) throw new Error(errorBuffer.join('\n'))
    })
  .share()
  const stdout = {
    next: (val) => typeof (val) === 'object'
      ? console.log(JSON.stringify(val))
      : console.log(val),
    error: (error) => console.error(error),
    complete: () => {}
  }
  const args = fifo()
  process.argv.slice(2).forEach(x => args.push(x))

  var url
  const readArgs = () => {
    const arg = args.shift()
    if (!arg) return
    if (arg.startsWith('tcp://')) url = arg
    else if (arg === 'subscribe') doSubscribe()
    else if (arg === 'publish') doPublish()
    else if (arg === 'consume') doConsume()
    else throw new Error('Unexpected arg ' + arg)
    readArgs()
  }

  const doSubscribe = () => {
    const topic = args.shift()
    if (!url) throw new Error('Specify URL first')
    if (!topic) throw new Error('Topic expected')
    eventStore(url).subscribe(topic).subscribe(stdout)
  }
  const doPublish = () => {
    const topic = args.shift()
    if (!url) throw new Error('Specify URL first')
    if (!topic) throw new Error('Topic expected')
    eventStore(url).publish(topic, stdin).subscribe(stdout)
  }
  const doConsume = () => {
    const topic = args.shift()
    const group = args.shift()
    if (!url) throw new Error('Specify URL first')
    if (!topic) throw new Error('Topic expected')
    if (!group) throw new Error('Group expected')
    eventStore(url).consume(topic, group).subscribe(stdout)
  }
  readArgs()
}

main()
