const csv = require('csv')
const stringify = require('streaming-json-stringify')()
const sfilter = require('stream-filter')
const request = require('request')
const fs = require('fs')
const id = a => a

//change this to filter out stars
// const filterStars = (star) => ((!!star.proper || star.lum > 200) && (star.dist < 100000))
const filterStars = (star) => (true)


const getOrRead = () => {
  if (fs.existsSync('./hygdata_v3.csv')) {
    console.log(`Processing 32MB of star coordinates. No progress info, I'm too lazy`)
    return fs.createReadStream('./hygdata_v3.csv')
  } else {
    console.log(`Downloading and processing 32MB of star coordinates. No progress info, I'm too lazy`)
    const requestForTheStars = request.get('https://github.com/astronexus/HYG-Database/raw/master/hygdata_v3.csv')
    requestForTheStars.pipe(fs.createWriteStream('./hygdata_v3.csv'))
    return requestForTheStars
  }
}

getOrRead()
.pipe(csv.parse())
.pipe(csv.transform(getFieldsBasedOnLine1({
  'id': parseFloat,
  'proper': id,
  'absmag': parseFloat,
  'ci': parseFloat,
  'x': parseFloat,
  'y': parseFloat,
  'z': parseFloat,
  'con': id,
  'lum': parseFloat,
  'dist': parseFloat,
  'bf': id
})))
.pipe(sfilter.obj((star) => (star.dist < 10000000)))
.pipe(sfilter.obj(filterStars))
.pipe(stringify)
.pipe(fs.createWriteStream('./hygdata.json'))

function getFieldsBasedOnLine1 (fields) {
  let isFirst = true
  const getters = []
  return record => {
    if (isFirst) {
      isFirst = false
      record.forEach((item, number) => {
        if (fields[item]) {
          getters.push(mkGetter(item, number, fields[item]))
        }
      })
    } else {
      return getters.map((get) => get(record)).reduce((mem, item) => (Object.assign(mem, item)))
    }
  }
}

function mkGetter (key, num, func) {
  return (record) => ({
    [key]: func(record[num])
  })
}
