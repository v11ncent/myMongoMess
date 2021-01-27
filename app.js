//api - https://covidtracking.com/
//api docs - https://covidtracking.com/data/api
//mongodb docs - https://docs.mongodb.com/drivers/node/usage-examples/insertOne


const { SSL_OP_SSLEAY_080_CLIENT_DH_BUG } = require('constants');
const https = require('https');
const { MongoClient } = require('mongodb');
const uri = '';
const client = new MongoClient(uri, { useUnifiedTopology: true });
const { performance } = require('perf_hooks');


async function fetchState(state) {
    const options = {
        host: 'api.covidtracking.com',
        path: `/v1/states/${state}/current.json`,
        port: 443,
        method: 'GET',
    }
    
    return new Promise((resolve, reject) => {
        try {
            const req = https.request(options, res => {
                let data = '';
                res.on('error', e => {
                    reject(e);
                })
                res.on('data', chunk => {
                    data += chunk;
                })
                res.on('end', () => {
                    data = JSON.parse(data);
                    let date = data.date;
                    let state = data.state;
                    let positive = data.positive;
                    let probableCases = data.probableCases;
                    let negative = data.negative;
                    resolve({
                        date: date,
                        state: state,
                        positive: positive,
                        probableCases: probableCases,
                        negative: negative
                    })
                })
            })
            req.end();
        }
        catch(e) {
            reject(e);
        }
    })
}

async function fetchAllStates() {
    const options = {
        host: 'api.covidtracking.com',
        path: `/v1/states/current.json`,
        port: 443,
        method: 'GET',
    }
    
    return new Promise((resolve, reject) => {
        try {
            const req = https.request(options, res => {
                let data = '';
                res.on('error', e => {
                    reject(e);
                })
                res.on('data', chunk => {
                    data += chunk;
                })
                res.on('end', () => {
                    data = JSON.parse(data);
                    resolve(data);
                })
            })
            req.end();
        }
        catch(e) {
            reject(e);
        }
    })
}

async function fetchStatesHistorical(dateInt) {
    const options = {
        host: 'api.covidtracking.com',
        path: `/v1/states/daily.json`,
        port: 443,
        method: 'GET',
    }
    
    return new Promise((resolve, reject) => {
        try {
            const req = https.request(options, res => {
                let data = '';
                res.on('error', e => {
                    reject(e);
                })
                res.on('data', chunk => {
                    data += chunk;
                })
                res.on('end', () => {
                    data = JSON.parse(data);
                    let dataArray = [];
                    let i = 0;
                    data.forEach(element => {
                        if(element.date === dateInt) {
                            dataArray.push(element);
                            i++;
                            console.log(`Pushed element of date ${element.date}; ${i} occurrences...`)
                        }
                    })
                    resolve(dataArray);
                })
            })
            req.end();
        }
        catch(e) {
            reject(e);
        }
    })
}

async function fetchStatesHistoricalAllDates() {
    const options = {
        host: 'api.covidtracking.com',
        path: `/v1/states/daily.json`,
        port: 443,
        method: 'GET',
    }
    
    return new Promise((resolve, reject) => {
        try {
            const req = https.request(options, res => {
                let data = '';
                res.on('error', e => {
                    reject(e);
                })
                res.on('data', chunk => {
                    data += chunk;
                })
                res.on('end', () => {
                    data = JSON.parse(data);
                    resolve(data);
                })
            })
            req.end();
        }
        catch(e) {
            reject(e);
        }
    })
}
//============================================================
async function connect() { 
    try { await client.connect() }
    catch (err) { console.error(err) }
}

async function close() {
    try { await client.close() }
    catch (err) { console.error(err) }
}

async function createCollection(date) {
    try {
        const database = client.db('virusDB');
        await database.createCollection(date);
        console.log('Successfully created new collection.');
    }
    catch (err) {
        const database = client.db('virusDB');
        if(err.code === 48) {
            console.log('Duplicate collection: deleting...');
            await database.collection(date).drop();
            console.log('Duplicate collection successfully deleted. Creating new collection...');
            await database.createCollection(date);
            console.log('New collection successfully created.');
        }
        else { console.error(err) }
    }
}

async function insertDailyData() {
    const date = new Date().toISOString().split('T')[0];
    try {
        const database = client.db('virusDB');
        const docArray = await fetchAllStates();
        await database.collection(date).insertMany(docArray);
        console.log('Data successfully inserted.');
    }
    catch (err) { console.error(err) }
}

async function updateDaily() {
    let date = 20210118;
    await connect();
    await createCollection(date);
    await insertDailyData();
    await close();
}

//updateDaily();
//============================================================


//============================================================
//collection name is dateString. Make this the current date
//since every record is from the previous day, dateInt is 1 day older
const dateString = '2021-01-16';
const dateInt = 20210115;

async function insertHistoricalData(dateInt, dateString) {
    try {
        const database = client.db('virusDB');
        const docArray = await fetchStatesHistorical(dateInt);
        await database.collection(dateString).insertMany(docArray);
        console.log(`Data of date ${dateInt} successfully inserted into collection ${dateString}.`);
    }
    catch (err) { console.error(err) }
}

async function updateHistoricalRecord(dateInt, dateString) {
    await connect();
    await createCollection(dateString);
    await insertHistoricalData(dateInt, dateString);
    await close();
}

//updateHistoricalRecord(dateInt, dateString);
//============================================================





async function insertHistoricalRecord(date, array) {
    try {
        const database = client.db('virusDB');
        await database.collection(date).insertMany(array);
        console.log(`Data of date ${dateInt} successfully inserted.`);
    }
    catch (err) { console.error(err) }
}



function decrementDate(date) {
    if (date.toString().includes('20210101')) {
        return 20201231;
    }
    //November
    else if (date.toString().includes('20201201')) { return 20201130 }
    //October
    else if (date.toString().includes('20201101')) { return 20201031 }
    //Sept
    else if (date.toString().includes('20201001')) { return 20200930 }
    //Aug
    else if (date.toString().includes('20200901')) { return 20200831 }
    //Jul
    else if (date.toString().includes('20200801')) { return 20200731 }
    //Jun
    else if (date.toString().includes('20200701')) { return 20200630 }
    //May
    else if (date.toString().includes('20200601')) { return 20200531 }
    //Apr
    else if (date.toString().includes('20200501')) { return 20200430 }
    //Mar
    else if(date.toString().includes('20200401')) { return 20200331 }
    //Feb
    else if (date.toString().includes('20200301')) { return 20200228 }
    //Jan
    else if (date.toString().includes('20200201')) { return 20200131 } 
    else { return --date }
}

async function insertHistoricalAllDates(date) {
    var date = 20210121;
    const endDate = 20200111;
    await connect();
    const dataArray = await fetchStatesHistoricalAllDates();
    const database = client.db('virusDB');
    
    let t0 = performance.now();
    
    while (date != 20200111) {
        console.log(date);
        await createCollection(date.toString());
        const collection = database.collection(date.toString());
        Promise.all(dataArray.map(async function(element) {
            let i = 0;
            while (i < dataArray.length) {
                if (element.date === date) {
                    await collection.insertOne(element);
                    dataArray.splice(i, 1);
                }
                else { ++i }
            }
        }))
        date = decrementDate(date);
    }
    
    let t1 = performance.now();
    console.log(`Length: ${dataArray.length}`);
    console.log(`Done! Time taken: ${(t1-t0)/1000}s.`);
    await close();
}
//insertHistoricalAllDates();


async function resetDb() {
    await connect();
    const database = client.db('virusDB');
    await database.dropDatabase();
    console.log('Database deleting...wait a minute.');
    await close();
}

resetDb();

