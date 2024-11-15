const https = require('https');
const readline = require('readline');
const fs    = require('fs');
const path  = require('path');


const BATCH_SIZE = 20; // batch size to insert db records
const TEMP_FILE_PATH = path.join(__dirname, 'temp.csv'); // Temporary file path for CSV

const { MongoClient } = require('mongodb');
const uri = process.env.MONGODB_URI;
const mongoClient = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
    //mongoClient.connect();    // as Task Queue is added, so when run API, just make sure mongo is connect (NOT need close mongo, as task queue run to insert records.)

// Main handler function for file upload and CSV processing
// const formidable  = require('formidable');


// ## VERCEL fail on REDIS  -->   "message": "Redis connection error", "data": "connect ECONNREFUSED 127.0.0.1:6379"
//    VERCEL serverless environments do not have a local Redis server running at localhost.pls connect to an external Redis service, not localhost
const TASKS_DEBUG  = true;   // PROD set false
const REDIS_EXPIRE = 2 * 24 * 60 * 60;      // 2 days in seconds
const redis        = require('redis');      // REDIS_URL need register account under upstash.com (FREE Plan)  -->  // 'redis://localhost:6379',  WITH Password:  "redis://:your_password@localhost:6379"
const redisClient  = redis.createClient({
                        url: process.env.REDIS_URL || '',
                        socket: {
                            connectTimeout: 5000,     // Set a shorter connection timeout, e.g., 5 seconds
                            reconnectStrategy: (retries) => Math.min(retries * 50, 500) // Retry with an exponential backoff
                        }
                    });


// Export the handler using CommonJS syntax
module.exports = handler;




// -------------
async function handler(req, res) {
  // Your cron task logic here
  console.log("Cron job executed at", new Date().toISOString());

  var resTask = await taskQueue(req.body.taskid || '');      // not use redis, so not need check redis connection
  console.log(` cron::handler(), content >> `,  resTask);

  res.status(200).json({ message: "Cron job executed successfully", data: {} });
}





// To implement a Redis task queue for this API, will push the task to Redis upon receiving the request and return a taskid immediately.
// Redis store the task and a separate background worker (which we’ll outline as well) can then pick it up for processing asynchronously, updating the task status as it completes.
const taskQueue = async (taskid) => {
    try {
      if( !taskid ) {
        return { status: 'err', message: `No Task ID = ${taskid}`, data: {} };
      }

      // ## Handle Connection
      await mongoClient.connect();    
      //await redisClient.connect();

      var currTask = await redis_read(taskid);    //console.log(` DEBUG redis_read(), content >> `,  currTask);
      //             await redis_write({ ...currTask, status: 'completed', updatetime: Date.now().toString() });

      let filepath =  await downloadCsvFromUrl( currTask.filepath || '' );
      if(!filepath) {  return { status: 'err', message: 'File not found.', data: {} };   }

      var resCsv = await extractColumnsFromCsv(filepath, currTask.db, currTask.collection, mongoClient);
      console.log(`cron::taskQueue(), resCsv >> `,  resCsv);

      // Return taskid to client to check back status
      return { status: 'ok', message: 'Task completed', data: currTask };

    } catch (error) {
      return { status: 'err', message: 'Task Queue error', data: error.message };

    } finally {
       await mongoClient.close();     // do NOT close connection, as mongo still connect to insert records.
       //await redisClient.quit();    // Close the Redis connection
    }
}


// -------------
async function redis_write(task={}, id='') {
  try {
      await redisClient.connect();

      // Set the key with a value and an expiration of 2 days (in seconds)
      if(TASKS_DEBUG) {   console.log(`redis_write() - ID ${id} >> `, task);    }
      await redisClient.set(id, JSON.stringify(task), { EX: REDIS_EXPIRE });

    } catch (error) {
      console.error("redis_write() - Redis error >> ", error);

    } finally {
      await redisClient.quit();   // Disconnect after use in serverless
    }
}

async function redis_read(id='') {
  try {
      await redisClient.connect();

      var data = await redisClient.get(id);
      var task = JSON.parse(data);
      if(TASKS_DEBUG) {   console.log(`redis_read() -  ID ${id} >> >> `, task);    }
      return task || {};

    } catch (error) {
      console.error("redis_read() - Redis error >> ", error);

    } finally {
      //await redisClient.quit();   // Disconnect after use in serverless
    }
}



// -------------
// ## INTERNAL FUNCTION
const downloadCsvFromUrl = (url) => {
  const TEMP_FILE_PATH = path.join('/tmp', `data_fileurl_${Date.now()}.csv`);   // Use "/tmp" for Vercel compatibility, as Vercel BLOCK for file upload

  // Ensure the /tmp directory exists (Vercel provides it by default, but this is a safeguard)
  const dir = path.dirname(TEMP_FILE_PATH);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  const writer = fs.createWriteStream(TEMP_FILE_PATH);

  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      if (response.statusCode !== 200) {
        return reject(new Error(`Failed to get '${url}' (${response.statusCode})`));
      }

      response.pipe(writer);
      writer.on('finish', () => {
        writer.close(() => resolve(TEMP_FILE_PATH));
      });
      writer.on('error', (error) => {
        fs.unlink(TEMP_FILE_PATH, () => reject(error)); // Delete temp file on error
      });
    }).on('error', reject);
  });
};


const extractColumnsFromCsv = async (filePath, db, collection, client) => {
  const BATCH_SIZE = 1000; // Adjust as needed based on performance requirements
  let currentBatch = [];
  let lineNumber = 0;
  let insertSuccess = 0;
  let trxndate = "", outlet = "", skipHeader = true;

  console.log(`extractColumnsFromCsv() - file uploaded at ${filePath} --->> start extract column .... `);

  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    const processBatch = async () => {
      if (currentBatch.length === 0) return;

      try {
        let recInsert = await processCsv(currentBatch, db, collection, client, { date: trxndate, outlet, skipHeader });

        // Set date and outlet after the first successful insertion
        if (insertSuccess === 0 && recInsert.count > 0) {
          skipHeader = false;
          trxndate = recInsert.date || '';
          outlet = recInsert.outlet || '';
        }

        insertSuccess += recInsert.count || 0;
        currentBatch = []; // Reset batch after successful processing

      } catch (error) {
        reject(new Error(`Error processing batch at line ${lineNumber}: ${error.message}`));
      }
    };

    rl.on('line', async (line) => {
      lineNumber++;
      currentBatch.push(line);

      if (currentBatch.length === BATCH_SIZE) {
        rl.pause(); // Pause reading while processing the batch
        await processBatch(); // Process the batch
        rl.resume(); // Resume reading after batch is processed
      }
    });

    rl.on('close', async () => {
      if (currentBatch.length > 0) {
        await processBatch(); // Process any remaining lines in the last batch
      }

      resolve({
        status: 'ok',
        message: `CSV processed. Total lines: ${lineNumber}. Records inserted: ${insertSuccess}.`,
        data: { inserted: insertSuccess }
      });
    });

    rl.on('error', (err) => reject(new Error(`Error reading CSV file: ${err.message}`)));
  });
};


const processCsv = async (lines, db, collection, client, csvParam) => {
  let rows = [], successCnt = 0, linenum = 0, headerline = 3;
  let trxndate = null, outlet = null;

  lines.forEach((line) => {
    var row = line.split(',');  linenum++;   // Split the line by commas (handling CSV format)
    if( csvParam.skipHeader == true  &&  linenum <= headerline ) {
        console.log(` processCsv() - SKIP header at line ${linenum}/${headerline} ... `, row);
    } else {
      // IF Change CSV format, change here.
      if (!outlet)   {    outlet = row[1] || null;     }    // each csv file 4th row, column 2
      if (!trxndate) {    trxndate = row[4] || null;   }    // each csv file 4th row, column 5
      let skuid = row[8] || null,  skuname = row[9] || null,  qty = row[10] || null;

      if (skuid  &&  row[9]  &&  qty) {   // auto FILTER header and footer in csv
        var tmpline = {
          trxn_date:  csvParam.date   ? csvParam.date   :  (trxndate || ''),    // fix bug
          outlet:     csvParam.outlet ? csvParam.outlet :  (outlet  || ''),
          sku_code: skuid,
          sku_name: skuname,
          qty: qty,
          uom: row[11] || null,
          price_unit:  row[12] || null,
          disc_amt:  row[14] || null,
          sales_amt:  row[16] || null,
        }
        console.log(`processCsv() - row-${linenum}/${headerline}, skipHeader=${csvParam.skipHeader}, (${outlet}, ${trxndate}) >> `, tmpline);
        rows.push(tmpline);
      }
    }  // END IF
  });

  console.log(`processCsv() - total rows [${rows.length}] to insert by calling insertMongo() ... `);
  if (rows.length > 0) {
    try {
      const result = await insertMongo(rows, db, collection, client);
      successCnt = result.count || 0;
      console.log('processCsv() - Batch insert mongo successful: ', result.message);

    } catch (error) {
      console.error('Error during batch insert:', error.message);
      throw new Error(`Failed to insert items for batch starting with ${rows[0]?.trxn_date || 'unknown date'}`);
    }
  }
  return { count: successCnt, date: trxndate, outlet: outlet };
};


async function insertMongo(data, db, collection, client) {
  console.log("insertMongo() - start"); // Check if function is called
  // await testMongoConnection();

  try {
    // -----
    if (!client.topology || client.topology.isDestroyed()) {
      console.log('insertMongo() - attempting to open mongo connection ... ');
      // Set a connection timeout (e.g., 10 seconds)
      // To avoid indefinite waiting, you can set a timeout for the client.connect() call. If the timeout is reached without a successful connection, it will throw an error, helping identify issues.
      const connectTimeout = new Promise((_, reject) =>
         setTimeout(() => reject(new Error("MongoDB connection timeout")), 5000)
      );

      await Promise.race([client.connect(), connectTimeout]); // Attempts to connect with a timeout
      console.log("insertMongo() - MongoDB connection established.");

    } else {
      console.log("insertMongo() - MongoDB client already connected.");
    }

    // -----
    const targetCollection = client.db(db).collection(collection);
    // console.log("insertMongo() - inserting data:", data); // Log data to confirm it’s passed correctly

    const result = await targetCollection.insertMany(data);
    console.log("Items successfully inserted:", result.insertedCount);

    return { status: 'ok', message: `Inserted ${result.insertedCount} items to "${collection}"`, count: result.insertedCount };

  } catch (error) {
    console.error("Error in insertMongo:", error.message); // More specific error message
    throw new Error("Failed to insert items");
  }
}


async function testMongoConnection() {
  console.log(`testMongoConnection() - Mongo uri  >>  ${uri}`);
  const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true, serverSelectionTimeoutMS: 5000 });   // 5000 = 5 seconds
  try {
    await client.connect();
    console.log("testMongoConnection() - Mongo Connection successful!");
  } catch (error) {
    console.error("testMongoConnection() - Mongo Connection error:", error.message);
  } finally {
    await client.close();
    console.log("testMongoConnection() - Mongo Connection closed!");
  }
}



/*
async function insertMongo__v2 (data, db, collection, client) {
  try {
    if (!client.topology || client.topology.isDestroyed()) {
      console.log('insertMongo() - open mongo connection ... ');
      await client.connect();
    } else {
      console.log('insertMongo() - mongo client already connected');
    }

    const targetCollection = client.db(db).collection(collection);
    console.log('insertMongo() - inserting data:', data); // Log data to confirm it’s passed correctly
    const result = await targetCollection.insertMany(data);

    console.log('Items inserted:', result.insertedCount);
    return { status: 'ok', message: `Inserted ${result.insertedCount} items to "${collection}"`, count: result.insertedCount };

  } catch (error) {
    console.error('insertMongo() - Error inserting items:', error);
    throw new Error('Failed to insert items');
  }
}

async function insertMongo__v1 (data, db, collection, client) {
  try {
    if (!client.topology || client.topology.isDestroyed()) {
        console.log('insertMongo() - open mongo connection ... ');
        await client.connect();   // becoz Task Queue run in bakground, so make sure connection is active and connect
    }
    const targetCollection = client.db(db).collection(collection);
    const result = await targetCollection.insertMany(data);

    console.log('Items inserted:', result.insertedCount);
    return { status: 'ok', message: `Inserted ${result.insertedCount} items to "${collection}"`, count: result.insertedCount };

  } catch (error) {
    console.error('Error inserting items:', error);
    throw new Error('Failed to insert items');
  }
}

// */
