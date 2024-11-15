const https = require('https');
const readline = require('readline');
const fs    = require('fs');
const path  = require('path');


const BATCH_SIZE = 20; // batch size to insert db records
const TEMP_FILE_PATH = path.join(__dirname, 'temp.csv'); // Temporary file path for CSV

const { MongoClient } = require('mongodb');
const uri = process.env.MONGODB_URI;
const mongoClient = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
  //  mongoClient.connect();    // as Task Queue is added, so when run API, just make sure mongo is connect (NOT need close mongo, as task queue run to insert records.)

// Main handler function for file upload and CSV processing
const formidable  = require('formidable');


// ## VERCEL fail on REDIS  -->   "message": "Redis connection error", "data": "connect ECONNREFUSED 127.0.0.1:6379"
//    VERCEL serverless environments do not have a local Redis server running at localhost.pls connect to an external Redis service, not localhost
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


// Main handler function for file upload and CSV processing
async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ status: 'err', message: 'Only POST method allowed' });
  }

  return await taskQueue(req, res);
}


/* // VERCEL will hit error:  SyntaxError: Unexpected token 'export' at wrapSafe ...   -->  Vercel’s serverless functions use CommonJS syntax (require and module.exports) rather than ES Module syntax (import and export).
export default async function handler_v1 (req, res) {
  // input param (RAW JSON) :  db, collection, url
  // input param (form-data):  db, collection, file

  // const { db, collection, nowait } = req.body || {};   -->  not support form-data in POSTMAN
  // const resFile = await processingFile(req, res, db, collection, client); // Pass client

  if (req.method !== 'POST') {
    return res.status(405).json({ status: 'err', message: 'Only POST method allowed' });
  }

  const form = new formidable.IncomingForm();   // can access req.body, via form.fields param. eg:  const { db, collection, url } = fields;
  form.parse(req, async (err, fields, files) => {
    const { db, collection, nowait } = fields;
    if (!db || !collection) {   return res.status(400).json({ status: 'err', message: 'Missing "db" or "collection" in form data' }); }
    if (err) {  return res.status(500).json({ status: 'err', message: 'Error parsing form-data handler.', error: err.message });  }

    try {
      const resCheck = await checkDbCollectionExist(db, collection);    // inside function got mongoClient open and close connection.
      if (resCheck.status === 'err') {  return res.status(400).json(resCheck);  }

      await mongoClient.connect();    // should call AFTER checkDbCollectionExist()

      const result = await processingFile({ body: fields, files }, res, db, collection, mongoClient);
      return res.status(200).json(result);

    } catch (error) {
       return res.status(500).json({ status: 'err', message: 'Database connection error', data: error.message });

    } finally {
       await mongoClient.close();
    }
  });
} //*/


// To implement a Redis task queue for this API, will push the task to Redis upon receiving the request and return a taskid immediately.
// Redis store the task and a separate background worker (which we’ll outline as well) can then pick it up for processing asynchronously, updating the task status as it completes.
const taskQueue = async (req, res) => {
    const form = new formidable.IncomingForm();

    form.parse(req, async (err, fields, files) => {
      // ---
      const { db, collection, nowait, taskid } = fields;
      if (!db || !collection) {
        return res.status(400).json({ status: 'err', message: 'Missing "db" or "collection" in form data' });
      }
      if (err) {
        return res.status(500).json({ status: 'err', message: 'Error parsing form-data handler.', error: err.message });
      }

      // ## If got param 'taskid', so check task status.
      if( taskid ) {
          var currTask = await redis_read(taskid);      // not use redis, so not need check redis connection
          console.log(` DEBUG redis_read(), content >> `,  currTask);

          return res.status(200).json({ status: 'ok', message: `Task status for ID = ${taskid}`, data: currTask });
      }


      try {
        // ## 1.1 - check or validate input
        const resCheck = await checkDbCollectionExist(db, collection);
        if (resCheck.status === 'err') {
          return res.status(400).json(resCheck);
        }


        // ## Handle Connection
        //await mongoClient.connect();    // should call AFTER checkDbCollectionExist()
        //await redisClient.connect();


        // ---------
        // ## 2.1 - Start Task, check status later, so NOT use await for waiting ...
        let csvfile = req.body.url || '';
        if ( !csvfile ) {
            return res.status(400).json({ status: 'err', message: 'Empty param url.' });
        }
        /*  ## DISABLE, as VERCEL not allow to create file, after exit function.
        let resFile = await processingFile({ body: fields, files }, res, db, collection, mongoClient);
        if( resFile.status == 'ok' ) {
            // Pass the connected client to the extraction function
            csvfile = resFile.data['filePath'] || '';    console.log(`taskQueue() - file uploaded at ${csvfile}. >> `, resFile.data);
            //extractColumnsFromCsv(csvfile, db, collection, mongoClient);

            //var resCsv = await extractColumnsFromCsv(csvfile, db, collection, mongoClient);   // VERCEL hit error: FUNCTION_INVOCATION_TIMEOUT,  not need use "await"
            //return res.status(200).json(resFile);
        }   // */



        // ---------
        // ## 3.1 - Task Creation
        const taskid_new = `ID_${Date.now()}`;
        const task = {
            taskid: taskid_new,
            createtime: Date.now().toString(),
            filepath: csvfile,   // files.filepath || '',  // Assuming `filepath` is a property of `files`
            db: db,
            collection: collection,
            status: 'pending',
            updatetime: ''
        };

        // Save task to Redis queue
        // const redisKey = `filetask_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}`;
        // await redisClient.lPush(redisKey, JSON.stringify(task));
        await redis_write(task, taskid_new);

        // ## 3.2 -
        // var currTask = await redis_read(taskid_new);    //console.log(` DEBUG redis_read(), content >> `,  currTask);
        //                await redis_write({ ...currTask, status: 'completed', updatetime: Date.now().toString() });


        // Return taskid to client to check back status
        return res.status(200).json({ status: 'ok', message: 'Task queued for processing', data: task });

      } catch (error) {
        return res.status(500).json({ status: 'err', message: 'Task Queue error', data: error.message });

      } finally {
         //await mongoClient.close();     // do NOT close connection, as mongo still connect to insert records.
         //await redisClient.quit();    // Close the Redis connection
      }
    });
}


// -------------
// # REMARK:  VERCEL create TMP file will frequently REMOVE file, so those functions below UNABLE to read data, as ALWAYS get back EMPTY data.
const TASKS_FILE_PATH = path.join('/tmp', 'tasks.json');   // Path to store tasks locally
const TASKS_DEBUG     = true;   // PROD set false

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
      await redisClient.quit();   // Disconnect after use in serverless
    }
}

// Simulated Redis write: Append a new task to the tasks file
async function redis_write_file_(task) {
  let tasks = [];

  // Read the existing tasks if the file exists
  if (fs.existsSync(TASKS_FILE_PATH)) {
    const data = await fs.readFileSync(TASKS_FILE_PATH, 'utf8');
    tasks = JSON.parse(data);
    if(TASKS_DEBUG) {   console.log(`redis_write( 1 ) - file path ${TASKS_FILE_PATH}, READ FULL data >> `, tasks);    }
  }

  // Append the new task to the array
  if(TASKS_DEBUG) {   console.log(`redis_write( 2 ), WRITE NEW task >> `,  task);    }
  tasks.push(task);

  // Write updated tasks back to the file
  fs.writeFileSync(TASKS_FILE_PATH, JSON.stringify(tasks, null, 2), 'utf8');
}

// Simulated Redis read: Retrieve the task with the specified taskid
async function redis_read_file_(taskid) {
  if (fs.existsSync(TASKS_FILE_PATH)) {
    const data  = await fs.readFileSync(TASKS_FILE_PATH, 'utf8');
    const tasks = JSON.parse(data);
    if(TASKS_DEBUG) {   console.log(`redis_read( 1 ) - file path ${TASKS_FILE_PATH}, READ FULL data >> `, tasks);    }

    // Find and return the task object that matches the taskid
    var result = tasks.find(task => task.id === taskid) || null;    // if find taskid, return as array.
    // var find   = result  ?  result[0]  :  null;                     // return object not array.
    if(TASKS_DEBUG) {   console.log(`redis_read( 2 ) - find TASK-ID ${taskid}, READ data >> `, result);    }
    return result;
  }
  return {}; // Return null if no tasks file exists or task not found
}

// Simulated Redis read: Read all tasks from the tasks file
async function redis_readAll(taskid) {
  if (fs.existsSync(TASKS_FILE_PATH)) {
    const data = fs.readFileSync(TASKS_FILE_PATH, 'utf8');
    console.log(` redis_read() - file path ${TASKS_FILE_PATH},  data >> `,  data);
    return JSON.parse(data);
  }
  return []; // Return an empty array if no tasks file exists
}





// -------------
// ## INTERNAL FUNCTION

const checkDbCollectionExist = async (db, collection) => {
  if (!db || !collection) {
    return { status: 'err', message: 'Missing "db" or "collection" in request body' };
  }

  let result = { status: 'err', message: `Missing Collection [${collection}] not found in database [${db}].` };;
  await mongoClient.connect();
  const adminDb = mongoClient.db().admin();
  const dbs = await adminDb.listDatabases();
  const dbExists = dbs.databases.some(database => database.name === db);

  if (!dbExists) {
    result = { status: 'err', message: `Database "${db}" not found` };

  } else {
    const targetDb = mongoClient.db(db);
    const collections = await targetDb.listCollections().toArray();
    const collectionExists = collections.some(col => col.name === collection);

    if (!collectionExists) {
      result = { status: 'err', message: `Collection "${collection}" not found in database "${db}"` };
    } else {
      result = { status: 'ok', message: `Database "${db}" and Collection "${collection}" found.` };
    }
  }
  await mongoClient.close();
  return result;
};


const processingFile = async (req, res, db, collection) => {
  try {
    let filePath, fileType;

    // Download CSV from the provided URL
    if (req.body.url) {
      fileType = "FILE_URL";
      filePath = await downloadCsvFromUrl(req.body.url || '');

    } else if (req.files && req.files.file) {
      // Process the uploaded file
      const uploadedFile = req.files.file;
            fileType = "FILE_UPLOAD";

      // Use Vercel-compatible temporary path "/tmp"
      const tempDir = '/tmp';
      const destPath = path.join(tempDir, `data_fileupload_${Date.now()}.csv`);

      // Move the uploaded file from its temporary path to the defined path
      fs.copyFileSync(uploadedFile.filepath, destPath);
      filePath = destPath;

    } else {
      return { status: 'err', message: 'No URL or file provided' };
    }

    return { status: 'ok', message: 'File path success.', data: { type: fileType, filePath: filePath } };

  } catch (error) {
    return { status: 'err', message: 'Unexpected processing error at processingFile.', data: error.message };
  }
};


const processingFile_url = async (req, res, db, collection, client) => {
  try {
    let filePath;
    if (req.body.url) {
      filePath = await downloadCsvFromUrl(req.body.url);
    } else {
      return res.status(400).json({ status: 'err', message: 'No URL provided' });
    }

    // Pass the connected client to the extraction function
    return await extractColumnsFromCsv(filePath, db, collection, client);
  } catch (error) {
    return { status: 'err', message: 'Unexpected processing error', data: error.message };
  }
};


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
  let currentBatch = [];
  let lineNumber = 0, insertSuccess = 0;
  let trxndate = "", outlet = "", skipHeader = true;
  console.log(`extractColumnsFromCsv() - file uploaded at ${filePath} --->> start extract column .... `);

  // A queue to store batch processing promises
  const processingQueue = [];

  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    // -----
    // Helper function to process the current batch
    const processBatch = async (batch) => {
      try {
        let recInsert = await processCsv(batch, db, collection, client, { date: trxndate, outlet, skipHeader });

        // Set date and outlet after the first successful insertion
        if (insertSuccess === 0 && recInsert.count > 0) {
          skipHeader = false;
          trxndate = recInsert.date || '';
          outlet = recInsert.outlet || '';
        }

        insertSuccess += recInsert.count || 0;

      } catch (error) {
        reject(new Error(`Error processing batch at line ${lineNumber}: ${error.message}`));
      }
    };

    // -----
    rl.on('line', (line) => {
      lineNumber++;
      currentBatch.push(line);

      // When batch size is reached, add the batch to the queue
      if (currentBatch.length === BATCH_SIZE) {
        const batch = [...currentBatch]; // Copy current batch
        currentBatch = []; // Reset batch
        processingQueue.push(processBatch(batch)); // Add processing to queue
      }
    });

    rl.on('close', async () => {
      // Process any remaining lines in the final batch
      if (currentBatch.length > 0) {
        processingQueue.push(processBatch(currentBatch));
      }

      try {
        // Wait for all batches to complete
        await Promise.all(processingQueue);
        resolve({
          status: 'ok',
          message: `CSV processed. Total lines: ${lineNumber}. Records inserted: ${insertSuccess}.`,
          data: { inserted: insertSuccess }
        });
      } catch (error) {
        reject(new Error(`Error in batch processing: ${error.message}`));
      }
    });

    rl.on('error', (err) => reject(new Error(`Error reading CSV file: ${err.message}`)));
  });
};


// ## SOLUTION: Move Batch Processing Outside Event Listeners  ==>  Handle this is to pause the readline stream when a batch is being processed.
//              By do this calling rl.pause() while processCsv runs, and then resuming it with rl.resume() after each batch processes.
const extractColumnsFromCsv__Batch = async (filePath, db, collection, client) => {
  let currentBatch = [];
  let lineNumber = 0, insertSuccess = 0;
  let trxndate = "", outlet = "", skipHeader = true;
  console.log(`extractColumnsFromCsv() - file uploaded at ${filePath} --->> start extract column .... `);

  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    // -----
    // Helper function to process and reset the current batch
    const processBatch = async () => {
      if (currentBatch.length === 0) return;

      try {
        let recInsert = await processCsv(currentBatch, db, collection, client, { date: trxndate, outlet, skipHeader });

        // Set date and outlet after first successful insertion
        if (insertSuccess === 0 && recInsert.count > 0) {
          skipHeader = false;
          trxndate = recInsert.date || '';
          outlet = recInsert.outlet || '';
        }

        insertSuccess += recInsert.count || 0;
        currentBatch = []; // Reset batch

      } catch (error) {
        reject(new Error(`Error processing batch at line ${lineNumber}: ${error.message}`));
      }
    };

    // -----
    // Process each line, pushing to currentBatch
    rl.on('line', async (line) => {
      lineNumber++;
      currentBatch.push(line);

      if (currentBatch.length === BATCH_SIZE) {
        rl.pause(); // Pause reading the file
        await processBatch(); // Process the batch
        rl.resume(); // Resume reading the file
      }
    });

    rl.on('close', async () => {
      // Process any remaining lines in the final batch
      if (currentBatch.length > 0) {
        await processBatch();
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


// ## REMARK:  Using await inside an event listener doesn’t halt the readline stream. The readline library will continue reading lines without waiting for await processCsv() to complete, which might cause skipped logs or incomplete processing.
const extractColumnsFromCsv__Simple  = async (filePath, db, collection, client) => {
  let currentBatch = [];
  let lineNumber = 0, insertSuccess = 0;
  let trxndate = "", outlet = "",  skipHeader = true;
  console.log(`extractColumnsFromCsv() - file uploaded at ${filePath}  --->> start extract column .... `);

  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    rl.on('line', async (line) => {
      lineNumber++;
      currentBatch.push(line);

      if (currentBatch.length === BATCH_SIZE) {
        try {
          let recInsert = await processCsv(currentBatch, db, collection, client, { date: trxndate, outlet, skipHeader: skipHeader });     // Pass client

          // Get trxnDate + outlet   -->  first time mark down trxn date, outlet   >>  already get 2 column, mark skipHeader = false;
          if( insertSuccess == 0  &&  recInsert.count > 0 ) {    skipHeader = false;   trxndate = recInsert.date || '';  outlet = recInsert.outlet || '';   }

          currentBatch  = [];    // reset
          insertSuccess = insertSuccess + recInsert.count || 0;

        } catch (error) {
          reject(new Error(`Error processing batch at line ${lineNumber}: ${error.message}`));
        }
      }
    });

    rl.on('close', async () => {
      if (currentBatch.length > 0) {
        try {
          let recInsert = await processCsv(currentBatch, db, collection, client, { date: trxndate, outlet, skipHeader: skipHeader });
          currentBatch  = [];    // reset
          insertSuccess = insertSuccess + recInsert.count || 0;

        } catch (error) {
          return reject(new Error(`Error processing batch csv records: ${error.message}`));
        }
      }
      resolve({ status: 'ok', message: `CSV processed. Total lines: ${lineNumber}. Records inserted: ${insertSuccess}.`, data: { inserted: insertSuccess }  });
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
