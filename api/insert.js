// /api/insert.js

const { MongoClient } = require('mongodb');

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ status: 'err', message: 'Method Not Allowed', error: {} });
  }

  const { db, collection, items } = req.body;

  if (!db || !collection || !items) {
    return res.status(400).json({ status: 'err', message: 'Missing "db", "collection", or "items" in request body', error: {} });
  }

  if (!Array.isArray(items)) {
    return res.status(400).json({ status: 'err', message: '"items" must be an array', error: {} });
  }

  try {
    await client.connect();
    const adminDb = client.db().admin();

    // List all databases
    const databases = await adminDb.listDatabases();
    const dbExists = databases.databases.some(database => database.name === db);

    if (!dbExists) {
      await client.close();
      return res.status(404).json({ status: 'err', message: `Database "${db}" not found`, error: {} });
    }

    const targetDb = client.db(db);

    // List all collections in the specified database
    const collections = await targetDb.listCollections().toArray();
    const collectionExists = collections.some(col => col.name === collection);

    if (!collectionExists) {
      await client.close();
      return res.status(404).json({ status: 'err', message: `Collection "${collection}" not found in database "${db}"`, error: {} });
    }

    const targetCollection = targetDb.collection(collection);

    const result = await targetCollection.insertMany(items);

    await client.close();

    res.status(201).json({
      status: 'ok',
      message: 'Success',
      data: {
        insertedCount: result.insertedCount,
        insertedIds: result.insertedIds
      }
    });
  } catch (error) {
    console.error('Error in POST /insert:', error);
    res.status(500).json({
      status: 'err',
      message: 'Fail',
      error: error.message
    });
  }
};
