// /api/update.js

const { MongoClient, ObjectId } = require('mongodb');

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

    const updateResults = [];

    for (const update of items) {
      const { id, ...fieldsToUpdate } = update;

      if (!id) {
        updateResults.push({ status: 'err', message: 'Missing "id" in update object', error: {} });
        continue;
      }

      try {
        const result = await targetCollection.updateOne(
          { _id: ObjectId(id) },
          { $set: fieldsToUpdate }
        );

        if (result.matchedCount === 0) {
          updateResults.push({ status: 'err', message: `No document found with id "${id}"`, error: {} });
        } else {
          updateResults.push({ status: 'ok', message: 'Success', data: { matchedCount: result.matchedCount, modifiedCount: result.modifiedCount } });
        }
      } catch (err) {
        updateResults.push({ status: 'err', message: err.message, error: {} });
      }
    }

    await client.close();

    res.status(200).json({
      status: 'ok',
      message: 'Success',
      data: updateResults
    });
  } catch (error) {
    console.error('Error in POST /update:', error);
    res.status(500).json({
      status: 'err',
      message: 'Fail',
      error: error.message
    });
  }
};
