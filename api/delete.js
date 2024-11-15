// 2024-10-28: Enhance code to delete based on id, or whole collection (param: deleteAll = "message2u")

const { MongoClient, ObjectId } = require('mongodb');

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ status: 'err', message: 'Method Not Allowed', error: {} });
  }

  const { db, collection, items, trxndate, deleteAll } = req.body;

  if (!db || !collection || (!items && !deleteAll)) {
    return res.status(400).json({ status: 'err', message: 'Missing "db", "collection", and either "items", or "deleteAll" in request body', error: {} });
  }

  if (items && !Array.isArray(items)) {
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

    // Determine the deletion query based on provided parameters
    let query = {};
    if (deleteAll == "message2u") {
      query = {};  // Empty query will delete all documents

    } else if (items) {
      const objectitems = items.map(id => {
        try {
          return ObjectId(id);
        } catch (error) {
          return null;
        }
      }).filter(id => id !== null);

      if (objectitems.length === 0) {
        await client.close();
        return res.status(400).json({ status: 'err', message: 'No valid items provided', error: {} });
      }

      query = { _id: { $in: objectitems } };
    } else if (trxndate) {
      query = { trxndate: trxndate };
    }

    // Execute delete operation based on the constructed query
    const result = await targetCollection.deleteMany(query);

    await client.close();

    res.status(200).json({
      status: 'ok',
      message: 'Success',
      data: {
        deletedCount: result.deletedCount
      }
    });
  } catch (error) {
    console.error('Error in POST /delete:', error);
    res.status(500).json({
      status: 'err',
      message: 'Fail',
      error: error.message
    });
  }
};
