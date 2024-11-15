// ## /api/query.js
// ## GET /api/read?db=mydb&collection=mycollection&name=John&fields=name,email  -->  To query collection filtering name by regex and displaying only name, email fields
// ## GET /api/read?db=mydb&collection=mycollection&arrayField=value             -->  filter inside a 2D array for a specific element:

const { MongoClient } = require('mongodb');

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    return res.status(405).json({ status: 'err', message: 'Method Not Allowed', error: {} });
  }

  const { db, collection, fields, ...filters } = req.query;

  if (!db || !collection) {
    return res.status(400).json({ status: 'err', message: 'Missing "db" or "collection" parameter', error: {} });
  }

  try {
    await client.connect();
    const adminDb = client.db().admin();

    // Check if the database exists
    const databases = await adminDb.listDatabases();
    const dbExists = databases.databases.some(database => database.name === db);

    if (!dbExists) {
      await client.close();
      return res.status(404).json({ status: 'err', message: `Database "${db}" not found`, error: {} });
    }

    const targetDb = client.db(db);

    // Check if the collection exists
    const collections = await targetDb.listCollections().toArray();
    const collectionExists = collections.some(col => col.name === collection);

    if (!collectionExists) {
      await client.close();
      return res.status(404).json({ status: 'err', message: `Collection "${collection}" not found in database "${db}"`, error: {} });
    }

    const targetCollection = targetDb.collection(collection);

    // Modify filters dynamically
    const mongoFilters = {};
    Object.keys(filters).forEach((key) => {
      const value = filters[key];   console.log(`  filtering key = ${key} ....  value = ${value}  >>  `, filters);

      // Handle array element search (e.g., benefits[].feature=cooling system)
      if (key.includes('[].')) {
        const [arrayField, nestedField] = key.split('[].'); // e.g. 'benefits[].feature' -> ['benefits', 'feature']
        mongoFilters[arrayField] = { $elemMatch: { [nestedField]: { $regex: new RegExp(value, 'i') } } };
      } else if (value.includes('%3E') || value.includes('gte')) { // Handle "greater than" conditions (encoded as %3E for ">")
        const actualValue = Number(value.split('%3E')[1] || value.split('gte')[1]);
        mongoFilters[key] = { $gte: actualValue };
      } else if (value.includes('%3C') || value.includes('lte')) { // Handle "less than" conditions (encoded as %3C for "<")
        const actualValue = Number(value.split('%3C')[1] || value.split('lte')[1]);
        mongoFilters[key] = { $lte: actualValue };      console.log(`  filtering value = ${value} ....  actualValue = ${actualValue} .... `);
      } else if (value === 'true' || value === 'false') { // Boolean conversion
        mongoFilters[key] = value === 'true';
      } else if (value.match(/^\d+$/)) { // Convert numbers
        mongoFilters[key] = Number(value);
      } else {
        mongoFilters[key] = { $regex: new RegExp(value, 'i') }; // Default to regex for string matching
      }
    });

    // Projection: Specify which fields to return
    let projection = {};
    if (fields) {
      const fieldList = fields.split(','); // e.g. "name,email" -> ['name', 'email']
      fieldList.forEach(field => {
        projection[field.trim()] = 1;
      });
      // projection._id = 0;     // Optionally exclude _id
    }

    const documents = await targetCollection.find(mongoFilters, { projection }).toArray();

    await client.close();

    res.status(200).json({
      status: 'ok',
      message: 'Success',
      data: documents
    });
  } catch (error) {
    console.error('Error in GET /read:', error);
    res.status(500).json({
      status: 'err',
      message: 'Fail',
      error: error.message
    });
  }
};
