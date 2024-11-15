// /api/testapi.js

require('dotenv').config(); // Load environment variables from .env
const { MongoClient } = require('mongodb');

const uri = process.env.MONGODB_URI; // Ensure this is set in Vercel Environment Variables
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

module.exports = async (req, res) => {
  try {
    // Attempt to connect to MongoDB Atlas
    await client.connect();
    const db = client.db('testdb'); // Replace 'testdb' with your database name
    const collection = db.collection('items'); // Replace 'items' with your collection name

    // Optionally, perform a simple operation like listing collections
    const collections = await db.collections();
    const collectionNames = collections.map(col => col.collectionName);

    res.status(200).json({
      success: true,
      message: "Connected to MongoDB Atlas successfully!",
      collections: collectionNames
    });

    // Close the connection after the operation
    await client.close();
  } catch (error) {
    console.error('MongoDB connection error:', error);
    res.status(500).json({
      success: false,
      message: "Failed to connect to MongoDB Atlas.",
      error: error.message
    });
  }
};
