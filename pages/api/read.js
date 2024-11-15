// pages/api/read.js
import { MongoClient } from 'mongodb';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ message: 'Method not allowed' });
  }

  try {
    const client = await MongoClient.connect(process.env.MONGODB_URI);
    const db = client.db('message2u');
    const collection = db.collection('messages');
    
    const data = await collection.find({}).toArray();
    
    await client.close();
    
    res.status(200).json(data);
  } catch (error) {
    res.status(500).json({ message: 'Error connecting to database' });
  }
}
