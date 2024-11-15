const formidable = require('formidable');
const fs = require('fs');
const readline = require('readline');

// Helper function to manually parse each row based on index
const extractColumnsFromCsv = (filePath) => {
  let trxndate = '';

  return new Promise((resolve, reject) => {
    let extractedData = [];
    let lineNumber = 0;

    // Create a read stream to process CSV line by line
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity
    });

    rl.on('line', (line) => {
      lineNumber++;

      // Split the line by commas (handling CSV format)
      const row = line.split(',');

      // Log each line for debugging purposes
      console.log(`Line ${lineNumber}:`, row);

      // Skip header lines or invalid lines if needed
      if (row.length < 30) {    // total 30 columns expected
        console.log(`Skipping line ${lineNumber}: Not enough columns`);
        return; // Skip lines that don't match the expected column count
      }

      // Extract data from the defined columns
      if (!trxndate) {
        trxndate = row[3] || null;  // Set the first valid transaction date
      }

      const col = {
        'trxn_date': trxndate,        // Column D (Sales Date)
        'sku_id': row[9] || null,     // Column J (STOCK CODE)
        'sku_name': row[12] || null,  // Column M (DESCRIPTION)
        'qty': row[19] || null        // Column S (Qty)
      };

      // Log the filtered row before adding it to the array
      console.log('Filtered Row:', col);

      // Only push rows that have valid data
      if (col['sku_id'] && col['sku_name'] && col['qty']) {
        extractedData.push(col);
      }
    });

    rl.on('close', () => {
      console.log('CSV file processing complete.');
      resolve({ extract: extractedData, trxndate: trxndate, count: extractedData.length || 0 });  // Return extracted data when parsing is complete
    });

    rl.on('error', (err) => {
      console.error('Error while processing CSV file:', err);
      reject(err);  // Handle any errors
    });
  });
};

// Vercel API handler for file upload and CSV processing
export default async function handler(req, res) {
  if (req.method === 'POST') {
    const form = new formidable.IncomingForm();

    form.parse(req, (err, fields, files) => {
      if (err) {
        console.error('Error parsing file upload:', err);
        return res.status(500).json({ error: 'Error parsing the file upload' });
      }

      const filePath = files.file.filepath; // Get the path to the uploaded file
      console.log('File uploaded:', filePath);

      // Ensure the correct function name is called
      extractColumnsFromCsv(filePath)
        .then((data) => {
          console.log('Extracted Data:', data);
          // res.status(200).json({ extractedData: data });
          res.status(200).json({
            status: 'ok',
            message: `Csv extracted for date ${data.trxndate || ''}. Total rows is ${data.count || 0}`,
            data: data.extract || {}
          })
        })
        .catch((error) => {
          console.error('Error extracting columns from CSV:', error);
          res.status(500).json({ status: 'err', message: 'Error processing the CSV file', data: {} });
        });
    });
  } else {
    res.status(405).json({ status: 'err', message: 'Method not allowed', data: {} }); // Only allow POST requests
  }
}
