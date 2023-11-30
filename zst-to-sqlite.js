const fs = require('fs');
const path = require('path');
const zstd = require ('node-zstandard');
const sqlite3 = require('sqlite3').verbose();
const readline = require('readline');




// Global vars
const LIST_OF_ZST_FILES_OBJS = []; // filename, tableName, subredditName
const LIST_OF_JSON_FILES_OBJS = [];  // filepath, tableName, subredditName
let DIRECTORY_PATH = __dirname; // Change it if you want to use a different directory
let SMALLEST_FILE_OBJ = null; // Used to infer schema {comments: file, submissions: file}
let SCHEMAS = {}; // {tableName: schema}
const ERRORS = {totalJSONLines: 0, totalJSONErrors: 0, totalDBLines: 0, totalDBErrors: 0};


// Initializing DB and query builder
const db = new sqlite3.Database(`${DIRECTORY_PATH}/reddit.db`);

const knex = require('knex')({
    client: 'sqlite3',
    connection: {
        filename: `${DIRECTORY_PATH}/reddit.db`
    },
    useNullAsDefault: true
})




async function main() {
    // Make a list of the zst files
    const localdir = fs.readdirSync(DIRECTORY_PATH);
    localdir.forEach((file) => {
        if (file.endsWith('.zst')) {
            const filename = file;
            const tableName =  file.split('.')[0].split('_')[1];
            const subredditName = file.split('.')[0].split('_')[0];

            LIST_OF_ZST_FILES_OBJS.push({
                filename,
                tableName,
                subredditName
            })
        }
    });

    // Create a ./decompressed directory if it doesn't exist
    if (!fs.existsSync(`${DIRECTORY_PATH}/decompressed`)) {
        fs.mkdirSync(`${DIRECTORY_PATH}/decompressed`);
    }

    // Decompress such files into ./decompressed. We'll launch all decompress ops in parallel and wait until all of em are resolved.
    const decompressPromises = [];
    for (const file of [LIST_OF_ZST_FILES_OBJS]) {
        console.log(`Decompressing file: ${file.filename}`);

        decompressPromises.push(decompressFile(file));

        LIST_OF_JSON_FILES_OBJS.push({
            filepath: `${DIRECTORY_PATH}/decompressed/${file.subredditName}_${file.tableName}.json`,
            tableName: file.tableName,
            subredditName: file.subredditName
        })
    }

    await Promise.all(decompressPromises);


    // Locate the smallest file in terms of weight
    SMALLEST_FILE_OBJ = getTheSmallestFile(`${DIRECTORY_PATH}/decompressed/`);

    // Infer schema from the first line of the smallet file
    try {
        for (const [column, file_for_schema] of Object.entries(SMALLEST_FILE_OBJ)) {
            const buffer = Buffer.alloc(1024 * 1024);
            const fileDescriptor = fs.openSync(file_for_schema, 'r');
            fs.readSync(fileDescriptor, buffer, 0, buffer.length, 0);
            const content = buffer.toString('utf8');
            const firstLine = content.split('\n')[0];
            const obj = JSON.parse(firstLine);
            SCHEMAS[column] = getSchemaFromFile(obj);
        }
    } catch (error) {
        console.error('Error inferring schema: ', error);
    }

    // Create the tables with the inferred schemas
    for (const [tableName, schema] of Object.entries(SCHEMAS)) {
        await createTable(tableName, schema);
    }

    // Insert data into the tables
    for (const file of LIST_OF_JSON_FILES_OBJS) {
        await streamJsonToDatabase(file);
    }

    console.log(`Job finished:
    JSON: Total lines: ${ERRORS.totalJSONLines}, Total errors: ${ERRORS.totalJSONErrors}, Failure ratio: ${(ERRORS.totalJSONErrors / ERRORS.totalJSONLines).toFixed(2)}
    DB: Total lines: ${ERRORS.totalDBLines}, Total errors: ${ERRORS.totalDBErrors}, Failure ratio: ${(ERRORS.totalDBErrors / ERRORS.totalDBLines).toFixed(2)}`);


}

/**
 * Asynchronously decompresses a zst file into ./decompressed with .json extension
 *
 * @param {string} file - The file to decompress.
 * @return {Promise} A promise that resolves with the decompressed file.
 */
async function decompressFile(file) {
    return new Promise((resolve, reject) => {
        zstd.decompress(
            `${DIRECTORY_PATH}/${file.filename}`,
            `${DIRECTORY_PATH}/decompressed/${file.subredditName}_${file.tableName}.json`,
            (err, result) => {
            if (err) {
                NUM_OF_PARSING_ERRORS += 1;
                console.error('Error decompressing file: ', err);
            }
            console.log('File decompressed successfully: ', result);
            resolve(result);
        });
    })
}

/**
 * Retrieves the smallest file in the specified directory.
 *
 * @param {string} directory - The directory path to search for the smallest file.
 * @return {object} - An object containing the paths of the smallest files for 'comments' and 'submissions'.
 */
function getTheSmallestFile(directory) {
    const smallestFile = {
        'comments': null,
        'submissions': null
    }
    let smalletSizeForComments = Infinity;
    let smallestSizeForSubmissions = Infinity;
    const files = fs.readdirSync(directory);

    for (file of files) {
        const filePath = path.join(directory, file);
        const fileName = path.basename(filePath);
        const stats = fs.statSync(filePath);

        if (stats.isFile() && fileName.includes('comments') && stats.size < smalletSizeForComments) {
            smallestFile.comments = filePath;
            smalletSizeForComments = stats.size;
        }

        if (stats.isFile() && fileName.includes('submissions') && stats.size < smallestSizeForSubmissions) {
            smallestFile.submissions = filePath;
            smallestSizeForSubmissions = stats.size;
        }
    }
    
    return smallestFile;
}

/**
 * Generates a schema based on the properties of the given object.
 *
 * @param {Object} objeto - The object to generate the schema from.
 * @return {Array} An array of objects representing the schema. Each object contains a name property 
 * and a type property.
 */
function getSchemaFromFile(objeto) {
    const schema = [];
    for (const key in objeto) {
      if (Object.prototype.hasOwnProperty.call(objeto, key)) {
        let type = typeof objeto[key];
        switch (type) {
            case 'number':
                type = Number.isInteger(objeto[key]) ? 'INTEGER' : 'REAL';
                break;
            case 'boolean':
                type = 'INTEGER'; // SQLite no tiene un tipo boolean, por lo que se usa INTEGER
                break;
            default:
                type = 'TEXT';
                break;
        }
        schema.push({ name: key, type: type });
      }
    }
    return schema;
}

/**
 * Creates a table in the database with the given name and schema.
 *
 * @param {string} tableName - The name of the table to be created.
 * @param {Array} schema - An array containing the columns of the table and their types.
 * @return {Promise} A promise that resolves when the table is created successfully.
 */
async function createTable(tableName, schema) {
    return new Promise((resolve, reject) => {

        const schemaString = schema.map((column) => `${column.name} ${column.type}`).join(", ");

        db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (${schemaString})`, (error) => {
            if (error) {
                console.error(`Error creating table: ${tableName}\n\n${error}`);
            }
            resolve();
        })
    })
}

/**
 * Asynchronously streams a JSON file to a database and inserts each line as a separate entry.
 *
 * @param {Object} file - The file object containing the filepath and the tableName.
 * @return {Promise} A promise that resolves when the file has been successfully streamed and inserted into the database.
 */
async function streamJsonToDatabase(file) {
    const fileStream = fs.createReadStream(file.filepath, 'utf8');
    const lineStream = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    console.log('Straming JSON to DB, be patient, this may take a while...');
    for await (const line of lineStream) {
        const thisLine = await parseJsonLine(line);
        if (thisLine == null) {continue;}
        await insertLineInDB(thisLine, file.tableName);

    }
}

async function parseJsonLine(line) {
    return new Promise((resolve, reject) => {
        try {
            const obj = JSON.parse(line);
            ERRORS.totalJSONLines += 1;
            resolve(obj);
        } catch (error) {
            console.error(`Error parsing line: ${error}\n\n`);
            ERRORS.totalJSONLines += 1;
            ERRORS.totalJSONErrors += 1;
            resolve(null);
        }
    })
}

async function insertLineInDB(line, tableName) {
    // Get the SQLite schema for tableName
    const thisSchema = SCHEMAS[`${tableName}`];

    // Match the line with the schema. We do this because sometimes we don't infer the schema correctly and we need to make a compromise, so the inferred schema is the source of truth.
    let lineMatchedToSchema = {};
    for (const column of thisSchema) {

        let tempValue = line[`${column.name}`];
        try {
            switch (typeof(tempValue)) {
                case 'number':
                case 'bigint':
                    tempValue = tempValue;
                    break;
                case 'boolean':
                    tempValue = tempValue ? 1 : 0;
                    break;
                default:
                    tempValue = JSON.stringify(tempValue);
            }

            lineMatchedToSchema[`${column.name}`] = tempValue;

        } catch (error) {
            lineMatchedToSchema[`${column.name}`] = null;
        }
    }

    try {
        await knex(`${tableName}`).insert(lineMatchedToSchema);
        ERRORS.totalDBLines += 1;
    } catch (error) {
        console.error(`Error inserting line: ${line}\n${error.message}\n\n`);
        ERRORS.totalDBErrors += 1;
        ERRORS.totalDBLines += 1;
    }
}

main();