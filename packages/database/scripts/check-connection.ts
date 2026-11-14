import {
  checkDatabaseConnection,
  closeDatabaseConnection,
} from '../src/index.js';

async function main(): Promise<void> {
  try {
    await checkDatabaseConnection();

    console.log('PostgreSQL connection successful!');
    console.log('Database responded to SELECT 1.');
  } catch (error) {
    console.error('PostgreSQL connection failed:', error);
    process.exitCode = 1;
  } finally {
    await closeDatabaseConnection();
  }
}

await main();