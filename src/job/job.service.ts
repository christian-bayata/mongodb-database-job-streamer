import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron, CronExpression } from '@nestjs/schedule';
import { MongoClient } from 'mongodb';

@Injectable()
export class JobService {
  constructor(private readonly configService: ConfigService) {}

  private logger = new Logger('Main');
  /**
   * @Responsibility: Cron job that runs every day by 12am to stream database collections/documents
   *
   * @returns {}
   */

  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT)
  async databaseStreamer() {
    try {
      /* Connect to both the staging and local databases */
      const [stagingDbClient, localDbClient] = await Promise.all([
        MongoClient.connect(this.databaseUrlLinks('staging')),
        MongoClient.connect(this.databaseUrlLinks('local')),
      ]);

      /* Retrieve all collection names form the staging database */
      const allStagingDbColNames = (
        await stagingDbClient.db().listCollections().toArray()
      ).map((col: any) => col?.name);

      /* Loop through the staging collections and perform the migration */
      const theLength: number = allStagingDbColNames.length;
      for (let i = 0; i < theLength; i++) {
        const collectionName = allStagingDbColNames[i];

        const theStagingCollection = stagingDbClient
          .db()
          .collection(collectionName);

        const theLocalCollection = localDbClient
          .db()
          .collection(collectionName);

        /* Ensure the local database has a collection before streaming into it */
        const localCollectionExists = await localDbClient
          .db()
          .listCollections({ name: collectionName })
          .hasNext();

        if (!localCollectionExists) {
          await localDbClient.db().createCollection(collectionName);
          this.logger.log(
            `Added collection to the local database: ${collectionName}`,
          );
        }

        /* Drop local database for fresh streaming */
        await theLocalCollection.drop();

        /* Move data from staging to local collection in streams */
        const stream = theStagingCollection.find().stream();

        stream.on('data', async (doc) => {
          await theLocalCollection.insertOne(doc);
        });

        stream.on('end', () => {
          this.logger.log(`Data migration completed for ${collectionName}`);
        });

        stream.on('error', (error) => {
          this.logger.log(`Data stream error: ${error}`);
        });

        /* After completion of data stream, move to the next collection */
        await new Promise((resolve) => stream.on('end', resolve));
      }

      stagingDbClient.close();
      localDbClient.close();
    } catch (error) {
      throw error;
    }
  }

  private databaseUrlLinks(key: string) {
    return {
      local: `${this.configService.get('LOCAL_DB_URL')}`,
      staging: `${this.configService.get('STAGING_DB_URL')}`,
    }[key];
  }
}
