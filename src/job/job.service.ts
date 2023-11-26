import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron, CronExpression } from '@nestjs/schedule';

@Injectable()
export class JobService {
  constructor(private readonly configService: ConfigService) {}

  @Cron(CronExpression.EVERY_30_SECONDS)
  async databaseStreamer() {}
}
