import { Aspect, defineAspects } from './operation';
import { CommandOperation, CommandOperationOptions } from './command';

export interface CollStatsOperationOptions extends CommandOperationOptions {
  scale?: any;
}

/**
 * Get all the collection statistics.
 *
 * @class
 * @property {Collection} collection Collection instance.
 * @property {object} [options] Optional settings. See Collection.prototype.stats for a list of options.
 */
class CollStatsOperation extends CommandOperation<CollStatsOperationOptions> {
  collectionName: string;

  /**
   * Construct a Stats operation.
   *
   * @param {Collection} collection Collection instance
   * @param {object} [options] Optional settings. See Collection.prototype.stats for a list of options.
   */
  constructor(collection: any, options?: CollStatsOperationOptions) {
    super(collection, options);
    this.collectionName = collection.collectionName;
  }

  execute(server: any, callback: Function) {
    const command: any = { collStats: this.collectionName };
    if (this.options.scale != null) {
      command.scale = this.options.scale;
    }

    super.executeCommand(server, command, callback);
  }
}

export interface DbStatsOperationOptions extends CommandOperationOptions {
  scale?: any;
}

class DbStatsOperation extends CommandOperation<DbStatsOperationOptions> {
  execute(server: any, callback: Function) {
    const command: any = { dbStats: true };
    if (this.options?.scale != null) {
      command.scale = this.options.scale;
    }

    super.executeCommand(server, command, callback);
  }
}

defineAspects(CollStatsOperation, [Aspect.READ_OPERATION, Aspect.EXECUTE_WITH_SELECTION]);
defineAspects(DbStatsOperation, [Aspect.READ_OPERATION, Aspect.EXECUTE_WITH_SELECTION]);
export { DbStatsOperation, CollStatsOperation };
