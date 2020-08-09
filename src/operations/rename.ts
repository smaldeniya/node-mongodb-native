import { checkCollectionName } from '../utils';
import { loadCollection } from '../dynamic_loaders';
import { RunAdminCommandOperation } from './run_command';
import { defineAspects, Aspect } from './operation';
import type { Callback } from '../types';
import type { Server } from '../sdam/server';
import type { Collection } from '../collection';
import type { CommandOperationOptions } from './command';
import { MongoError } from '../error';

export interface RenameOptions extends CommandOperationOptions {
  /** Drop the target name collection if it previously exists. */
  dropTarget?: boolean;
  /** Unclear */
  new_collection?: boolean;
}

export class RenameOperation extends RunAdminCommandOperation {
  collection: Collection;
  newName: string;

  constructor(collection: Collection, newName: string, options: RenameOptions) {
    // Check the collection name
    checkCollectionName(newName);

    // Build the command
    const renameCollection = collection.namespace;
    const toCollection = collection.s.namespace.withCollection(newName).toString();
    const dropTarget = typeof options.dropTarget === 'boolean' ? options.dropTarget : false;
    const cmd = { renameCollection: renameCollection, to: toCollection, dropTarget: dropTarget };
    super(collection, cmd, options);

    this.collection = collection;
    this.newName = newName;
  }

  execute(server: Server, callback: Callback<Collection>): void {
    const Collection = loadCollection();
    const coll = this.collection;

    super.execute(server, (err, doc) => {
      if (err) return callback(err);
      // We have an error
      if (doc.errmsg) {
        return callback(new MongoError(doc));
      }

      try {
        return callback(
          undefined,
          new Collection(
            coll.s.db,
            coll.s.topology,
            coll.s.namespace.db,
            this.newName,
            coll.s.pkFactory,
            coll.s.options
          )
        );
      } catch (err) {
        return callback(new MongoError(err));
      }
    });
  }
}

defineAspects(RenameOperation, [Aspect.WRITE_OPERATION, Aspect.EXECUTE_WITH_SELECTION]);
