import { EventEmitter } from 'events';
import { GridFSBucketReadStream, GridFSBucketReadStreamOptions } from './download';
import { GridFSBucketWriteStream, GridFSBucketWriteStreamOptions, Id } from './upload';
import { executeLegacyOperation } from '../utils';
import { WriteConcernOptions, WriteConcern } from '../write_concern';
import type { Callback, AnyError } from '../types';
import type { Db, ReadPreference } from '..';
import type { Collection } from '../collection';
import type { Document } from './../types.d';
import type { Cursor } from './../cursor/cursor';
import type { FindOptions } from './../operations/find';
import { MongoError } from '../error';
import type { Logger } from '../logger';

const DEFAULT_GRIDFS_BUCKET_OPTIONS = {
  bucketName: 'fs',
  chunkSizeBytes: 255 * 1024
};

interface GridFSBucketOptions extends WriteConcernOptions {
  /** The 'files' and 'chunks' collections will be prefixed with the bucket name followed by a dot. */
  bucketName?: string;
  /** Number of bytes stored in each chunk. Defaults to 255KB */
  chunkSizeBytes?: number;
  /** Read preference to be passed to read operations */
  readPreference?: ReadPreference;
}

interface GridFSBucketPrivate {
  db: Db;
  options: {
    bucketName: string;
    chunkSizeBytes: number;
    readPreference?: ReadPreference;
    writeConcern: WriteConcern | undefined;
  };
  _chunksCollection: Collection;
  _filesCollection: Collection;
  checkedIndexes: boolean;
  calledOpenUploadStream: boolean;
}

/**
 * Constructor for a streaming GridFS interface
 *
 */
export class GridFSBucket extends EventEmitter {
  s: GridFSBucketPrivate;

  constructor(db: Db, options: GridFSBucketOptions) {
    super();
    this.setMaxListeners(0);
    const privateOptions = {
      ...DEFAULT_GRIDFS_BUCKET_OPTIONS,
      ...options,
      writeConcern: WriteConcern.fromOptions(options)
    };
    this.s = {
      db,
      options: privateOptions,
      _chunksCollection: db.collection(privateOptions.bucketName + '.chunks'),
      _filesCollection: db.collection(privateOptions.bucketName + '.files'),
      checkedIndexes: false,
      calledOpenUploadStream: false
    };
  }

  /**
   * When the first call to openUploadStream is made, the upload stream will
   * check to see if it needs to create the proper indexes on the chunks and
   * files collections. This event is fired either when 1) it determines that
   * no index creation is necessary, 2) when it successfully creates the
   * necessary indexes.
   *
   * @event GridFSBucket#index
   * @type {Error}
   */

  /**
   * Returns a writable stream (GridFSBucketWriteStream) for writing
   * buffers to GridFS. The stream's 'id' property contains the resulting
   * file's id.
   */
  openUploadStream(
    filename: string,
    options?: GridFSBucketWriteStreamOptions
  ): GridFSBucketWriteStream {
    return new GridFSBucketWriteStream(this, filename, options);
  }

  /**
   * Returns a writable stream (GridFSBucketWriteStream) for writing
   * buffers to GridFS for a custom file id. The stream's 'id' property contains the resulting
   * file's id.
   */
  openUploadStreamWithId(
    id: Id,
    filename: string,
    options?: GridFSBucketWriteStreamOptions
  ): GridFSBucketWriteStream {
    return new GridFSBucketWriteStream(this, filename, { ...options, id });
  }

  /** Returns a readable stream (GridFSBucketReadStream) for streaming file data from GridFS. */
  openDownloadStream(id: Id, options: GridFSBucketReadStreamOptions): GridFSBucketReadStream {
    return new GridFSBucketReadStream(
      this.s._chunksCollection,
      this.s._filesCollection,
      this.s.options.readPreference,
      { _id: id },
      options
    );
  }

  /**
   * Deletes a file with the given id
   *
   * @function
   * @param {ObjectId} id The id of the file doc
   * @param {GridFSBucket~errorCallback} [callback]
   */
  delete(id: Id): Promise<undefined>;
  delete(id: Id, callback: Callback<undefined>): void;
  delete(id: Id, callback?: Callback<undefined>) {
    return executeLegacyOperation(this.s.db.s.topology, _delete, [this, id, callback], {
      skipSessions: true
    });
  }

  /** Convenience wrapper around find on the files collection */
  find(filter: Document, options: FindOptions): Cursor {
    filter = filter || {};
    options = options || {};
    return this.s._filesCollection.find(filter, options);
  }

  /**
   * Returns a readable stream (GridFSBucketReadStream) for streaming the
   * file with the given name from GridFS. If there are multiple files with
   * the same name, this will stream the most recent file with the given name
   * (as determined by the `uploadDate` field). You can set the `revision`
   * option to change this behavior.
   */
  openDownloadStreamByName(
    filename: string,
    options: GridFSBucketReadStreamOptions & {
      /**  The revision number relative to the oldest file with the given filename. 0 gets you the oldest file, 1 gets you the 2nd oldest, -1 gets you the newest. */
      revision?: number;
    }
  ): GridFSBucketReadStream {
    var sort = { uploadDate: -1 };
    var skip = undefined;
    if (options && options.revision != null) {
      if (options.revision >= 0) {
        sort = { uploadDate: 1 };
        skip = options.revision;
      } else {
        skip = -options.revision - 1;
      }
    }
    return new GridFSBucketReadStream(
      this.s._chunksCollection,
      this.s._filesCollection,
      this.s.options.readPreference,
      { filename },
      { ...options, sort, skip }
    );
  }

  /**
   * Renames the file with the given _id to the given string
   *
   * @function
   * @param {ObjectId} id the id of the file to rename
   * @param {string} filename new name for the file
   * @param {GridFSBucket~errorCallback} [callback]
   */
  rename(id: Id, filename: string): Promise<undefined>;
  rename(id: Id, filename: string, callback: Callback<undefined>): void;
  rename(id: Id, filename: string, callback?: Callback<undefined>): Promise<undefined> | void {
    return executeLegacyOperation(this.s.db.s.topology, _rename, [this, id, filename, callback], {
      skipSessions: true
    });
  }

  /**
   * Removes this bucket's files collection, followed by its chunks collection.
   *
   * @function
   * @param {GridFSBucket~errorCallback} [callback]
   */
  drop(): Promise<undefined>;
  drop(callback: Callback<undefined>): void;
  drop(callback?: Callback<undefined>): Promise<undefined> | void {
    return executeLegacyOperation(this.s.db.s.topology, _drop, [this, callback], {
      skipSessions: true
    });
  }

  /**
   * Return the db logger
   *
   * @function
   * @returns {Logger} return the db logger
   */
  getLogger(): Logger {
    return this.s.db.s.logger;
  }
}

function _delete(self: GridFSBucket, id: Id, callback: Callback<undefined>) {
  return self.s._filesCollection.deleteOne({ _id: id }, (error, res) => {
    if (error) {
      return callback(error);
    }

    return self.s._chunksCollection.deleteMany({ files_id: id }, (error?: AnyError) => {
      if (error) {
        return callback(error);
      }

      // Delete orphaned chunks before returning FileNotFound
      if (!res?.result.n) {
        var errmsg = 'FileNotFound: no file with id ' + id + ' found';
        return callback(new Error(errmsg));
      }

      return callback();
    });
  });
}

function _rename(self: GridFSBucket, id: Id, filename: string, callback: Callback<undefined>) {
  const filter = { _id: id };
  const update = { $set: { filename } };
  return self.s._filesCollection.updateOne(filter, update, (error?, res?) => {
    if (error) {
      return callback(error);
    }
    if (!res?.result.n) {
      return callback(new MongoError(`File with id ${id} not found`));
    }
    return callback();
  });
}

function _drop(self: GridFSBucket, callback: Callback<undefined>) {
  return self.s._filesCollection.drop((error?: Error) => {
    if (error) {
      return callback(error);
    }
    return self.s._chunksCollection.drop((error?: Error) => {
      if (error) {
        return callback(error);
      }

      return callback();
    });
  });
}
