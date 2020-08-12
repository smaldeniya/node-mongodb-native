import * as crypto from 'crypto';
import { PromiseProvider } from '../promise_provider';
import { Writable } from 'stream';
import { ObjectId } from '../bson';
import type { Callback, AnyError } from '../types';
import type { Collection } from '../collection';
import type { Document } from './../types.d';
import type { GridFSBucket } from './index';
import type { GridFsFile } from './download';
import type { WriteConcernOptions } from '../write_concern';
import { MongoError } from '../error';

const ERROR_NAMESPACE_NOT_FOUND = 26;

export type Id = string | number | object | ObjectId;

export interface GridFSBucketWriteStreamOptions {
  /** Overwrite this bucket's chunkSizeBytes for this file */
  chunkSizeBytes?: number;
  /** Custom file id for the GridFS file. */
  id?: Id;
  /** Object to store in the file document's `metadata` field */
  metadata?: Document;
  /** String to store in the file document's `contentType` field */
  contentType?: string;
  /** Array of strings to store in the file document's `aliases` field */
  aliases?: string[];
  /** If true, disables adding an md5 field to file data */
  disableMD5?: boolean;
}

/**
 * A writable stream that enables you to write buffers to GridFS.
 *
 * Do not instantiate this class directly. Use `openUploadStream()` instead.
 *
 * @class
 * @extends external:Writable
 * @param {GridFSBucket} bucket Handle for this stream's corresponding bucket
 * @param {string} filename The value of the 'filename' key in the files doc
 * @param {object} [options] Optional settings.
 * @fires GridFSBucketWriteStream#error
 * @fires GridFSBucketWriteStream#finish
 */
export class GridFSBucketWriteStream extends Writable {
  bucket: GridFSBucket;
  chunks: Collection;
  filename: string;
  files: Collection;
  options: GridFSBucketWriteStreamOptions;
  done: boolean;
  id: string | number | object | ObjectId;
  chunkSizeBytes: number;
  bufToStore: Buffer;
  length: number;
  md5: false | crypto.Hash;
  n: number;
  pos: number;
  state: {
    streamEnd: boolean;
    outstandingRequests: number;
    errored: boolean;
    aborted: boolean;
  };

  constructor(bucket: GridFSBucket, filename: string, options?: GridFSBucketWriteStreamOptions) {
    super();

    options = options || {};
    this.bucket = bucket;
    this.chunks = bucket.s._chunksCollection;
    this.filename = filename;
    this.files = bucket.s._filesCollection;
    this.options = options;
    // Signals the write is all done
    this.done = false;

    this.id = options.id ? options.id : new ObjectId();
    // properly inherit the default chunksize from parent
    this.chunkSizeBytes = options.chunkSizeBytes || this.bucket.s.options.chunkSizeBytes;
    this.bufToStore = Buffer.alloc(this.chunkSizeBytes);
    this.length = 0;
    this.md5 = !options.disableMD5 && crypto.createHash('md5');
    this.n = 0;
    this.pos = 0;
    this.state = {
      streamEnd: false,
      outstandingRequests: 0,
      errored: false,
      aborted: false
    };

    if (!this.bucket.s.calledOpenUploadStream) {
      this.bucket.s.calledOpenUploadStream = true;

      checkIndexes(this, () => {
        this.bucket.s.checkedIndexes = true;
        this.bucket.emit('index');
      });
    }
  }

  /**
   * An error occurred
   *
   * @event GridFSBucketWriteStream#error
   * @type {Error}
   */

  /**
   * `end()` was called and the write stream successfully wrote the file
   * metadata and all the chunks to MongoDB.
   *
   * @event GridFSBucketWriteStream#finish
   * @type {object}
   */

  /**
   * Write a buffer to the stream.
   *
   * @function
   * @param {Buffer} chunk Buffer to write
   * @param {string} encoding Optional encoding for the buffer
   * @param {GridFSBucket~errorCallback} callback Function to call when the chunk was added to the buffer, or if the entire chunk was persisted to MongoDB if this chunk caused a flush.
   * @returns {boolean} False if this write required flushing a chunk to MongoDB. True otherwise.
   */

  write(
    chunk: Buffer,
    encodingOrCallback?: Callback<undefined> | BufferEncoding,
    callback?: Callback<undefined>
  ): boolean {
    const encoding = typeof encodingOrCallback === 'function' ? undefined : encodingOrCallback;
    callback = typeof encodingOrCallback === 'function' ? encodingOrCallback : callback;
    return waitForIndexes(this, () => doWrite(this, chunk, encoding, callback));
  }

  /**
   * Places this write stream into an aborted state (all future writes fail)
   * and deletes all chunks that have already been written.
   *
   * @function
   * @param {GridFSBucket~errorCallback} callback called when chunks are successfully removed or error occurred
   * @returns {Promise<void>} if no callback specified
   */

  abort(callback: Callback<undefined>): Promise<void> | void {
    const Promise = PromiseProvider.get();
    if (this.state.streamEnd) {
      var error = new Error('Cannot abort a stream that has already completed');
      if (typeof callback === 'function') {
        return callback(error);
      }
      return Promise.reject(error);
    }
    if (this.state.aborted) {
      error = new Error('Cannot call abort() on a stream twice');
      if (typeof callback === 'function') {
        return callback(error);
      }
      return Promise.reject(error);
    }
    this.state.aborted = true;
    this.chunks.deleteMany({ files_id: this.id }, (error?: Error) => {
      if (typeof callback === 'function') callback(error);
    });
  }

  /**
   * Tells the stream that no more data will be coming in. The stream will
   * persist the remaining data to MongoDB, write the files document, and
   * then emit a 'finish' event.
   *
   * @function
   * @param {Buffer} chunk Buffer to write
   * @param {string} encoding Optional encoding for the buffer
   * @param {GridFSBucket~errorCallback} callback Function to call when all files and chunks have been persisted to MongoDB
   */

  end(
    chunkOrCallback?: Buffer | Callback<GridFsFile | undefined>,
    encodingOrCallback?: BufferEncoding | Callback<GridFsFile | undefined>,
    callback?: Callback<GridFsFile | undefined>
  ): void {
    const chunk = typeof chunkOrCallback === 'function' ? undefined : chunkOrCallback;
    const encoding = typeof encodingOrCallback === 'function' ? undefined : encodingOrCallback;
    callback =
      typeof chunkOrCallback === 'function'
        ? chunkOrCallback
        : typeof encodingOrCallback === 'function'
        ? encodingOrCallback
        : callback;

    if (checkAborted(this, callback)) return;

    this.state.streamEnd = true;

    if (callback) {
      this.once('finish', (result: GridFsFile) => {
        callback!(undefined, result);
      });
    }

    if (!chunk) {
      waitForIndexes(this, () => !!writeRemnant(this));
      return;
    }

    this.write(chunk, encoding, () => {
      writeRemnant(this);
    });
  }
}

function __handleError(self: GridFSBucketWriteStream, error: AnyError, callback?: Callback) {
  if (self.state.errored) {
    return;
  }
  self.state.errored = true;
  if (callback) {
    return callback(error);
  }
  self.emit('error', error);
}

function createChunkDoc(filesId: Id, n: number, data: Buffer) {
  return {
    _id: new ObjectId(),
    files_id: filesId,
    n,
    data
  };
}

function checkChunksIndex(self: GridFSBucketWriteStream, callback: Callback) {
  self.chunks.listIndexes().toArray((error?: AnyError, indexes?: Document[]) => {
    if (error) {
      // Collection doesn't exist so create index
      if (error instanceof MongoError && error.code === ERROR_NAMESPACE_NOT_FOUND) {
        var index = { files_id: 1, n: 1 };
        self.chunks.createIndex(index, { background: false, unique: true }, (error?: AnyError) => {
          if (error) {
            return callback(error);
          }

          callback();
        });
        return;
      }
      return callback(error);
    }

    var hasChunksIndex = false;
    if (indexes) {
      indexes.forEach((index: Document) => {
        if (index.key) {
          var keys = Object.keys(index.key);
          if (keys.length === 2 && index.key.files_id === 1 && index.key.n === 1) {
            hasChunksIndex = true;
          }
        }
      });
    }

    if (hasChunksIndex) {
      callback();
    } else {
      index = { files_id: 1, n: 1 };
      var writeConcernOptions = getWriteOptions(self);

      self.chunks.createIndex(
        index,
        {
          ...writeConcernOptions,
          background: true,
          unique: true
        },
        callback
      );
    }
  });
}

function checkDone(self: GridFSBucketWriteStream, callback?: Callback): boolean {
  if (self.done) return true;
  if (self.state.streamEnd && self.state.outstandingRequests === 0 && !self.state.errored) {
    // Set done so we dont' trigger duplicate createFilesDoc
    self.done = true;
    // Create a new files doc
    var filesDoc = createFilesDoc(
      self.id,
      self.length,
      self.chunkSizeBytes,
      self.md5 && self.md5.digest('hex'),
      self.filename,
      self.options.contentType,
      self.options.aliases,
      self.options.metadata
    );

    if (checkAborted(self, callback)) {
      return false;
    }

    self.files.insertOne(filesDoc, getWriteOptions(self), (error?: AnyError) => {
      if (error) {
        return __handleError(self, error, callback);
      }
      self.emit('finish', filesDoc);
    });

    return true;
  }

  return false;
}

function checkIndexes(self: GridFSBucketWriteStream, callback: Callback) {
  self.files.findOne({}, { _id: 1 }, (error?: AnyError, doc?: GridFsFile) => {
    if (error) {
      return callback(error);
    }
    if (doc) {
      return callback();
    }

    self.files.listIndexes().toArray((error?: AnyError, indexes?: Document) => {
      if (error) {
        // Collection doesn't exist so create index
        if (error instanceof MongoError && error.code === ERROR_NAMESPACE_NOT_FOUND) {
          var index = { filename: 1, uploadDate: 1 };
          self.files.createIndex(index, { background: false }, (error?: AnyError) => {
            if (error) {
              return callback(error);
            }

            checkChunksIndex(self, callback);
          });
          return;
        }
        return callback(error);
      }

      var hasFileIndex = false;
      if (indexes) {
        indexes.forEach((index: Document) => {
          var keys = Object.keys(index.key);
          if (keys.length === 2 && index.key.filename === 1 && index.key.uploadDate === 1) {
            hasFileIndex = true;
          }
        });
      }

      if (hasFileIndex) {
        checkChunksIndex(self, callback);
      } else {
        index = { filename: 1, uploadDate: 1 };

        const writeConcernOptions = getWriteOptions(self);

        self.files.createIndex(
          index,
          {
            ...writeConcernOptions,
            background: false
          },
          (error?: AnyError) => {
            if (error) {
              return callback(error);
            }

            checkChunksIndex(self, callback);
          }
        );
      }
    });
  });
}

function createFilesDoc(
  _id: GridFsFile['_id'],
  length: GridFsFile['length'],
  chunkSize: GridFsFile['chunkSize'],
  md5: GridFsFile['md5'],
  filename: GridFsFile['filename'],
  contentType: GridFsFile['contentType'],
  aliases: GridFsFile['aliases'],
  metadata: GridFsFile['metadata']
): GridFsFile {
  const ret: GridFsFile = {
    _id,
    length,
    chunkSize,
    uploadDate: new Date(),
    filename
  };

  if (md5) {
    ret.md5 = md5;
  }

  if (contentType) {
    ret.contentType = contentType;
  }

  if (aliases) {
    ret.aliases = aliases;
  }

  if (metadata) {
    ret.metadata = metadata;
  }

  return ret;
}

function doWrite(
  self: GridFSBucketWriteStream,
  chunk: Buffer,
  encoding: BufferEncoding | undefined,
  callback?: Callback<undefined>
): boolean {
  if (checkAborted(self, callback)) {
    return false;
  }

  var inputBuf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding);

  self.length += inputBuf.length;

  // Input is small enough to fit in our buffer
  if (self.pos + inputBuf.length < self.chunkSizeBytes) {
    inputBuf.copy(self.bufToStore, self.pos);
    self.pos += inputBuf.length;

    callback && callback();

    // Note that we reverse the typical semantics of write's return value
    // to be compatible with node's `.pipe()` function.
    // True means client can keep writing.
    return true;
  }

  // Otherwise, buffer is too big for current chunk, so we need to flush
  // to MongoDB.
  var inputBufRemaining = inputBuf.length;
  var spaceRemaining: number = self.chunkSizeBytes - self.pos;
  var numToCopy = Math.min(spaceRemaining, inputBuf.length);
  var outstandingRequests = 0;
  while (inputBufRemaining > 0) {
    var inputBufPos = inputBuf.length - inputBufRemaining;
    inputBuf.copy(self.bufToStore, self.pos, inputBufPos, inputBufPos + numToCopy);
    self.pos += numToCopy;
    spaceRemaining -= numToCopy;
    if (spaceRemaining === 0) {
      if (self.md5) {
        self.md5.update(self.bufToStore);
      }
      var doc = createChunkDoc(self.id, self.n, Buffer.from(self.bufToStore));
      ++self.state.outstandingRequests;
      ++outstandingRequests;

      if (checkAborted(self, callback)) {
        return false;
      }

      self.chunks.insertOne(doc, getWriteOptions(self), (error?: AnyError) => {
        if (error) {
          return __handleError(self, error);
        }
        --self.state.outstandingRequests;
        --outstandingRequests;

        if (!outstandingRequests) {
          self.emit('drain', doc);
          callback && callback();
          checkDone(self);
        }
      });

      spaceRemaining = self.chunkSizeBytes;
      self.pos = 0;
      ++self.n;
    }
    inputBufRemaining -= numToCopy;
    numToCopy = Math.min(spaceRemaining, inputBufRemaining);
  }

  // Note that we reverse the typical semantics of write's return value
  // to be compatible with node's `.pipe()` function.
  // False means the client should wait for the 'drain' event.
  return false;
}

function getWriteOptions(self: GridFSBucketWriteStream) {
  var obj: WriteConcernOptions = {};
  if (self.bucket.s.options.writeConcern) {
    obj.w = self.bucket.s.options.writeConcern.w;
    obj.wtimeout = self.bucket.s.options.writeConcern.wtimeout;
    obj.j = self.bucket.s.options.writeConcern.j;
  }
  return obj;
}

function waitForIndexes(
  self: GridFSBucketWriteStream,
  callback: (res: boolean) => boolean
): boolean {
  if (self.bucket.s.checkedIndexes) {
    return callback(false);
  }

  self.bucket.once('index', () => {
    callback(true);
  });

  return true;
}

function writeRemnant(self: GridFSBucketWriteStream, callback?: Callback): boolean {
  // Buffer is empty, so don't bother to insert
  if (self.pos === 0) {
    return checkDone(self, callback);
  }

  ++self.state.outstandingRequests;

  // Create a new buffer to make sure the buffer isn't bigger than it needs
  // to be.
  var remnant = Buffer.alloc(self.pos);
  self.bufToStore.copy(remnant, 0, 0, self.pos);
  if (self.md5) {
    self.md5.update(remnant);
  }
  var doc = createChunkDoc(self.id, self.n, remnant);

  // If the stream was aborted, do not write remnant
  if (checkAborted(self, callback)) {
    return false;
  }

  self.chunks.insertOne(doc, getWriteOptions(self), (error?: AnyError) => {
    if (error) {
      return __handleError(self, error);
    }
    --self.state.outstandingRequests;
    checkDone(self);
  });
  return true;
}

function checkAborted(self: GridFSBucketWriteStream, callback?: Callback<undefined>): boolean {
  if (self.state.aborted) {
    if (typeof callback === 'function') {
      callback(new Error('this stream has been aborted'));
    }
    return true;
  }
  return false;
}
