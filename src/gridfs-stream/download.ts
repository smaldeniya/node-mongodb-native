import type { FindOptions } from './../operations/find';
import type { Document, AnyError } from './../types.d';
import type { Cursor } from './../cursor/cursor';
import { Readable } from 'stream';
import type { Callback } from '../types';
import type { Collection, ReadPreference } from '..';
import type { GridFSBucketWriteStream } from './upload';

export interface GridFSBucketReadStreamOptions {
  sort?: number | { uploadDate: number };
  skip?: number;
  /** 0-based offset in bytes to start streaming from */
  start?: number;
  /** 0-based offset in bytes to stop streaming before */
  end?: number;
}

export interface GridFsFile {
  _id: GridFSBucketWriteStream['id'];
  length: GridFSBucketWriteStream['length'];
  chunkSize: GridFSBucketWriteStream['chunkSizeBytes'];
  md5?: boolean | string;
  filename: GridFSBucketWriteStream['filename'];
  contentType?: GridFSBucketWriteStream['options']['contentType'];
  aliases?: GridFSBucketWriteStream['options']['aliases'];
  metadata?: GridFSBucketWriteStream['options']['metadata'];
  uploadDate: Date;
}

export interface GridFSBucketReadStreamPrivate {
  bytesRead: number;
  bytesToTrim: number;
  bytesToSkip: number;
  chunks: Collection;
  cursor?: Cursor;
  expected: number;
  files: Collection;
  filter: Document;
  init: boolean;
  expectedEnd: number;
  file?: GridFsFile;
  options: {
    sort?: number | { uploadDate: number };
    skip?: number;
    /** 0-based offset in bytes to start streaming from */
    start: number;
    /** 0-based offset in bytes to stop streaming before */
    end: number;
  };
  readPreference?: ReadPreference;
}

/**
 * A readable stream that enables you to read buffers from GridFS.
 *
 * Do not instantiate this class directly. Use `openDownloadStream()` instead.
 *
 * @class
 * @extends external:Readable
 * @param {Collection} chunks Handle for chunks collection
 * @param {Collection} files Handle for files collection
 * @param {object} readPreference The read preference to use
 * @param {object} filter The query to use to find the file document
 * @param {object} [options] Optional settings.
 * @param {number} [options.sort] Optional sort for the file find query
 * @param {number} [options.skip] Optional skip for the file find query
 * @param {number} [options.start] Optional 0-based offset in bytes to start streaming from
 * @param {number} [options.end] Optional 0-based offset in bytes to stop streaming before
 * @fires GridFSBucketReadStream#error
 * @fires GridFSBucketReadStream#file
 */
export class GridFSBucketReadStream extends Readable {
  s: GridFSBucketReadStreamPrivate;

  constructor(
    chunks: Collection,
    files: Collection,
    readPreference: ReadPreference | undefined,
    filter: Document,
    options?: GridFSBucketReadStreamOptions
  ) {
    super();
    this.s = {
      bytesToTrim: 0,
      bytesToSkip: 0,
      bytesRead: 0,
      chunks,
      expected: 0,
      files,
      filter,
      init: false,
      expectedEnd: 0,
      options: {
        start: 0,
        end: 0,
        ...options
      },
      readPreference
    };
  }

  /**
   * An error occurred
   *
   * @event GridFSBucketReadStream#error
   * @type {Error}
   */

  /**
   * Fires when the stream loaded the file document corresponding to the
   * provided id.
   *
   * @event GridFSBucketReadStream#file
   * @type {object}
   */

  /**
   * Emitted when a chunk of data is available to be consumed.
   *
   * @event GridFSBucketReadStream#data
   * @type {object}
   */

  /**
   * Fired when the stream is exhausted (no more data events).
   *
   * @event GridFSBucketReadStream#end
   * @type {object}
   */

  /**
   * Fired when the stream is exhausted and the underlying cursor is killed
   *
   * @event GridFSBucketReadStream#close
   * @type {object}
   */

  /**
   * Reads from the cursor and pushes to the stream.
   * Private Impl, do not call directly
   */
  _read() {
    if (this.destroyed) return;
    waitForFile(this, () => doRead(this));
  }

  /**
   * Sets the 0-based offset in bytes to start streaming from. Throws
   * an error if this stream has entered flowing mode
   * (e.g. if you've already called `on('data')`)
   */
  start(
    /** 0-based offset in bytes to start streaming from */
    start = 0
  ): GridFSBucketReadStream {
    throwIfInitialized(this);
    this.s.options.start = start;
    return this;
  }

  /**
   * Sets the 0-based offset in bytes to start streaming from. Throws
   * an error if this stream has entered flowing mode
   * (e.g. if you've already called `on('data')`)
   *
   * @function
   * @param {number} end Offset in bytes to stop reading at
   * @returns {GridFSBucketReadStream} Reference to self
   */

  end(
    /** Offset in bytes to stop reading at */
    end = 0
  ) {
    throwIfInitialized(this);
    this.s.options.end = end;
    return this;
  }

  /**
   * Marks this stream as aborted (will never push another `data` event)
   * and kills the underlying cursor. Will emit the 'end' event, and then
   * the 'close' event once the cursor is successfully killed.
   *
   * @function
   * @param {GridFSBucket~errorCallback} [callback] called when the cursor is successfully closed or an error occurred.
   * @fires GridFSBucketWriteStream#close
   * @fires GridFSBucketWriteStream#end
   */

  abort(callback: Callback<undefined>) {
    this.push(null);
    this.destroyed = true;
    if (this.s.cursor) {
      this.s.cursor.close((error?: Error) => {
        this.emit('close');
        callback && callback(error);
      });
    } else {
      if (!this.s.init) {
        // If not initialized, fire close event because we will never
        // get a cursor
        this.emit('close');
      }
      callback && callback();
    }
  }
}

function throwIfInitialized(self: GridFSBucketReadStream) {
  if (self.s.init) {
    throw new Error('You cannot change options after the stream has entered' + 'flowing mode!');
  }
}

function doRead(self: GridFSBucketReadStream) {
  if (self.destroyed) return;
  if (!self.s.cursor) return;
  if (!self.s.file) return;

  self.s.cursor.next((error?: Error, doc?: Document) => {
    if (self.destroyed) {
      return;
    }
    if (error) {
      return __handleError(self, error);
    }
    if (!doc) {
      self.push(null);

      process.nextTick(() => {
        if (!self.s.cursor) return;
        self.s.cursor.close((error?: Error) => {
          if (error) {
            __handleError(self, error);
            return;
          }

          self.emit('close');
        });
      });

      return;
    }

    if (!self.s.file) return;

    var bytesRemaining = self.s.file.length - self.s.bytesRead;
    var expectedN = self.s.expected++;
    var expectedLength = Math.min(self.s.file.chunkSize, bytesRemaining);

    if (doc.n > expectedN) {
      var errmsg = 'ChunkIsMissing: Got unexpected n: ' + doc.n + ', expected: ' + expectedN;
      return __handleError(self, new Error(errmsg));
    }

    if (doc.n < expectedN) {
      errmsg = 'ExtraChunk: Got unexpected n: ' + doc.n + ', expected: ' + expectedN;
      return __handleError(self, new Error(errmsg));
    }

    var buf = Buffer.isBuffer(doc.data) ? doc.data : doc.data.buffer;

    if (buf.length !== expectedLength) {
      if (bytesRemaining <= 0) {
        errmsg = 'ExtraChunk: Got unexpected n: ' + doc.n;
        return __handleError(self, new Error(errmsg));
      }

      errmsg =
        'ChunkIsWrongSize: Got unexpected length: ' + buf.length + ', expected: ' + expectedLength;
      return __handleError(self, new Error(errmsg));
    }

    self.s.bytesRead += buf.length;

    if (buf.length === 0) {
      return self.push(null);
    }

    var sliceStart = null;
    var sliceEnd = null;

    if (self.s.bytesToSkip != null) {
      sliceStart = self.s.bytesToSkip;
      self.s.bytesToSkip = 0;
    }

    const atEndOfStream = expectedN === self.s.expectedEnd - 1;
    const bytesLeftToRead = self.s.options.end - self.s.bytesToSkip;
    if (atEndOfStream && self.s.bytesToTrim != null) {
      sliceEnd = self.s.file.chunkSize - self.s.bytesToTrim;
    } else if (self.s.options.end && bytesLeftToRead < doc.data.length()) {
      sliceEnd = bytesLeftToRead;
    }

    if (sliceStart != null || sliceEnd != null) {
      buf = buf.slice(sliceStart || 0, sliceEnd || buf.length);
    }

    self.push(buf);
  });
}

function init(self: GridFSBucketReadStream) {
  var findOneOptions: FindOptions = {};
  if (self.s.readPreference) {
    findOneOptions.readPreference = self.s.readPreference;
  }
  if (self.s.options && self.s.options.sort) {
    findOneOptions.sort = self.s.options.sort;
  }
  if (self.s.options && self.s.options.skip) {
    findOneOptions.skip = self.s.options.skip;
  }

  self.s.files.findOne(self.s.filter, findOneOptions, (error?: AnyError, doc?: GridFsFile) => {
    if (error) {
      return __handleError(self, error);
    }

    if (!doc) {
      var identifier = self.s.filter._id ? self.s.filter._id.toString() : self.s.filter.filename;
      var errmsg = 'FileNotFound: file ' + identifier + ' was not found';
      var err = new Error(errmsg);
      (err as any).code = 'ENOENT';
      return __handleError(self, err);
    }

    // If document is empty, kill the stream immediately and don't
    // execute any reads
    if (doc.length <= 0) {
      self.push(null);
      return;
    }

    if (self.destroyed) {
      // If user destroys the stream before we have a cursor, wait
      // until the query is done to say we're 'closed' because we can't
      // cancel a query.
      self.emit('close');
      return;
    }

    try {
      self.s.bytesToSkip = handleStartOption(self, doc, self.s.options);
    } catch (error) {
      return __handleError(self, error);
    }

    var filter: Document = { files_id: doc._id };

    // Currently (MongoDB 3.4.4) skip function does not support the index,
    // it needs to retrieve all the documents first and then skip them. (CS-25811)
    // As work around we use $gte on the "n" field.
    if (self.s.options && self.s.options.start != null) {
      var skip = Math.floor(self.s.options.start / doc.chunkSize);
      if (skip > 0) {
        filter['n'] = { $gte: skip };
      }
    }
    self.s.cursor = self.s.chunks.find(filter).sort({ n: 1 });

    if (self.s.readPreference) {
      self.s.cursor.setReadPreference(self.s.readPreference);
    }

    self.s.expectedEnd = Math.ceil(doc.length / doc.chunkSize);
    self.s.file = doc;

    try {
      self.s.bytesToTrim = handleEndOption(self, doc, self.s.cursor, self.s.options);
    } catch (error) {
      return __handleError(self, error);
    }

    self.emit('file', doc);
  });
}

function waitForFile(self: GridFSBucketReadStream, callback: Callback) {
  if (self.s.file) {
    return callback();
  }

  if (!self.s.init) {
    init(self);
    self.s.init = true;
  }

  self.once('file', () => {
    callback();
  });
}

function handleStartOption(
  stream: GridFSBucketReadStream,
  doc: Document,
  options: GridFSBucketReadStreamOptions
) {
  if (options && options.start != null) {
    if (options.start > doc.length) {
      throw new Error(
        'Stream start (' +
          options.start +
          ') must not be ' +
          'more than the length of the file (' +
          doc.length +
          ')'
      );
    }
    if (options.start < 0) {
      throw new Error('Stream start (' + options.start + ') must not be ' + 'negative');
    }
    if (options.end != null && options.end < options.start) {
      throw new Error(
        'Stream start (' +
          options.start +
          ') must not be ' +
          'greater than stream end (' +
          options.end +
          ')'
      );
    }

    stream.s.bytesRead = Math.floor(options.start / doc.chunkSize) * doc.chunkSize;
    stream.s.expected = Math.floor(options.start / doc.chunkSize);

    return options.start - stream.s.bytesRead;
  }
  throw new Error('no option available');
}

function handleEndOption(
  stream: GridFSBucketReadStream,
  doc: Document,
  cursor: Cursor,
  options: GridFSBucketReadStreamOptions
) {
  if (options && options.end != null) {
    if (options.end > doc.length) {
      throw new Error(
        'Stream end (' +
          options.end +
          ') must not be ' +
          'more than the length of the file (' +
          doc.length +
          ')'
      );
    }
    if (options.start == null || options.start < 0) {
      throw new Error('Stream end (' + options.end + ') must not be ' + 'negative');
    }

    var start = options.start != null ? Math.floor(options.start / doc.chunkSize) : 0;

    cursor.limit(Math.ceil(options.end / doc.chunkSize) - start);

    stream.s.expectedEnd = Math.ceil(options.end / doc.chunkSize);

    return Math.ceil(options.end / doc.chunkSize) * doc.chunkSize - options.end;
  }
  throw new Error('no option available');
}

function __handleError(self: GridFSBucketReadStream, error?: AnyError) {
  self.emit('error', error);
}
