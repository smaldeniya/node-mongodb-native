import type { Db } from './db';
import type { Collection } from './collection';
import type { ClientSession } from './sessions';

export type W = number | 'majority';

export interface WriteConcernOptions {
  /** The write concern */
  w?: W;
  /** The write concern timeout */
  wtimeout?: number;
  /** The write concern timeout */
  wtimeoutMS?: number;
  /** The journal write concern */
  j?: boolean;
  /** The journal write concern */
  journal?: boolean;
  /** The file sync write concern */
  fsync?: boolean | 1;
  /** Write Concern as an object */
  writeConcern?: WriteConcernOptions | WriteConcern | W;
}

/**
 * A MongoDB WriteConcern, which describes the level of acknowledgement
 * requested from MongoDB for write operations.
 *
 * @see https://docs.mongodb.com/manual/reference/write-concern/
 */
export class WriteConcern {
  /** The write concern */
  w?: W;
  /** The write concern timeout */
  wtimeout?: number;
  /** The journal write concern */
  j?: boolean;
  /** The file sync write concern */
  fsync?: boolean | 1;

  /** Constructs a WriteConcern from the write concern properties. */
  constructor(
    /** The write concern */
    w?: W,
    /** The write concern timeout */
    wtimeout?: number,
    /** The journal write concern */
    j?: boolean,
    /** The file sync write concern */
    fsync?: boolean | 1
  ) {
    if (w != null) {
      this.w = w;
    }
    if (wtimeout != null) {
      this.wtimeout = wtimeout;
    }
    if (j != null) {
      this.j = j;
    }
    if (fsync != null) {
      this.fsync = fsync;
    }
  }

  /** Construct a WriteConcern given an options object. */
  static fromOptions(
    options?: WriteConcernOptions | WriteConcern | W,
    inherit?: WriteConcernOptions | WriteConcern
  ): WriteConcern | undefined {
    const { fromOptions } = WriteConcern;
    if (typeof options === 'undefined') return undefined;
    if (typeof options === 'number') return fromOptions({ ...inherit, w: options });
    if (typeof options === 'string') return fromOptions({ ...inherit, w: options });
    if (options instanceof WriteConcern) return fromOptions({ ...inherit, ...options });
    if (options.writeConcern) {
      const { writeConcern, ...viable } = { ...inherit, ...options };
      return fromOptions(writeConcern, viable);
    }
    const { w, wtimeout, j, fsync, journal, wtimeoutMS } = { ...inherit, ...options };
    if (
      w != null ||
      wtimeout != null ||
      wtimeoutMS != null ||
      j != null ||
      journal != null ||
      fsync != null
    ) {
      return new WriteConcern(w, wtimeout ?? wtimeoutMS, j ?? journal, fsync);
    }
    return undefined;
  }

  static inherit(
    options: WriteConcernOptions,
    parent: {
      db?: Db;
      collection?: Collection;
      session?: ClientSession;
    }
  ) {
    const { session, collection } = parent;
    const db = parent.db || (collection && collection.s.db) || undefined;

    if (session && session.inTransaction()) return undefined;

    const writeConcern = WriteConcern.fromOptions(options);
    if (writeConcern) return writeConcern;

    if (collection) {
      const writeConcern = WriteConcern.fromOptions(collection.writeConcern);
      if (writeConcern) return writeConcern;
    }

    if (db) {
      const writeConcern = WriteConcern.fromOptions(db.writeConcern);
      if (writeConcern) return writeConcern;
    }
  }
}
