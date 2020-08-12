import { AddUserOperation, AddUserOptions } from './operations/add_user';
import { RemoveUserOperation, RemoveUserOptions } from './operations/remove_user';
import { ValidateCollectionOperation } from './operations/validate_collection';
import { ListDatabasesOperation, ListDatabasesOptions } from './operations/list_databases';
import { executeOperation } from './operations/execute_operation';
import { RunCommandOperation } from './operations/run_command';
import type { Callback, Document } from './types';
import type { CommandOperationOptions } from './operations/command';

/**
 * The **Admin** class is an internal class that allows convenient access to
 * the admin functionality and commands for MongoDB.
 *
 * **ADMIN Cannot directly be instantiated**
 *
 * @example
 * const MongoClient = require('mongodb').MongoClient;
 * const test = require('assert');
 * // Connection url
 * const url = 'mongodb://localhost:27017';
 * // Database Name
 * const dbName = 'test';
 *
 * // Connect using MongoClient
 * MongoClient.connect(url, function(err, client) {
 *   // Use the admin database for the operation
 *   const adminDb = client.db(dbName).admin();
 *
 *   // List all the available databases
 *   adminDb.listDatabases(function(err, dbs) {
 *     expect(err).to.not.exist;
 *     test.ok(dbs.databases.length > 0);
 *     client.close();
 *   });
 * });
 */

export class Admin {
  s: any;

  /**
   * Create a new Admin instance (INTERNAL TYPE, do not instantiate directly)
   *
   * @param {any} db
   * @param {any} topology
   * @returns {Admin} a collection instance.
   */
  constructor(db: any, topology: any) {
    this.s = {
      db,
      topology
    };
  }

  /**
   * The callback format for results
   *
   * @callback Admin~resultCallback
   * @param {MongoError} error An error instance representing the error during the execution.
   * @param {object} result The result object if the command was executed successfully.
   */

  /**
   * Execute a command
   *
   * @function
   * @param {object} command The command hash
   * @param {object} [options] Optional settings.
   * @param {(ReadPreference|string)} [options.readPreference] The preferred read preference (ReadPreference.PRIMARY, ReadPreference.PRIMARY_PREFERRED, ReadPreference.SECONDARY, ReadPreference.SECONDARY_PREFERRED, ReadPreference.NEAREST).
   * @param {number} [options.maxTimeMS] Number of milliseconds to wait before aborting the query.
   * @param {Admin~resultCallback} [callback] The command result callback
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  command(command: object, options?: any, callback?: Callback<Document>): Promise<Document> | void {
    const args = Array.prototype.slice.call(arguments, 1);
    callback = typeof args[args.length - 1] === 'function' ? args.pop() : undefined;
    options = Object.assign({ dbName: 'admin' }, args.length ? args.shift() : {});
    return executeOperation(
      this.s.db.s.topology,
      new RunCommandOperation(this.s.db, command, options),
      callback
    );
  }

  /**
   * Retrieve the server information for the current
   * instance of the db client
   *
   * @param {object} [options] optional parameters for this operation
   * @param {ClientSession} [options.session] optional session to use for this operation
   * @param {Admin~resultCallback} [callback] The command result callback
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  buildInfo(options?: any, callback?: Callback<Document>): Promise<Document> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};
    return this.command({ buildinfo: 1 }, options, callback);
  }

  /**
   * Retrieve the server information for the current
   * instance of the db client
   *
   * @param {object} [options] optional parameters for this operation
   * @param {ClientSession} [options.session] optional session to use for this operation
   * @param {Admin~resultCallback} [callback] The command result callback
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  serverInfo(options?: any, callback?: Callback<Document>): Promise<Document> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};
    return this.command({ buildinfo: 1 }, options, callback);
  }

  /**
   * Retrieve this db's server status.
   *
   * @param {object} [options] optional parameters for this operation
   * @param {ClientSession} [options.session] optional session to use for this operation
   * @param {Admin~resultCallback} [callback] The command result callback
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  serverStatus(options?: any, callback?: Callback<Document>): Promise<Document> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};
    return this.command({ serverStatus: 1 }, options, callback);
  }

  /**
   * Ping the MongoDB server and retrieve results
   *
   * @param {object} [options] optional parameters for this operation
   * @param {ClientSession} [options.session] optional session to use for this operation
   * @param {Admin~resultCallback} [callback] The command result callback
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  ping(options?: any, callback?: Callback<Document>): Promise<Document> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};
    return this.command({ ping: 1 }, options, callback);
  }

  /**
   * Add a user to the database
   *
   * @param username The username for the new user
   * @param password An optional password for the new user
   * @param options Optional settings for the command
   * @param callback An optional callback, a Promise will be returned if none is provided
   */
  addUser(
    username: string,
    password?: string,
    options?: AddUserOptions,
    callback?: Callback<Document>
  ): Promise<Document> | void {
    const args = Array.prototype.slice.call(arguments, 2);
    callback = typeof args[args.length - 1] === 'function' ? args.pop() : undefined;
    options = args.length ? args.shift() : {};
    options = Object.assign({ dbName: 'admin' }, options);

    return executeOperation(
      this.s.db.s.topology,
      new AddUserOperation(this.s.db, username, password, options),
      callback
    );
  }

  /**
   * Remove a user from a database
   *
   * @param username The username to remove
   * @param options Optional settings for the command
   * @param callback An optional callback, a Promise will be returned if none is provided
   */
  removeUser(
    username: string,
    options?: RemoveUserOptions,
    callback?: Callback<boolean>
  ): Promise<boolean> | void {
    const args = Array.prototype.slice.call(arguments, 1);
    callback = typeof args[args.length - 1] === 'function' ? args.pop() : undefined;
    options = args.length ? args.shift() : {};
    options = Object.assign({}, options);
    options.dbName = 'admin';

    return executeOperation(
      this.s.db.s.topology,
      new RemoveUserOperation(this.s.db, username, options),
      callback
    );
  }

  /**
   * Validate an existing collection
   *
   * @param {string} collectionName The name of the collection to validate.
   * @param {object} [options] Optional settings.
   * @param {boolean} [options.background] Validates a collection in the background, without interrupting read or write traffic (only in MongoDB 4.4+)
   * @param {ClientSession} [options.session] optional session to use for this operation
   * @param {Admin~resultCallback} [callback] The command result callback.
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  validateCollection(
    collectionName: string,
    options?: any,
    callback?: Callback<Document>
  ): Promise<Document> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};

    return executeOperation(
      this.s.db.s.topology,
      new ValidateCollectionOperation(this, collectionName, options),
      callback
    );
  }

  /**
   * List the available databases
   *
   * @param {object} [options] Optional settings.
   * @param {boolean} [options.nameOnly=false] Whether the command should return only db names, or names and size info.
   * @param {ClientSession} [options.session] optional session to use for this operation
   * @param {Admin~resultCallback} [callback] The command result callback.
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  listDatabases(
    options?: ListDatabasesOptions,
    callback?: Callback<string[]>
  ): Promise<string[]> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};

    return executeOperation(
      this.s.db.s.topology,
      new ListDatabasesOperation(this.s.db, options),
      callback
    );
  }

  /**
   * Get ReplicaSet status
   *
   * @param {object} [options] optional parameters for this operation
   * @param {ClientSession} [options.session] optional session to use for this operation
   * @param {Admin~resultCallback} [callback] The command result callback.
   * @returns {Promise<void> | void} returns Promise if no callback passed
   */
  replSetGetStatus(
    options?: CommandOperationOptions,
    callback?: Callback<Document>
  ): Promise<Document> | void {
    if (typeof options === 'function') (callback = options), (options = {});
    options = options || {};
    return this.command({ replSetGetStatus: 1 }, options, callback);
  }
}
