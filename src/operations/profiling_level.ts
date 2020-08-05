import { defineAspects, Aspect } from './operation';
import { CommandOperation } from './command';
import type { Callback } from '../types';
import type { Server } from '../sdam/server';

export class ProfilingLevelOperation extends CommandOperation {
  constructor(db: any, options: any) {
    super(db, options);
  }

  execute(server: Server, callback: Callback) {
    super.executeCommand(server, { profile: -1 }, (err?: any, doc?: any) => {
      if (err == null && doc.ok === 1) {
        const was = doc.was;
        if (was === 0) return callback(undefined, 'off');
        if (was === 1) return callback(undefined, 'slow_only');
        if (was === 2) return callback(undefined, 'all');
        return callback(new Error('Error: illegal profiling level value ' + was), null);
      } else {
        err != null ? callback(err, null) : callback(new Error('Error with profile command'), null);
      }
    });
  }
}

defineAspects(ProfilingLevelOperation, [Aspect.EXECUTE_WITH_SELECTION]);