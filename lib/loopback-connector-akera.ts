import { connect, IConnection } from '@akera/api';
import { debug, Debugger } from 'debug';
import { ConnectInfo } from '@akera/net';

export interface ConnectionOptions extends ConnectInfo {
  debug?: boolean
}

export class AkeraConnector {
  private connection: IConnection;
  private modelDefinitions: any;
  private debugEnable: boolean;
  private debugger: Debugger = debug('loopback:connector:akera');

  constructor(
    private config: ConnectionOptions
  ) {
    this.connection = null;
    this.debugger.enabled = this.debugger.enabled || config.debug;
  }


  // connects to an Akera Application Server
  public connect(callback?: (err?: Error) => {}): void {
    if (this.connection !== null) {
      callback && callback();
    } else {
      connect(this.config)
        .then(rsp => {
          this.connection = rsp;
          this.connection.autoReconnect = true;
          this.debugger.log(['Connection established: %s.', this.config.host + ':' + this.config.port]);
          callback && callback();
        })
        .catch(err => {
          this.debugger.log(['Connection error: %j', err]);
          callback && callback(err);
        })
    }
  }

  // // connects to an Akera Application Server
  // this.connect = function(callback) {
  //   if (self.connection !== null) {
  //     callback();
  //   } else {
  //     akeraApi.connect(config).then(
  //         function(conn) {
  //           self.connection = conn;
  //           self.connection.autoReconnect = true;
  //           self.debuglog('Connection established: %s.', config.host + ':'
  //               + config.port);
  //           callback();
  //         }, function(err) {
  //           self.debuglog('Connection error: %j', err);
  //           callback(err);
  //         });
  //   }
  // };

  public disconect(): Promise<any> {
    if (this.connection === null) {
      return Promise.resolve();
    } else {
      this.connection = null;
      return Promise.resolve();
    }
  }


  // // closes the active connection
  // this.disconnect = function(callback) {
  //   if (self.connection === null) {
  //     callback();
  //   } else {
  //     var closeCallback = function(err) {
  //       self.connection = null;
  //       callback(err);
  //     };
  //     self.connection.disconnect().then(closeCallback, closeCallback);
  //   }
  // };

}



