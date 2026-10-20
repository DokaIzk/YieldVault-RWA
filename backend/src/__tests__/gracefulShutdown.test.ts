import http from 'http';
import { GracefulShutdownHandler } from '../gracefulShutdown';

function listen(server: http.Server): Promise<number> {
  return new Promise((resolve) => {
    server.listen(0, () => {
      const address = server.address();
      resolve(typeof address === 'object' && address ? address.port : 0);
    });
  });
}

describe('GracefulShutdownHandler', () => {
  let exitSpy: jest.SpyInstance;

  beforeEach(() => {
    exitSpy = jest.spyOn(process, 'exit').mockImplementation(((_code?: number) => undefined) as never);
  });

  afterEach(() => {
    exitSpy.mockRestore();
    process.removeAllListeners('SIGTERM');
    process.removeAllListeners('SIGINT');
  });

  it('does not run cleanup (e.g. prisma.$disconnect) until in-flight requests have finished committing', async () => {
    // Reproduces the fixed bug: a request holding an open
    // prisma.vault.update transaction must be allowed to finish before the
    // DB connection is torn down, or the transaction is killed mid-commit
    // and the row stays locked (P2034 on the next deploy's migration).
    const events: string[] = [];
    let transactionCommitted = false;

    const server = http.createServer((_req, res) => {
      setTimeout(() => {
        transactionCommitted = true;
        events.push('transaction-committed');
        res.writeHead(200);
        res.end('ok');
      }, 100);
    });

    const port = await listen(server);
    const handler = new GracefulShutdownHandler(2000);
    handler.register(server);

    handler.onShutdown(async () => {
      events.push('prisma-disconnect');
      if (!transactionCommitted) {
        throw new Error('prisma disconnected before the in-flight transaction committed');
      }
    });

    expect(handler.isShuttingDown()).toBe(false);

    // agent: false disables keep-alive so the underlying socket closes as
    // soon as the response ends, instead of idling and only being reaped by
    // the (much longer, 2000ms) drain-timeout force-destroy below.
    const requestFinished = new Promise<void>((resolve, reject) => {
      http
        .get(`http://127.0.0.1:${port}`, { agent: false }, (res) => {
          res.on('data', () => {});
          res.on('end', resolve);
        })
        .on('error', reject);
    });

    // Let the server start handling the request before signalling shutdown.
    await new Promise((r) => setTimeout(r, 20));
    process.emit('SIGTERM');

    // The shutdown gate flips synchronously, before the drain/cleanup work.
    expect(handler.isShuttingDown()).toBe(true);

    await requestFinished;

    // Give the post-drain cleanup step a chance to run.
    await new Promise((r) => setTimeout(r, 100));

    expect(events).toEqual(['transaction-committed', 'prisma-disconnect']);
    expect(exitSpy).toHaveBeenCalledWith(0);
  });

  it('bounds cleanup to its own timeout and still exits if a task hangs', async () => {
    const server = http.createServer((_req, res) => res.end('ok'));
    await listen(server);

    // drainTimeout=2000 (no active connections, so drain finishes
    // immediately); cleanupTimeout=50 so a hung cleanup task doesn't leave
    // this test waiting out the real 10s production default.
    const handler = new GracefulShutdownHandler(2000, 50);
    handler.register(server);

    handler.onShutdown(() => new Promise(() => {})); // never resolves

    process.emit('SIGTERM');

    await new Promise((r) => setTimeout(r, 150));

    expect(handler.isShuttingDown()).toBe(true);
    expect(exitSpy).toHaveBeenCalledWith(0);
  });

  // Issue #1675: one shutdown path — the handler registered in index.ts runs
  // outbox, then DB cleanup, in a documented order after the HTTP server has
  // drained in-flight requests.
  it('runs onShutdown tasks in documented order after the server drains', async () => {
    const server = http.createServer((_req, res) => {
      // Hold the socket open briefly so the drain step is exercised.
      setTimeout(() => {
        res.writeHead(200);
        res.end('ok');
      }, 50);
    });
    await listen(server);

    const order: string[] = [];
    const handler = new GracefulShutdownHandler(2000, 50);
    handler.register(server);

    // Documented order (matches index.ts): background jobs/outbox first, DB last.
    handler.onShutdown(async () => {
      order.push('outbox-stop');
    });
    handler.onShutdown(async () => {
      order.push('db-disconnect');
    });

    let serverClosed = false;
    await new Promise<void>((resolve) => {
      server.close(() => {
        serverClosed = true;
        resolve();
      });
    });

    process.emit('SIGTERM');

    await new Promise((r) => setTimeout(r, 200));

    expect(handler.isShuttingDown()).toBe(true);
    // Server closes before cleanup tasks run.
    expect(serverClosed).toBe(true);
    // Cleanup tasks run in registration order: outbox before DB.
    expect(order).toEqual(['outbox-stop', 'db-disconnect']);
    expect(exitSpy).toHaveBeenCalledWith(0);
  });
});
