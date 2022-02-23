import argparse
import time
from multiprocessing import Manager, Pool
from multiprocessing.connection import Listener
import handlers

def sql_updater(cache):
    proc = mp.current_process()
    print(f"[{proc}] starting sql_updater")
    while True:
        print(f"[{proc}] sleeping for 10 seconds; Zzz...")
        time.sleep(10)
        print(f"[{proc}] Just woke up; here's the cache:")
        print(cache)
    return

def dmm(hostname, port, authkey_file, n_workers=4):
    with open(authkey_file, "rb") as f_in:
        authkey = f_in.read()
    # Start DMM listener
    listener = Listener((hostname, port), authkey=authkey)
    # Initialize local cache
    manager = Manager()
    cache = manager.dict()
    # Initialize worker pool
    pool = Pool(processes=n_workers)
    while True:
        pool.apply_async(handlers.sql_updater, (cache))
        with listener.accept() as connection:
            print("connection accepted from", listener.last_accepted)
            daemon, payload = connection.recv()
            if daemon == "PREPARER":
                pool.apply_async(handlers.preparer_handler, (payload, cache))
            elif daemon == "SUBMITTER":
                # FIXME: currently, any request that expects a response breaks
                #        parallelism, because next listener.accept() is not allowed
                #        until connection.send(resp.get()) finishes
                resp = pool.apply_async(handlers.submitter_handler, (payload, cache))
                connection.send(resp.get())
            elif daemon == "FINISHER":
                pool.apply_async(handlers.finisher_handler, (payload, cache))

    pool.close()
    listener.close()

if __name__ == "__main__":
    cli = argparse.ArgumentParser(description="Rucio-SENSE data movement manager")
    cli.add_argument(
        "--host", type=str, default="localhost", 
        help="hostname for DMM"
    )
    cli.add_argument(
        "--port", type=int, default=6000, 
        help="port for DMM to listen to"
    )
    cli.add_argument(
        "--authkey", type=str, default="dummykey", 
        help="path to file with authorization key for DMM listener"
    )
    cli.add_argument(
        "-n", "--n_workers", type=int, default=4, 
        help="maximum number of worker processes"
    )
    args = cli.parse_args()

    print("Starting DMM...")
    dmm(args.host, args.port, args.authkey, n_workers=args.n_workers)
