"""
Echo Server & Client using Multiprocessing
============================================
Demonstrates a multi-process TCP echo server that handles
multiple clients concurrently using Python's multiprocessing module.

How it works:
  - The server runs in its own process and forks a new child
    process for each incoming client connection.
  - Each client runs in its own process, connects, sends messages,
    receives echoed responses, and then disconnects.
  - Communication between the main orchestrator and the server
    process uses multiprocessing.Event for synchronization.

Timing:
  - Each message records: send_time, recv_time, round_trip, server_handle_time.
  - A summary table is printed at the end.

Run:
    python multiprocessing_echo.py
"""

import socket
import multiprocessing
import time
from datetime import datetime


# ──────────────────────────── Configuration ────────────────────────────
HOST = "127.0.0.1"
PORT = 9003
NUM_CLIENTS = 5          # Number of concurrent clients to launch
MESSAGES_PER_CLIENT = 3  # Messages each client sends


# ──────────────────────────── Server Side ──────────────────────────────

def handle_client(conn: socket.socket, addr: tuple) -> None:
    """Handle a single client connection in its own process."""
    print(f"[SERVER] Connected by {addr} (PID {multiprocessing.current_process().pid})")
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            server_start = time.perf_counter()
            message = data.decode()
            print(f"[SERVER] Received from {addr}: {message}")
            # Echo the message back with a prefix + server handling time
            server_elapsed_ms = (time.perf_counter() - server_start) * 1000
            response = f"Echo: {message}|server_ms={server_elapsed_ms:.4f}"
            conn.sendall(response.encode())
    except ConnectionResetError:
        pass
    finally:
        conn.close()
        print(f"[SERVER] Disconnected {addr}")


def run_server(ready_event: multiprocessing.Event, stop_event: multiprocessing.Event) -> None:
    """Start the TCP echo server in its own process.

    Args:
        ready_event: Set once the server is listening.
        stop_event:  When set, the server will gracefully shut down.
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.settimeout(1.0)  # Allow periodic shutdown checks
    server_sock.bind((HOST, PORT))
    server_sock.listen()
    print(f"[SERVER] Listening on {HOST}:{PORT} (PID {multiprocessing.current_process().pid})")
    ready_event.set()

    workers: list[multiprocessing.Process] = []
    try:
        while not stop_event.is_set():
            try:
                conn, addr = server_sock.accept()
                p = multiprocessing.Process(target=handle_client, args=(conn, addr), daemon=True)
                p.start()
                conn.close()  # Parent closes its copy; child has its own
                workers.append(p)
            except socket.timeout:
                continue
    finally:
        server_sock.close()
        for p in workers:
            p.join(timeout=2)
        print("[SERVER] Shut down.")


# ──────────────────────────── Client Side ──────────────────────────────

def run_client(client_id: int, results_queue: multiprocessing.Queue) -> None:
    """Connect to the server, send messages, and print echoed responses.

    Args:
        client_id:     A unique identifier for this client instance.
        results_queue: Queue to send timing records back to the main process.
    """
    timing_records = []
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        pid = multiprocessing.current_process().pid
        print(f"  [CLIENT-{client_id}] Connected to server (PID {pid})")

        for i in range(1, MESSAGES_PER_CLIENT + 1):
            message = f"Hello from client {client_id}, message #{i}"

            send_dt = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            send_time = time.perf_counter()
            sock.sendall(message.encode())

            raw_response = sock.recv(1024).decode()
            recv_time = time.perf_counter()
            recv_dt = datetime.now().strftime("%H:%M:%S.%f")[:-3]

            round_trip_ms = (recv_time - send_time) * 1000

            # Parse server handling time from response
            # Format: "Echo: <msg>|server_ms=<float>"
            if "|server_ms=" in raw_response:
                echo_part, server_part = raw_response.rsplit("|server_ms=", 1)
                server_handle_ms = float(server_part)
            else:
                echo_part = raw_response
                server_handle_ms = 0.0

            timing_records.append({
                "msg": i,
                "send_time": send_dt,
                "recv_time": recv_dt,
                "round_trip_ms": round_trip_ms,
                "server_handle_ms": server_handle_ms,
            })

            print(
                f"  [CLIENT-{client_id}] Msg #{i}: "
                f"response={echo_part} | "
                f"round_trip={round_trip_ms:.3f}ms | "
                f"server_handle={server_handle_ms:.4f}ms"
            )
            time.sleep(0.2)  # Simulate real work

        sock.close()
        print(f"  [CLIENT-{client_id}] Done.")
    except Exception as e:
        print(f"  [CLIENT-{client_id}] Error: {e}")

    results_queue.put((client_id, timing_records))


def print_timing_summary(all_results: dict) -> None:
    """Print a formatted timing summary table for all clients."""
    print("\n" + "=" * 80)
    print("  TIMING SUMMARY")
    print("=" * 80)
    print(
        f"  {'Client':<10} {'Msg':<5} {'Send Time':>14} {'Recv Time':>14} "
        f"{'Round Trip':>12} {'Server Handle':>14}"
    )
    print("  " + "-" * 72)

    all_round_trips = []
    all_server_handles = []

    for cid in sorted(all_results.keys()):
        records = all_results[cid]
        for rec in records:
            print(
                f"  Client-{cid:<3} {rec['msg']:<5} {rec['send_time']:>14} {rec['recv_time']:>14} "
                f"{rec['round_trip_ms']:>10.3f}ms {rec['server_handle_ms']:>12.4f}ms"
            )
            all_round_trips.append(rec["round_trip_ms"])
            all_server_handles.append(rec["server_handle_ms"])

    print("  " + "-" * 72)
    if all_round_trips:
        print(
            f"  {'AVG':<16} {'':<14} {'':<14} "
            f"{sum(all_round_trips)/len(all_round_trips):>10.3f}ms "
            f"{sum(all_server_handles)/len(all_server_handles):>12.4f}ms"
        )
        print(
            f"  {'MIN':<16} {'':<14} {'':<14} "
            f"{min(all_round_trips):>10.3f}ms "
            f"{min(all_server_handles):>12.4f}ms"
        )
        print(
            f"  {'MAX':<16} {'':<14} {'':<14} "
            f"{max(all_round_trips):>10.3f}ms "
            f"{max(all_server_handles):>12.4f}ms"
        )
    print("=" * 80)


# ──────────────────────────── Main ─────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Multiprocessing Echo Server / Client Demo")
    print("=" * 60)

    server_ready = multiprocessing.Event()
    server_stop = multiprocessing.Event()

    # Queue to collect timing results from client processes
    results_queue: multiprocessing.Queue = multiprocessing.Queue()

    # Start server in a separate process
    server_proc = multiprocessing.Process(target=run_server, args=(server_ready, server_stop))
    server_proc.start()
    server_ready.wait()  # Wait until server is listening

    # Launch multiple client processes concurrently
    client_procs: list[multiprocessing.Process] = []
    for cid in range(1, NUM_CLIENTS + 1):
        p = multiprocessing.Process(target=run_client, args=(cid, results_queue))
        p.start()
        client_procs.append(p)

    # Wait for all clients to finish
    for p in client_procs:
        p.join()

    # Collect timing results from the queue
    all_results: dict = {}
    while not results_queue.empty():
        cid, records = results_queue.get()
        all_results[cid] = records

    # Print timing summary
    print_timing_summary(all_results)

    # Signal server to shut down
    print("\nAll clients finished. Shutting down server...")
    server_stop.set()
    server_proc.join(timeout=5)
    print("Done!")
