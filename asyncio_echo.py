"""
Echo Server & Client using Asyncio
====================================
Demonstrates an asynchronous TCP echo server that handles
multiple clients concurrently using Python's asyncio module.

How it works:
  - The server creates a raw TCP socket (just like threading &
    multiprocessing versions), then uses asyncio's event loop
    to accept and handle connections as lightweight coroutines.
  - Each incoming client is handled as an async task
    (no OS threads needed).
  - Multiple clients connect as asyncio tasks, send messages,
    and receive echoed responses — all on a single thread.

Timing:
  - Each message records: send_time, recv_time, round_trip, server_handle_time.
  - A summary table is printed at the end.

Run:
    python asyncio_echo.py
"""

import asyncio
import socket
import time
from datetime import datetime


# ──────────────────────────── Configuration ────────────────────────────
HOST = "127.0.0.1"
PORT = 9002
NUM_CLIENTS = 5          # Number of concurrent clients to launch
MESSAGES_PER_CLIENT = 3  # Messages each client sends


# ──────────────────────────── Server Side ──────────────────────────────

async def handle_client(conn: socket.socket, addr: tuple) -> None:
    """Handle a single client connection as an async coroutine."""
    loop = asyncio.get_event_loop()
    print(f"[SERVER] Connected by {addr}")
    try:
        while True:
            data = await loop.sock_recv(conn, 1024)
            if not data:
                break
            server_start = time.perf_counter()
            message = data.decode()
            print(f"[SERVER] Received from {addr}: {message}")
            # Echo the message back with a prefix + server handling time
            server_elapsed_ms = (time.perf_counter() - server_start) * 1000
            response = f"Echo: {message}|server_ms={server_elapsed_ms:.4f}"
            await loop.sock_sendall(conn, response.encode())
    except ConnectionResetError:
        pass
    finally:
        conn.close()
        print(f"[SERVER] Disconnected {addr}")


async def run_server(started: asyncio.Event, stop: asyncio.Event) -> None:
    """Start the async TCP echo server.

    Args:
        started: Set once the server is listening, so clients
                 know they can begin connecting.
        stop:    When set, the server will gracefully shut down.
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen()
    server_sock.setblocking(False)  # Required for asyncio
    print(f"[SERVER] Listening on {HOST}:{PORT}")
    started.set()

    loop = asyncio.get_event_loop()
    tasks: list[asyncio.Task] = []
    try:
        while not stop.is_set():
            try:
                conn, addr = await asyncio.wait_for(
                    loop.sock_accept(server_sock), timeout=0.5
                )
                task = asyncio.create_task(handle_client(conn, addr))
                tasks.append(task)
            except asyncio.TimeoutError:
                continue
    finally:
        server_sock.close()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        print("[SERVER] Shut down.")


# ──────────────────────────── Client Side ──────────────────────────────

async def run_client(client_id: int) -> list[dict]:
    """Connect to the server, send messages, and print echoed responses.

    Args:
        client_id: A unique identifier for this client instance.

    Returns:
        A list of timing records for each message sent.
    """
    loop = asyncio.get_event_loop()
    timing_records = []
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        await loop.sock_connect(sock, (HOST, PORT))
        print(f"  [CLIENT-{client_id}] Connected to server")

        for i in range(1, MESSAGES_PER_CLIENT + 1):
            message = f"Hello from client {client_id}, message #{i}"

            send_dt = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            send_time = time.perf_counter()
            await loop.sock_sendall(sock, message.encode())

            raw_response = (await loop.sock_recv(sock, 1024)).decode()
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
            await asyncio.sleep(0.2)  # Simulate real work

        sock.close()
        print(f"  [CLIENT-{client_id}] Done.")
    except Exception as e:
        print(f"  [CLIENT-{client_id}] Error: {e}")

    return timing_records


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

async def main() -> None:
    print("=" * 60)
    print("  Asyncio Echo Server / Client Demo")
    print("=" * 60)

    server_started = asyncio.Event()
    server_stop = asyncio.Event()

    # Start the server as a background task
    server_task = asyncio.create_task(run_server(server_started, server_stop))
    await server_started.wait()  # Wait until server is listening

    # Launch all clients concurrently as async tasks
    client_tasks = [
        asyncio.create_task(run_client(cid))
        for cid in range(1, NUM_CLIENTS + 1)
    ]

    # Wait for all clients to finish and collect timing results
    results_list = await asyncio.gather(*client_tasks)
    all_results = {cid: records for cid, records in zip(range(1, NUM_CLIENTS + 1), results_list)}

    # Print timing summary
    print_timing_summary(all_results)

    # Signal server to shut down
    print("\nAll clients finished. Shutting down server...")
    server_stop.set()
    await server_task

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
