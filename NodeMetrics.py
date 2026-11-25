import asyncio
import json
import os
import random
import time
from pathlib import Path

import base58
import grpc
import typer
from rich.console import Console
from rich.table import Table
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction

from protos import auth_pb2, auth_pb2_grpc, bundle_pb2, packet_pb2, searcher_pb2, searcher_pb2_grpc

app = typer.Typer()
console = Console()

TXS_PER_BUNDLE = 4


class Session:
    channel: grpc.aio.Channel = None
    searcher: searcher_pb2_grpc.SearcherServiceStub = None
    kp: Keypair = None
    token: str = ""
    refresh = None
    expires: int = 0
    sent: dict = {}
    landed: set = set()
    failed: set = set()


session = Session()


async def connect(host: str, kp: Keypair):
    session.kp = kp
    session.channel = grpc.aio.secure_channel(host, grpc.ssl_channel_credentials())
    session.searcher = searcher_pb2_grpc.SearcherServiceStub(session.channel)
    await login()


async def login():
    stub = auth_pb2_grpc.AuthServiceStub(session.channel)
    pk = session.kp.pubkey()
    req = auth_pb2.GenerateAuthChallengeRequest(role=auth_pb2.Role.SEARCHER, pubkey=bytes(pk))
    resp = await stub.GenerateAuthChallenge(req)
    challenge = f"{pk}-{resp.challenge}"
    sig = session.kp.sign_message(challenge.encode())
    tokens = await stub.GenerateAuthTokens(
        auth_pb2.GenerateAuthTokensRequest(
            challenge=challenge, client_pubkey=bytes(pk), signed_challenge=bytes(sig)
        )
    )
    session.token = tokens.access_token.value
    session.expires = tokens.access_token.expires_at_utc.seconds
    session.refresh = tokens.refresh_token


async def meta():
    if time.time() > session.expires - 30:
        if session.refresh and time.time() < session.refresh.expires_at_utc.seconds - 30:
            stub = auth_pb2_grpc.AuthServiceStub(session.channel)
            resp = await stub.RefreshAccessToken(
                auth_pb2.RefreshAccessTokenRequest(refresh_token=session.refresh.value)
            )
            session.token = resp.access_token.value
            session.expires = resp.access_token.expires_at_utc.seconds
        else:
            await login()
    return (("authorization", f"Bearer {session.token}"),)


async def send(txs: list[bytes]) -> str:
    packets = [packet_pb2.Packet(data=raw, meta=packet_pb2.Meta(size=len(raw))) for raw in txs]
    req = searcher_pb2.SendBundleRequest(bundle=bundle_pb2.Bundle(packets=packets))
    resp = await session.searcher.SendBundle(req, metadata=await meta())
    return resp.uuid


async def watch_results():
    while True:
        try:
            stream = session.searcher.SubscribeBundleResults(
                searcher_pb2.SubscribeBundleResultsRequest(), metadata=await meta()
            )
            async for res in stream:
                kind = res.WhichOneof("result")
                if kind in ("processed", "finalized"):
                    session.landed.add(res.bundle_id)
                elif kind in ("rejected", "dropped"):
                    session.failed.add(res.bundle_id)
        except grpc.aio.AioRpcError as e:
            console.print(f"[yellow]results stream died: {e.code().name}, reconnecting[/yellow]")
            await asyncio.sleep(0.5)


async def tip_tx(rpc: AsyncClient, tip_accounts: list, lamports: int) -> bytes:
    kp = session.kp
    to = random.choice(tip_accounts)
    ix = transfer(TransferParams(from_pubkey=kp.pubkey(), to_pubkey=to, lamports=lamports))
    blockhash = (await rpc.get_latest_blockhash()).value.blockhash
    msg = Message.new_with_blockhash([ix], kp.pubkey(), blockhash)
    return bytes(Transaction([kp], msg, blockhash))


async def _run(txdir: Path, keypair: Path, host: str, rpc_url: str, tip: int, group_size: int, timeout_secs: int):
    kp = Keypair.from_bytes(bytes(json.loads(keypair.read_text())))
    files = sorted(p for p in txdir.iterdir() if p.is_file())
    txs = [base58.b58decode(p.read_text().strip()) for p in files]

    await connect(host, kp)
    watcher = asyncio.create_task(watch_results())
    resp = await session.searcher.GetTipAccounts(searcher_pb2.GetTipAccountsRequest(), metadata=await meta())
    tip_accounts = [Pubkey.from_string(a) for a in resp.accounts]

    bundles = [txs[i:i + TXS_PER_BUNDLE] for i in range(0, len(txs), TXS_PER_BUNDLE)]
    groups = [bundles[i:i + group_size] for i in range(0, len(bundles), group_size)]
    console.print(f"{len(txs)} txs -> {len(bundles)} bundles in {len(groups)} groups, tip {tip} lamports")

    async with AsyncClient(rpc_url) as rpc:
        for n, group in enumerate(groups):
            for b in group:
                try:
                    uuid = await send(b + [await tip_tx(rpc, tip_accounts, tip)])
                    session.sent[uuid] = n
                except grpc.aio.AioRpcError as e:
                    console.print(f"[red]group {n}: send failed: {e.details()}[/red]")
            await asyncio.sleep(2)

    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        if all(u in session.landed or u in session.failed for u in session.sent):
            break
        await asyncio.sleep(1)
    watcher.cancel()
    await session.channel.close()

    table = Table(title="jito bundles")
    for col in ("group", "sent", "landed", "rate"):
        table.add_column(col, justify="right")
    for n in range(len(groups)):
        ids = [u for u, g in session.sent.items() if g == n]
        ok = sum(1 for u in ids if u in session.landed)
        rate = f"{ok / len(ids) * 100:.0f}%" if ids else "-"
        table.add_row(str(n), str(len(ids)), str(ok), rate)
    console.print(table)


@app.command()
def run(
    txdir: Path = typer.Argument(..., exists=True, file_okay=False),
    keypair: Path = typer.Option(Path("auth.json"), "--keypair", "-k"),
    host: str = typer.Option(os.environ.get("JITO_HOST", "ny.mainnet.block-engine.jito.wtf:443"), "--host"),
    rpc_url: str = typer.Option("https://api.mainnet-beta.solana.com", "--rpc"),
    tip: int = typer.Option(10_000, "--tip", help="lamports"),
    group_size: int = typer.Option(5, "--group-size"),
    timeout_secs: int = typer.Option(75, "--timeout-secs"),
):
    asyncio.run(_run(txdir, keypair, host, rpc_url, tip, group_size, timeout_secs))


if __name__ == "__main__":
    app()
