"""
Multi-Bot Suite Engine - Centralized credential and GraphQL management.

This module houses shared infrastructure for distributed trading bots.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp
from eth_account import Account
from eth_utils import to_checksum_address
from web3 import AsyncHTTPProvider, AsyncWeb3


dataclass(frozen=True)
class Secrets:
    """Immutable credentials container for bot operations."""
    owner_private_key: str
    rpc_url: str
    base_aave_hands_address: str
    flash_receiver_address: str | None
    graph_api_key: str | None


def initialize_web3(rpc_url: str) -> AsyncWeb3:
    """Initialize AsyncWeb3 with HTTP provider.
    
    Args:
        rpc_url: RPC endpoint URL (e.g., Chainstack, Alchemy)
        
    Returns:
        Configured AsyncWeb3 instance
    """
    return AsyncWeb3(AsyncHTTPProvider(rpc_url))


async def query_subgraph(query: str, variables: dict[str, Any] | None = None) -> dict:
    """Execute GraphQL query against AaveKit 2026 Production Gateway.
    
    Args:
        query: GraphQL query string
        variables: Optional query variables
        
    Returns:
        Parsed JSON response from subgraph
        
    Raises:
        ValueError: If GRAPH_API_KEY environment variable is missing
        aiohttp.ClientError: On network error
    """
    graph_api_key = os.getenv("GRAPH_API_KEY")
    if not graph_api_key:
        raise ValueError("GRAPH_API_KEY environment variable is required")
    
    url = "https://api.v3.aave.com/graphql"
    headers = {
        "Authorization": f"Bearer {graph_api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "query": query,
    }
    if variables:
        payload["variables"] = variables
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    raise ValueError(
                        f"Subgraph query failed with status {response.status}: "
                        f"{await response.text()}"
                    )
                return await response.json()
        except aiohttp.ClientError as exc:
            raise aiohttp.ClientError(
                f"Failed to reach subgraph at {url}: {str(exc)}"
            ) from exc


def normalize_private_key(value: str) -> str:
    """Validate and normalize private key format.
    
    Args:
        value: Raw private key (with or without 0x prefix)
        
    Returns:
        Normalized key with 0x prefix
        
    Raises:
        RuntimeError: If key is not valid 32-byte hex
    """
    normalized = value if value.startswith("0x") else f"0x{value}"
    if len(normalized) != 66:
        raise RuntimeError("PRIVATE_KEY must be a 32-byte hex value")
    return normalized


async def await_secrets() -> Secrets | None:
    """Poll for required environment secrets until available.
    
    Returns:
        Populated Secrets object when all required keys are present
    """
    heartbeat_interval = int(os.getenv("HEARTBEAT_SECONDS", "20"))
    
    while True:
        owner_private_key = os.getenv("OWNER_PRIVATE_KEY") or os.getenv("PRIVATE_KEY")
        rpc_url = os.getenv("RPC_URL", "https://mainnet.base.org")
        base_aave_hands_address = os.getenv("BASE_AAVE_HANDS_ADDRESS")
        flash_receiver_address = os.getenv("FLASH_RECEIVER_ADDRESS")
        graph_api_key = os.getenv("GRAPH_API_KEY")
        
        missing = [
            name
            for name, value in {
                "PRIVATE_KEY": owner_private_key,
                "BASE_AAVE_HANDS_ADDRESS": base_aave_hands_address,
            }.items()
            if not value
        ]
        
        if not missing:
            return Secrets(
                owner_private_key=normalize_private_key(str(owner_private_key)),
                rpc_url=str(rpc_url),
                base_aave_hands_address=str(base_aave_hands_address),
                flash_receiver_address=flash_receiver_address,
                graph_api_key=graph_api_key,
            )
        
        print(
            "Awaiting Secrets: "
            f"missing {', '.join(missing)}. "
            "Add the secrets and the bot will continue automatically.",
            flush=True,
        )
        await asyncio.sleep(heartbeat_interval)
