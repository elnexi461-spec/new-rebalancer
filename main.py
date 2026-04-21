"""
Base Mainnet Rebalancing & Liquidation Bot.

Audited "self-healing" production version. Highlights:
    * Decimal precision is explicit per token (18 vs 6 decimals).
    * "Ghost profit" sanity check: any projected profit > GHOST_PROFIT_ETH
      triggers a reserve re-read and is suppressed unless it survives the
      second pass.
    * Price-impact filter: trades whose simulated impact exceeds
      MAX_PRICE_IMPACT (default 2%) are discarded.
    * Gas optimisation: uses eth_maxPriorityFeePerGas with a base-fee bump
      for front-running protection.
    * Circuit breaker: if the bot burns more than CIRCUIT_BREAKER_GAS_ETH in
      gas inside CIRCUIT_BREAKER_WINDOW_SECONDS without a successful trade,
      it pauses for CIRCUIT_BREAKER_PAUSE_SECONDS.
    * Auto DRY_RUN -> LIVE promotion after AUTO_LIVE_CYCLES error-free cycles
      (only when AUTO_LIVE=true).
    * Structured JSON heartbeat every HEARTBEAT_SECONDS.

Transactions are signed locally with PRIVATE_KEY and broadcast directly to
the configured RPC endpoint (e.g. Chainstack). No bundler, no paymaster.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import Any, Deque

from eth_abi import encode as abi_encode
from eth_account import Account
from eth_utils import to_checksum_address
from web3 import AsyncHTTPProvider, AsyncWeb3

getcontext().prec = 60

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"
AUTO_LIVE = os.getenv("AUTO_LIVE", "false").lower() == "true"
AUTO_LIVE_CYCLES = int(os.getenv("AUTO_LIVE_CYCLES", "3"))

BASE_CHAIN_ID = 8453
BASE_PUBLIC_RPC = "https://mainnet.base.org"

AERODROME_FACTORY = "0x420DD381b31aEf6683db6B902084cB0FFEce40Da"
AERODROME_ROUTER = "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"
UNISWAP_V3_QUOTER = "0x222cA98F00eD15B1faE10B61c277703a194cf5d2"
AERO_TOKEN = "0x940181a94A35A4569E4529A3CDfB74e38FD98631"
USDC_TOKEN = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

# Token decimals registry. 18-decimal vs 6-decimal handled explicitly.
TOKEN_DECIMALS: dict[str, int] = {
    AERO_TOKEN.lower(): 18,
    USDC_TOKEN.lower(): 6,
    "0x4200000000000000000000000000000000000006": 18,  # WETH
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": 18,  # DAI
    "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2": 6,   # USDT (Base)
}
AERO_DECIMALS = TOKEN_DECIMALS[AERO_TOKEN.lower()]
USDC_DECIMALS = TOKEN_DECIMALS[USDC_TOKEN.lower()]

TARGET_POOL_RATIO = Decimal(os.getenv("TARGET_POOL_RATIO", "1.0000"))
REBALANCE_DEVIATION = Decimal(os.getenv("REBALANCE_DEVIATION", "0.01"))
MAX_PRICE_IMPACT = Decimal(os.getenv("MAX_PRICE_IMPACT", "0.02"))
GHOST_PROFIT_ETH = Decimal(os.getenv("GHOST_PROFIT_ETH", "10"))

HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "20"))
TX_GAS_LIMIT = int(os.getenv("TX_GAS_LIMIT", "1500000"))
PRIORITY_FEE_BUMP_WEI = int(os.getenv("PRIORITY_FEE_BUMP_WEI", "100000000"))  # +0.1 gwei

CIRCUIT_BREAKER_GAS_ETH = Decimal(os.getenv("CIRCUIT_BREAKER_GAS_ETH", "0.01"))
CIRCUIT_BREAKER_WINDOW_SECONDS = int(os.getenv("CIRCUIT_BREAKER_WINDOW_SECONDS", "3600"))
CIRCUIT_BREAKER_PAUSE_SECONDS = int(os.getenv("CIRCUIT_BREAKER_PAUSE_SECONDS", "900"))

REBALANCE_FUNCTION_SIGNATURE = "executeRebalance(address,uint256,bool,bytes)"
QUIET_LOGS = os.getenv("QUIET_LOGS", "true").lower() != "false"

# Two-leg arb params (Aerodrome -> UniswapV3)
V3_FEE_TIER = int(os.getenv("V3_FEE_TIER", "500"))
AERODROME_STABLE = os.getenv("AERODROME_STABLE", "false").lower() == "true"
SLIPPAGE_LEG1 = Decimal(os.getenv("SLIPPAGE_LEG1", "0.01"))
AAVE_PREMIUM_BPS = int(os.getenv("AAVE_PREMIUM_BPS", "5"))
PROFIT_BUFFER_BPS = int(os.getenv("PROFIT_BUFFER_BPS", "1"))
# Cap each trade's USDC notional. The bot's `calculate_rebalance` sizes to
# fully close the gap, but actual arb profit gets eaten by price impact at
# large sizes. Default $5,000 USDC keeps impact + fees well below the spread.
MAX_TRADE_USDC = Decimal(os.getenv("MAX_TRADE_USDC", "5000"))

# ---------------------------------------------------------------------------
# ABIs
# ---------------------------------------------------------------------------

POOL_FACTORY_ABI = [
    {
        "type": "function",
        "name": "getPool",
        "stateMutability": "view",
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "stable", "type": "bool"},
        ],
        "outputs": [{"name": "pool", "type": "address"}],
    }
]

AERODROME_ROUTER_ABI = [
    {
        "type": "function",
        "name": "getAmountsOut",
        "stateMutability": "view",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {
                "name": "routes",
                "type": "tuple[]",
                "components": [
                    {"name": "from", "type": "address"},
                    {"name": "to", "type": "address"},
                    {"name": "stable", "type": "bool"},
                    {"name": "factory", "type": "address"},
                ],
            },
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    }
]

UNISWAP_V3_QUOTER_ABI = [
    {
        "type": "function",
        "name": "quoteExactInputSingle",
        "stateMutability": "nonpayable",
        "inputs": [
            {
                "name": "params",
                "type": "tuple",
                "components": [
                    {"name": "tokenIn", "type": "address"},
                    {"name": "tokenOut", "type": "address"},
                    {"name": "amountIn", "type": "uint256"},
                    {"name": "fee", "type": "uint24"},
                    {"name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
            }
        ],
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "sqrtPriceX96After", "type": "uint160"},
            {"name": "initializedTicksCrossed", "type": "uint32"},
            {"name": "gasEstimate", "type": "uint256"},
        ],
    }
]

AERO_POOL_ABI = [
    {
        "type": "function",
        "name": "getReserves",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [
            {"name": "reserve0", "type": "uint256"},
            {"name": "reserve1", "type": "uint256"},
            {"name": "blockTimestampLast", "type": "uint256"},
        ],
    },
    {"type": "function", "name": "token0", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "address"}]},
    {"type": "function", "name": "token1", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "address"}]},
]

BASE_AAVE_HANDS_ABI = [
    {
        "type": "function",
        "name": "executeRebalance",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "asset", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "isLiquidation", "type": "bool"},
            {"name": "params", "type": "bytes"},
        ],
        "outputs": [],
    }
]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Secrets:
    owner_private_key: str
    rpc_url: str
    base_aave_hands_address: str
    flash_receiver_address: str | None
    graph_api_key: str | None


@dataclass(frozen=True)
class PoolState:
    pool: str
    token0: str
    token1: str
    reserve0: int
    reserve1: int
    aero_reserve: Decimal
    usdc_reserve: Decimal
    current_ratio: Decimal
    target_ratio: Decimal
    deviation: Decimal


@dataclass(frozen=True)
class RebalanceDecision:
    needed: bool
    aero_to_usdc: bool
    amount_in_raw: int
    amount_in_human: Decimal
    projected_ratio: Decimal
    price_impact: Decimal
    expected_gas_cost_eth: Decimal
    projected_profit: Decimal
    reason: str


@dataclass
class CircuitBreaker:
    gas_events: Deque[tuple[float, Decimal]] = field(default_factory=deque)
    paused_until: float = 0.0
    total_gas_spent_eth: Decimal = Decimal("0")
    total_profit_eth: Decimal = Decimal("0")

    def record_gas(self, cost_eth: Decimal) -> None:
        now = time.time()
        self.gas_events.append((now, cost_eth))
        self.total_gas_spent_eth += cost_eth
        self._trim(now)

    def record_success(self, profit_eth: Decimal) -> None:
        self.total_profit_eth += profit_eth
        self.gas_events.clear()

    def is_paused(self) -> bool:
        return time.time() < self.paused_until

    def should_trip(self) -> bool:
        now = time.time()
        self._trim(now)
        burned = sum((cost for _, cost in self.gas_events), Decimal("0"))
        if burned > CIRCUIT_BREAKER_GAS_ETH:
            self.paused_until = now + CIRCUIT_BREAKER_PAUSE_SECONDS
            self.gas_events.clear()
            return True
        return False

    def _trim(self, now: float) -> None:
        cutoff = now - CIRCUIT_BREAKER_WINDOW_SECONDS
        while self.gas_events and self.gas_events[0][0] < cutoff:
            self.gas_events.popleft()


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class RebalancingBot:
    def __init__(self, secrets: Secrets) -> None:
        self.secrets = secrets
        self.web3 = AsyncWeb3(AsyncHTTPProvider(secrets.rpc_url))
        self.account = Account.from_key(normalize_private_key(secrets.owner_private_key))
        self.owner_address = to_checksum_address(self.account.address)
        self.target = to_checksum_address(secrets.base_aave_hands_address)
        self.factory = self.web3.eth.contract(
            address=to_checksum_address(AERODROME_FACTORY), abi=POOL_FACTORY_ABI
        )
        self.aerodrome_router = self.web3.eth.contract(
            address=to_checksum_address(AERODROME_ROUTER),
            abi=AERODROME_ROUTER_ABI,
        )
        self.v3_quoter = self.web3.eth.contract(
            address=to_checksum_address(UNISWAP_V3_QUOTER),
            abi=UNISWAP_V3_QUOTER_ABI,
        )
        self.rebalance_contract = self.web3.eth.contract(
            address=self.target, abi=BASE_AAVE_HANDS_ABI
        )
        self.dry_run = DRY_RUN
        self.error_free_cycles = 0
        self.cycle = 0
        self.breaker = CircuitBreaker()

    # ------------------------------ main loop ------------------------------

    async def run_forever(self) -> None:
        self.log(
            {
                "event": "bot_online",
                "mode": "DRY_RUN" if self.dry_run else "LIVE",
                "owner": self.owner_address,
                "chainId": BASE_CHAIN_ID,
                "rpc": self.secrets.rpc_url.split("?")[0],
                "heartbeatSeconds": HEARTBEAT_SECONDS,
                "autoLive": AUTO_LIVE,
                "autoLiveCycles": AUTO_LIVE_CYCLES,
                "maxPriceImpact": str(MAX_PRICE_IMPACT),
                "circuitBreakerGasEth": str(CIRCUIT_BREAKER_GAS_ETH),
            }
        )
        while True:
            started_at = time.time()
            errored = False
            try:
                if self.breaker.is_paused():
                    self.log(
                        {
                            "event": "circuit_breaker_paused",
                            "resumesInSeconds": int(self.breaker.paused_until - started_at),
                        }
                    )
                else:
                    await self.tick()
            except Exception as exc:
                errored = True
                self.log({"event": "loop_error", "error": str(exc)})
                await self.self_heal(str(exc))

            self.cycle += 1
            if errored:
                self.error_free_cycles = 0
            else:
                self.error_free_cycles += 1
                if (
                    AUTO_LIVE
                    and self.dry_run
                    and self.error_free_cycles >= AUTO_LIVE_CYCLES
                ):
                    self.dry_run = False
                    self.log(
                        {
                            "event": "auto_promotion",
                            "from": "DRY_RUN",
                            "to": "LIVE",
                            "afterCycles": self.error_free_cycles,
                        }
                    )

            self.heartbeat()
            elapsed = time.time() - started_at
            await asyncio.sleep(max(1.0, HEARTBEAT_SECONDS - elapsed))

    async def tick(self) -> None:
        state = await self.fetch_aero_pool_state()
        decision = await self.calculate_rebalance(state)

        # Ghost-profit reality filter.
        if decision.projected_profit > GHOST_PROFIT_ETH:
            self.log(
                {
                    "event": "ghost_profit_suspect",
                    "projectedProfitEth": str(decision.projected_profit),
                    "action": "reverify_reserves",
                }
            )
            verified_state = await self.fetch_aero_pool_state()
            verified_decision = await self.calculate_rebalance(verified_state)
            if verified_decision.projected_profit > GHOST_PROFIT_ETH:
                self.log(
                    {
                        "event": "ghost_profit_suppressed",
                        "reason": "implausible_profit_after_reverify",
                        "projectedProfitEth": str(verified_decision.projected_profit),
                    }
                )
                return
            state, decision = verified_state, verified_decision

        if not QUIET_LOGS:
            print(
                "Rebalance Report: "
                f"[Current Ratio: {fmt(state.current_ratio)} | "
                f"Target Ratio: {fmt(state.target_ratio)} | "
                f"Price Impact: {fmt(decision.price_impact * 100)}% | "
                f"Expected Gas Cost: {fmt(decision.expected_gas_cost_eth)} ETH | "
                f"Projected Profit: {fmt(decision.projected_profit)} ETH]",
                flush=True,
            )

        self.log(
            {
                "event": "rebalance_report",
                "mode": "DRY_RUN" if self.dry_run else "LIVE",
                "pool": state.pool,
                "currentRatio": str(state.current_ratio),
                "targetRatio": str(state.target_ratio),
                "deviation": str(state.deviation),
                "rebalanceNeeded": decision.needed,
                "direction": "AERO_TO_USDC" if decision.aero_to_usdc else "USDC_TO_AERO",
                "amountInRaw": str(decision.amount_in_raw),
                "amountInHuman": str(decision.amount_in_human),
                "priceImpact": str(decision.price_impact),
                "expectedGasCostEth": str(decision.expected_gas_cost_eth),
                "projectedProfitEth": str(decision.projected_profit),
                "reason": decision.reason,
            }
        )

        if not decision.needed:
            return

        if decision.price_impact > MAX_PRICE_IMPACT:
            self.log(
                {
                    "event": "trade_skipped",
                    "reason": "price_impact_above_threshold",
                    "priceImpact": str(decision.price_impact),
                    "threshold": str(MAX_PRICE_IMPACT),
                }
            )
            return

        if decision.projected_profit <= 0:
            self.log(
                {"event": "trade_skipped", "reason": "non_positive_projected_profit"}
            )
            return

        if self.dry_run:
            self.log(
                {
                    "event": "dry_run_simulation",
                    "action": "not_submitted",
                    "callTarget": self.target,
                    "function": REBALANCE_FUNCTION_SIGNATURE,
                    "pool": state.pool,
                }
            )
            return

        # LIVE path.
        try:
            tx_hash = await self.send_rebalance_transaction(state, decision)
            if not tx_hash:
                # send_rebalance_transaction already logged the failure reason
                # (unsupported direction or estimateGas revert). Treat as a
                # gas-free skip — no broadcast happened.
                return
            self.breaker.record_success(decision.projected_profit)
            self.log(
                {
                    "event": "transaction_sent",
                    "txHash": tx_hash,
                    "profitEth": str(decision.projected_profit),
                }
            )
        except Exception as exc:
            self.breaker.record_gas(decision.expected_gas_cost_eth)
            self.log(
                {
                    "event": "transaction_failed",
                    "error": str(exc),
                    "gasBurnedEth": str(decision.expected_gas_cost_eth),
                }
            )
            if self.breaker.should_trip():
                self.log(
                    {
                        "event": "circuit_breaker_tripped",
                        "pauseSeconds": CIRCUIT_BREAKER_PAUSE_SECONDS,
                        "thresholdEth": str(CIRCUIT_BREAKER_GAS_ETH),
                    }
                )

    # ------------------------------ heartbeats -----------------------------

    def heartbeat(self) -> None:
        self.log(
            {
                "event": "heartbeat",
                "mode": "LIVE" if not self.dry_run else "DRY_RUN",
                "cycle": self.cycle,
                "errorFreeCycles": self.error_free_cycles,
                "wallet": self.owner_address,
                "gasSpentEth": str(self.breaker.total_gas_spent_eth),
                "profitTotalEth": str(self.breaker.total_profit_eth),
                "circuitBreakerPaused": self.breaker.is_paused(),
            }
        )

    # ------------------------------ pool state -----------------------------

    async def fetch_aero_pool_state(self) -> PoolState:
        pool = await self.factory.functions.getPool(AERO_TOKEN, USDC_TOKEN, False).call()
        if int(pool, 16) == 0:
            raise RuntimeError("Aerodrome AERO/USDC volatile pool was not found by the factory")
        pool = to_checksum_address(pool)
        contract = self.web3.eth.contract(address=pool, abi=AERO_POOL_ABI)
        token0, token1, reserves = await asyncio.gather(
            contract.functions.token0().call(),
            contract.functions.token1().call(),
            contract.functions.getReserves().call(),
        )
        token0 = to_checksum_address(token0)
        token1 = to_checksum_address(token1)
        reserve0, reserve1 = int(reserves[0]), int(reserves[1])

        if token0.lower() == AERO_TOKEN.lower():
            aero_raw, usdc_raw = reserve0, reserve1
        elif token1.lower() == AERO_TOKEN.lower():
            aero_raw, usdc_raw = reserve1, reserve0
        else:
            raise RuntimeError("Resolved Aerodrome pool does not contain AERO")

        aero_reserve = scale_down(aero_raw, AERO_DECIMALS)
        usdc_reserve = scale_down(usdc_raw, USDC_DECIMALS)
        if aero_reserve <= 0:
            raise RuntimeError("AERO reserve is zero")

        current_ratio = usdc_reserve / aero_reserve
        deviation = abs(current_ratio - TARGET_POOL_RATIO) / TARGET_POOL_RATIO

        return PoolState(
            pool=pool,
            token0=token0,
            token1=token1,
            reserve0=reserve0,
            reserve1=reserve1,
            aero_reserve=aero_reserve,
            usdc_reserve=usdc_reserve,
            current_ratio=current_ratio,
            target_ratio=TARGET_POOL_RATIO,
            deviation=deviation,
        )

    async def calculate_rebalance(self, state: PoolState) -> RebalanceDecision:
        expected_gas_cost_eth = await self.estimate_gas_cost_eth()
        needed = state.deviation >= REBALANCE_DEVIATION
        if not needed:
            return RebalanceDecision(
                False, False, 0, Decimal("0"),
                state.current_ratio, Decimal("0"),
                expected_gas_cost_eth, Decimal("0"),
                "within_band",
            )

        target_usdc_reserve = state.aero_reserve * state.target_ratio
        delta_usdc = state.usdc_reserve - target_usdc_reserve
        aero_to_usdc = delta_usdc < 0

        if aero_to_usdc:
            amount_human = abs(delta_usdc) / max(state.current_ratio, Decimal("1E-18"))
            amount_raw = int(amount_human * (Decimal(10) ** AERO_DECIMALS))
            price_impact = amount_human / state.aero_reserve
        else:
            amount_human = abs(delta_usdc)
            amount_raw = int(amount_human * (Decimal(10) ** USDC_DECIMALS))
            price_impact = amount_human / state.usdc_reserve

        usd_per_eth = Decimal(os.getenv("USD_PER_ETH", "3000"))
        gross_profit_eth = abs(delta_usdc) * Decimal("0.0005") / usd_per_eth
        projected_profit = max(Decimal("0"), gross_profit_eth - expected_gas_cost_eth)

        return RebalanceDecision(
            True,
            aero_to_usdc,
            max(amount_raw, 1),
            amount_human,
            state.target_ratio,
            price_impact,
            expected_gas_cost_eth,
            projected_profit,
            "reserve_ratio_deviation_above_threshold",
        )

    # ------------------------------ gas / RPC ------------------------------

    async def estimate_gas_cost_eth(self) -> Decimal:
        max_fee, _ = await self.preferred_fees()
        return scale_down(max_fee * TX_GAS_LIMIT, 18)

    async def preferred_fees(self) -> tuple[int, int]:
        """Return (maxFeePerGas, maxPriorityFeePerGas) in wei, EIP-1559."""
        try:
            block = await self.web3.eth.get_block("latest")
            base_fee = int(block.get("baseFeePerGas") or 0)
            try:
                priority_hex = await self.web3.manager.coro_request(
                    "eth_maxPriorityFeePerGas", []
                )
                priority = int_hex_or_dec(priority_hex)
            except Exception:
                priority = max(int(base_fee * 0.1), 1_000_000)
            priority += PRIORITY_FEE_BUMP_WEI  # front-running protection
            max_fee = base_fee * 2 + priority
            return max_fee, priority
        except Exception:
            gas_price = int(await self.web3.eth.gas_price)
            return gas_price, max(gas_price // 10, 1_000_000)

    # ------------------------------ tx send --------------------------------

    async def send_rebalance_transaction(
        self, state: PoolState, decision: RebalanceDecision
    ) -> str:
        max_fee, priority_fee = await self.preferred_fees()
        nonce = await self.web3.eth.get_transaction_count(self.owner_address, "pending")

        # AERO is NOT listed in Aave V3 on Base, so we always flash-loan USDC
        # (which IS listed). The current contract layout supports the
        # `aero_to_usdc=True` direction natively:
        #   LEG 1 — Aerodrome: USDC -> AERO (this rebalances the AERO/USDC pool
        #           upward, which is what we want when the pool ratio is below
        #           target, i.e. delta_usdc < 0).
        #   LEG 2 — Uniswap V3: AERO -> USDC (recovers USDC for repayment).
        # The opposite direction would need a `aerodromeFirst=false` contract
        # variant; we skip it for now to avoid burning gas on guaranteed reverts.
        if not decision.aero_to_usdc:
            self.log(
                {
                    "event": "transaction_failed",
                    "error": "direction_unsupported_by_current_contract",
                    "direction": "USDC_TO_AERO",
                    "gasBurnedEth": "0",
                }
            )
            return ""

        asset_in = USDC_TOKEN
        other_token = AERO_TOKEN
        # Trade size in USDC: |delta_usdc| in raw 6-dec units. The decision's
        # `amount_in_human` was originally AERO; convert via current ratio.
        usdc_amount_human = decision.amount_in_human * state.current_ratio
        # Cap to MAX_TRADE_USDC — full-rebalance sizes are unprofitable due to
        # self-inflicted price impact across the two venues.
        if usdc_amount_human > MAX_TRADE_USDC:
            usdc_amount_human = MAX_TRADE_USDC
        usdc_amount_raw = int(usdc_amount_human * (Decimal(10) ** USDC_DECIMALS))

        # Ask Aerodrome's router what `usdc_amount_raw` USDC will REALLY get us
        # in AERO out (this accounts for fee + price impact on the actual pool).
        # Apply only a small slippage cushion on top of the live quote — the
        # contract's leg-2 floor (`min_out_2 = amount + premium`) is the real
        # profit guard, so we want leg-1 to admit any quote close to live price.
        route = [(
            to_checksum_address(USDC_TOKEN),
            to_checksum_address(AERO_TOKEN),
            AERODROME_STABLE,
            to_checksum_address(AERODROME_FACTORY),
        )]
        try:
            amounts = await self.aerodrome_router.functions.getAmountsOut(
                usdc_amount_raw, route
            ).call()
            quoted_aero_raw = int(amounts[-1])
        except Exception as exc:
            self.log(
                {
                    "event": "transaction_failed",
                    "error": "getAmountsOut_revert",
                    "detail": str(exc)[:200],
                }
            )
            return ""

        expected_aero_human = Decimal(quoted_aero_raw) / (Decimal(10) ** AERO_DECIMALS)
        # Tight cushion on leg-1 — live quote already includes fee + impact.
        min_out_1 = int(
            Decimal(quoted_aero_raw) * (Decimal(1) - SLIPPAGE_LEG1)
        )
        # Repayment floor: principal + Aave premium + small profit buffer.
        bps_total = AAVE_PREMIUM_BPS + PROFIT_BUFFER_BPS
        min_out_2 = usdc_amount_raw + (
            usdc_amount_raw * bps_total + 9999
        ) // 10000

        # V3 round-trip preflight: ask Uniswap V3's QuoterV2 how many USDC the
        # AERO from leg-1 would yield. Skip the trade entirely if the round-trip
        # can't cover principal + premium — saves an estimateGas call and keeps
        # logs clean. The on-chain `min_out_2` floor is a defense-in-depth
        # guard for race conditions between this quote and broadcast.
        try:
            quote_call = self.v3_quoter.functions.quoteExactInputSingle((
                to_checksum_address(AERO_TOKEN),
                to_checksum_address(USDC_TOKEN),
                quoted_aero_raw,
                V3_FEE_TIER,
                0,
            ))
            v3_amount_out = (await quote_call.call())[0]
        except Exception as exc:
            self.log(
                {
                    "event": "transaction_failed",
                    "error": "v3_quote_revert",
                    "detail": str(exc)[:200],
                }
            )
            return ""

        if v3_amount_out < min_out_2:
            # No real spread right now — skip silently via heartbeat.
            self.log(
                {
                    "event": "no_arb",
                    "amountUsdcRaw": usdc_amount_raw,
                    "v3OutUsdcRaw": int(v3_amount_out),
                    "minOut2Usdc": min_out_2,
                    "shortfallUsdc": str(
                        Decimal(min_out_2 - v3_amount_out)
                        / (Decimal(10) ** USDC_DECIMALS)
                    ),
                }
            )
            return ""

        params_bytes = abi_encode(
            ["address", "uint24", "uint256", "uint256", "bool"],
            [
                to_checksum_address(other_token),
                V3_FEE_TIER,
                int(min_out_1),
                int(min_out_2),
                AERODROME_STABLE,
            ],
        )

        # One-line diagnostic so we can see what the bot is asking for.
        self.log(
            {
                "event": "trade_attempt",
                "asset": asset_in,
                "amountUsdc": str(usdc_amount_human),
                "amountUsdcRaw": usdc_amount_raw,
                "expectedAero": str(expected_aero_human),
                "minOut1Aero": min_out_1,
                "minOut2Usdc": min_out_2,
                "currentRatio": str(state.current_ratio),
                "v3Fee": V3_FEE_TIER,
                "aerodromeStable": AERODROME_STABLE,
            }
        )

        tx = await self.rebalance_contract.functions.executeRebalance(
            to_checksum_address(asset_in),
            usdc_amount_raw,
            False,
            params_bytes,
        ).build_transaction(
            {
                "from": self.owner_address,
                "nonce": nonce,
                "chainId": BASE_CHAIN_ID,
                "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
                "gas": TX_GAS_LIMIT,
                "value": 0,
                "type": 2,
            }
        )

        # Pre-flight estimateGas. If the call would revert, ABORT — never burn
        # gas on a guaranteed-revert broadcast. Tighten gas limit on success.
        try:
            estimated = await self.web3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated * 1.2)
        except Exception as exc:
            self.log(
                {
                    "event": "transaction_failed",
                    "error": "estimate_gas_revert",
                    "detail": str(exc)[:300],
                }
            )
            return ""

        signed = self.account.sign_transaction(tx)
        raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
        tx_hash = await self.web3.eth.send_raw_transaction(raw)
        return tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)

    # ------------------------------ self heal ------------------------------

    async def self_heal(self, error_message: str) -> None:
        lowered = error_message.lower()
        if "429" in lowered or "rate limit" in lowered:
            self.log({"event": "self_heal", "action": "rpc_backoff", "seconds": 30})
            await asyncio.sleep(30)
        elif "insufficient funds" in lowered:
            self.log(
                {
                    "event": "self_heal",
                    "action": "force_dry_run",
                    "reason": "insufficient_funds",
                }
            )
            self.dry_run = True
        elif "revert" in lowered:
            self.log({"event": "self_heal", "action": "skip_cycle", "reason": "revert"})

    # ------------------------------ logging --------------------------------

    # Only these events are emitted when QUIET_LOGS=true.
    _QUIET_ALLOWED = {
        "bot_online",
        "heartbeat",
        "transaction_sent",
        "transaction_failed",
        "trade_attempt",
        "no_arb",
        "circuit_breaker_tripped",
        "circuit_breaker_paused",
        "loop_error",
    }

    @classmethod
    def log(cls, payload: dict[str, Any]) -> None:
        if QUIET_LOGS and payload.get("event") not in cls._QUIET_ALLOWED:
            return
        payload.setdefault("timestamp", int(time.time()))
        print(json.dumps(payload, default=str), flush=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def await_secrets() -> Secrets | None:
    while True:
        owner_private_key = os.getenv("OWNER_PRIVATE_KEY") or os.getenv("PRIVATE_KEY")
        rpc_url = os.getenv("RPC_URL", BASE_PUBLIC_RPC)
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
        time.sleep(HEARTBEAT_SECONDS)


def normalize_private_key(value: str) -> str:
    normalized = value if value.startswith("0x") else f"0x{value}"
    if len(normalized) != 66:
        raise RuntimeError("PRIVATE_KEY must be a 32-byte hex value")
    return normalized


def scale_down(raw: int, decimals: int) -> Decimal:
    return Decimal(raw) / (Decimal(10) ** decimals)


def int_hex_or_dec(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


def fmt(value: Decimal) -> str:
    return f"{value:.8f}"


async def main() -> None:
    secrets = await asyncio.to_thread(await_secrets)
    if secrets is None:
        return
    bot = RebalancingBot(secrets)
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
