// SPDX-License-Identifier: MIT
pragma solidity ^0.8.10;

import {IFlashLoanSimpleReceiver} from "@aave/core-v3/contracts/flashloan/interfaces/IFlashLoanSimpleReceiver.sol";
import {IPoolAddressesProvider} from "@aave/core-v3/contracts/interfaces/IPoolAddressesProvider.sol";
import {IPool} from "@aave/core-v3/contracts/interfaces/IPool.sol";
import {IERC20} from "@aave/core-v3/contracts/dependencies/openzeppelin/contracts/IERC20.sol";

interface IAerodromeRouter {
    struct Route {
        address from;
        address to;
        bool stable;
        address factory;
    }

    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        Route[] calldata routes,
        address to,
        uint256 deadline
    ) external returns (uint256[] memory amounts);
}

interface ISwapRouter02 {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut);
}

/**
 * @title BaseAaveHands — Aave V3 Flash-Loan Two-Leg Arbitrage
 * @notice Flash-borrows `asset`, sells it on Aerodrome (the rebalanced venue),
 *         buys it back on Uniswap V3 (the reference venue), repays Aave, keeps
 *         the spread. Reverts (lose only gas) if round-trip cannot cover
 *         amount + premium.
 */
contract BaseAaveHands is IFlashLoanSimpleReceiver {
    address public immutable owner;
    IPoolAddressesProvider public immutable ADDRESSES_PROVIDER;
    IPool public immutable POOL;
    IAerodromeRouter public immutable AERO_ROUTER;
    address public immutable AERO_FACTORY;
    ISwapRouter02 public immutable V3_ROUTER;

    modifier onlyOwner() {
        require(msg.sender == owner, "Only owner");
        _;
    }

    constructor(
        address addressProvider,
        address aerodromeRouter,
        address aerodromeFactory,
        address uniV3Router
    ) {
        ADDRESSES_PROVIDER = IPoolAddressesProvider(addressProvider);
        POOL = IPool(IPoolAddressesProvider(addressProvider).getPool());
        AERO_ROUTER = IAerodromeRouter(aerodromeRouter);
        AERO_FACTORY = aerodromeFactory;
        V3_ROUTER = ISwapRouter02(uniV3Router);
        owner = msg.sender;
    }

    /**
     * @notice Bot entry-point. Triggers the Aave flash loan.
     * @param asset Token to flash-borrow (e.g. AERO when rebalancing AERO->USDC).
     * @param amount Raw amount to borrow.
     * @param isLiquidation Reserved for future liquidation path.
     * @param params abi.encode(otherToken, v3Fee, minOutLeg1, minOutLeg2, aerodromeStable).
     */
    function executeRebalance(
        address asset,
        uint256 amount,
        bool isLiquidation,
        bytes calldata params
    ) external onlyOwner {
        isLiquidation; // silence unused-var warning, hook for future logic
        POOL.flashLoanSimple(address(this), asset, amount, params, 0);
    }

    /**
     * @notice Aave V3 callback. Performs the two-leg swap and repays.
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        require(msg.sender == address(POOL), "Only Pool");
        require(initiator == address(this), "Only self");

        (address otherToken, uint24 v3Fee, uint256 minOut1, uint256 minOut2, bool aerodromeStable) =
            abi.decode(params, (address, uint24, uint256, uint256, bool));

        // LEG 1: Aerodrome — sell `asset` for `otherToken` on the mispriced pool.
        IERC20(asset).approve(address(AERO_ROUTER), amount);
        IAerodromeRouter.Route[] memory routes = new IAerodromeRouter.Route[](1);
        routes[0] = IAerodromeRouter.Route({
            from: asset,
            to: otherToken,
            stable: aerodromeStable,
            factory: AERO_FACTORY
        });
        AERO_ROUTER.swapExactTokensForTokens(
            amount,
            minOut1,
            routes,
            address(this),
            block.timestamp + 60
        );

        // LEG 2: Uniswap V3 — buy back `asset` with the entire otherToken balance.
        uint256 otherBal = IERC20(otherToken).balanceOf(address(this));
        IERC20(otherToken).approve(address(V3_ROUTER), otherBal);
        V3_ROUTER.exactInputSingle(
            ISwapRouter02.ExactInputSingleParams({
                tokenIn: otherToken,
                tokenOut: asset,
                fee: v3Fee,
                recipient: address(this),
                amountIn: otherBal,
                amountOutMinimum: minOut2,
                sqrtPriceLimitX96: 0
            })
        );

        // Repayment guard: revert if round-trip didn't recover principal + fee.
        uint256 totalDebt = amount + premium;
        require(
            IERC20(asset).balanceOf(address(this)) >= totalDebt,
            "Insufficient repayment"
        );
        IERC20(asset).approve(address(POOL), totalDebt);
        return true;
    }

    function withdraw(address token) external onlyOwner {
        uint256 balance = IERC20(token).balanceOf(address(this));
        IERC20(token).transfer(owner, balance);
    }

    receive() external payable {}
}
